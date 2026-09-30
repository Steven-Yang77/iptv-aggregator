import os
import re
import time
import threading
from datetime import datetime
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import PlainTextResponse


# ============================================================
# Configuration
# ============================================================

def env_float(name, default, min_value=None):
    try:
        value = float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        value = default
    if min_value is not None and value < min_value:
        value = min_value
    return value


def env_int(name, default, min_value=None):
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        value = default
    if min_value is not None and value < min_value:
        value = min_value
    return value


SPIDER_URL = os.getenv(
    "SPIDER_URL",
    "https://iptvs.910501.xyz"
).rstrip("/")

PRIORITY_KEYWORDS = [
    x.strip()
    for x in os.getenv(
        "PRIORITY_KEYWORDS",
        "北京,联通"
    ).split(",")
    if x.strip()
]

REFRESH_INTERVAL_HOURS = env_float(
    "REFRESH_INTERVAL_HOURS",
    12.0,
    min_value=0.1
)

TIMEOUT = env_int(
    "TIMEOUT",
    15,
    min_value=1
)

MAX_SOURCES_PER_CHANNEL = env_int(
    "MAX_SOURCES_PER_CHANNEL",
    5,
    min_value=1
)

# 并发抓取节点的线程数
NODE_FETCH_WORKERS = env_int(
    "NODE_FETCH_WORKERS",
    32,
    min_value=1
)

# 最多处理多少个节点
MAX_NODES = env_int(
    "MAX_NODES",
    300,
    min_value=1
)

ADMIN_TOKEN = os.getenv("ADMIN_TOKEN")

ENABLE_REFRESH_LOOP = os.getenv(
    "ENABLE_REFRESH_LOOP",
    "true"
).lower() in ("1", "true", "yes")


# ============================================================
# HTTP Session
# ============================================================

_thread_local = threading.local()


def get_session():
    """每个线程独立 Session，避免 requests.Session 的线程安全问题。"""
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        s.headers.update({
            "User-Agent": "IPTV-Aggregator-Raymond/1.3"
        })
        _thread_local.session = s
    return _thread_local.session


# 用于获取节点列表的主 Session（仅主线程使用）
main_session = requests.Session()
main_session.headers.update({
    "User-Agent": "IPTV-Aggregator-Raymond/1.3"
})


# ============================================================
# Runtime state
# ============================================================

cached_m3u = "#EXTM3U\n"

last_update = None
last_error = None

last_nodes = 0
last_success_nodes = 0
last_failed_nodes = 0
last_channels = 0

# 刷新进度（运行中可见）
refresh_in_progress = False
refresh_progress_done = 0
refresh_progress_total = 0

refresh_lock = threading.Lock()


# ============================================================
# Get node list
# ============================================================

def get_nodes():
    url = f"{SPIDER_URL}/api/hotel/ips"

    response = main_session.get(
        url,
        params={"limit": 1000},
        timeout=TIMEOUT
    )

    response.raise_for_status()

    data = response.json()

    if not isinstance(data, list):
        raise RuntimeError("Node API returned invalid data")

    return data


# ============================================================
# Node score
# ============================================================

def node_score(node):
    text = " ".join(
        str(node.get(field) or "")
        for field in ["region", "city", "isp"]
    )

    score = 0

    for keyword in PRIORITY_KEYWORDS:
        if keyword in text:
            score += 30

    return score


# ============================================================
# Get node M3U
# ============================================================

def get_node_m3u(node_id):
    url = f"{SPIDER_URL}/api/hotel/ips/{node_id}/m3u"

    session = get_session()

    response = session.get(url, timeout=TIMEOUT)

    if response.status_code != 200:
        return "", f"HTTP {response.status_code}"

    text = response.content.decode(
        "utf-8-sig",
        errors="ignore"
    ).strip()

    if not text:
        return "", "empty body"

    if "#EXTM3U" not in text:
        return "", "non-M3U data"

    return text, None


# ============================================================
# Parse M3U
# ============================================================

def parse_m3u(m3u_text):
    lines = [
        line.strip()
        for line in m3u_text.splitlines()
        if line.strip()
    ]

    blocks = []
    current_block = []

    for line in lines:

        if line == "#EXTM3U":
            continue

        if not line.startswith("#"):

            if not current_block:
                continue

            url = line

            if not any(
                item.startswith("#EXTINF:")
                for item in current_block
            ):
                current_block = []
                continue

            blocks.append((current_block, url))
            current_block = []

        else:
            current_block.append(line)

    return blocks


# ============================================================
# Extract channel info from EXTINF
# ============================================================

def get_channel_info(block):
    display_name = ""
    key = ""

    for line in block:

        if not line.startswith("#EXTINF:"):
            continue

        m = re.search(r'tvg-id="([^"]*)"', line)
        if m and m.group(1).strip():
            key = m.group(1).strip()

        m = re.search(r'tvg-name="([^"]*)"', line)
        if m and m.group(1).strip():
            display_name = m.group(1).strip()
            if not key:
                key = display_name

        if not display_name and "," in line:
            name = line.split(",", 1)[1].strip()
            if name:
                display_name = name
                if not key:
                    key = name

        if display_name and key:
            break

    if not display_name:
        display_name = key or "Unknown"

    if not key:
        key = display_name

    return display_name, key


# ============================================================
# Fetch one node (runs in thread pool)
# ============================================================

def fetch_one_node(index, node):
    node_id = node.get("id")
    try:
        m3u, err = get_node_m3u(node_id)
        return index, m3u, err
    except Exception as e:
        return index, "", str(e)


# ============================================================
# Build M3U
# ============================================================

def build_m3u():

    global last_nodes
    global last_success_nodes
    global last_failed_nodes
    global refresh_progress_done
    global refresh_progress_total

    nodes = get_nodes()

    last_nodes = len(nodes)

    # 只取打分最高的前 MAX_NODES 个节点
    nodes.sort(key=node_score, reverse=True)
    nodes = nodes[:MAX_NODES]

    refresh_progress_total = len(nodes)
    refresh_progress_done = 0

    print(f"[INFO] Nodes fetched: {last_nodes}")
    print(f"[INFO] Nodes selected: {len(nodes)} (MAX_NODES={MAX_NODES})")
    print(f"[INFO] Priority keywords: {PRIORITY_KEYWORDS}")
    print(f"[INFO] Max sources per channel: {MAX_SOURCES_PER_CHANNEL}")
    print(f"[INFO] Concurrent workers: {NODE_FETCH_WORKERS}")

    # --------------------------------------------------------
    # 并发抓取所有节点（顺序保持）
    # --------------------------------------------------------

    fetch_results = [None] * len(nodes)

    with ThreadPoolExecutor(
        max_workers=NODE_FETCH_WORKERS,
        thread_name_prefix="node-fetch"
    ) as executor:

        future_to_index = {
            executor.submit(fetch_one_node, i, node): i
            for i, node in enumerate(nodes)
        }

        for future in as_completed(future_to_index):
            idx, m3u, err = future.result()
            fetch_results[idx] = (m3u, err)

            refresh_progress_done += 1

            if refresh_progress_done % 20 == 0 or \
               refresh_progress_done == len(nodes):
                print(
                    f"[INFO] Progress: "
                    f"{refresh_progress_done}/{len(nodes)}"
                )

    # --------------------------------------------------------
    # 合并结果（按优先级顺序处理）
    # --------------------------------------------------------

    all_blocks = []
    seen_urls = set()
    channel_source_count = {}

    success_nodes = 0
    failed_nodes = 0

    total_new_channels = 0
    total_duplicate_urls = 0
    total_limit_skipped = 0

    for index, node in enumerate(nodes):

        node_id = node.get("id")
        region = node.get("region") or ""
        city = node.get("city") or ""
        isp = node.get("isp") or ""
        score = node_score(node)

        m3u, err = fetch_results[index] if fetch_results[index] else ("", "not fetched")

        if not m3u:
            failed_nodes += 1
            print(
                f"[WARNING] Node {node_id} "
                f"({region} {city} {isp}) "
                f"failed: {err}"
            )
            continue

        blocks = parse_m3u(m3u)

        if not blocks:
            failed_nodes += 1
            print(
                f"[WARNING] Node {node_id} parsed 0 channels"
            )
            continue

        success_nodes += 1

        node_new = 0
        node_dup = 0
        node_limit = 0

        for block, url in blocks:

            url = url.strip()
            if not url:
                continue

            if url in seen_urls:
                node_dup += 1
                total_duplicate_urls += 1
                continue

            display_name, channel_key = get_channel_info(block)
            if not channel_key:
                channel_key = url

            current_count = channel_source_count.get(channel_key, 0)

            if current_count >= MAX_SOURCES_PER_CHANNEL:
                node_limit += 1
                total_limit_skipped += 1
                continue

            seen_urls.add(url)
            all_blocks.extend(block)
            all_blocks.append(url)

            channel_source_count[channel_key] = current_count + 1

            node_new += 1
            total_new_channels += 1

        print(
            f"[INFO] Node {node_id} "
            f"{region} {city} {isp} "
            f"Score={score} "
            f"new={node_new} dup={node_dup} limit={node_limit}"
        )

    last_success_nodes = success_nodes
    last_failed_nodes = failed_nodes

    if not all_blocks:
        raise RuntimeError(
            "No channels collected from selected nodes"
        )

    result = (
        "#EXTM3U\n"
        + "\n".join(all_blocks)
        + "\n"
    )

    channel_count = result.count("#EXTINF:")

    print("=" * 60)
    print("[INFO] Build completed")
    print(f"[INFO] Nodes total: {last_nodes}")
    print(f"[INFO] Nodes selected: {len(nodes)}")
    print(f"[INFO] Successful nodes: {success_nodes}")
    print(f"[INFO] Failed nodes: {failed_nodes}")
    print(f"[INFO] Unique channels/sources: {channel_count}")
    print(f"[INFO] Unique URLs: {len(seen_urls)}")
    print(f"[INFO] Duplicate URLs skipped: {total_duplicate_urls}")
    print(f"[INFO] Source-limit skipped: {total_limit_skipped}")
    print("=" * 60)

    return result


# ============================================================
# Refresh
# ============================================================

def refresh():

    global cached_m3u
    global last_update
    global last_error
    global last_channels
    global refresh_in_progress

    if not refresh_lock.acquire(blocking=False):
        print("[WARNING] Refresh already running.")
        return

    refresh_in_progress = True

    try:

        print("=" * 60)
        print("[INFO] Starting refresh...")

        m3u = build_m3u()

        channel_count = m3u.count("#EXTINF:")

        if channel_count <= 0:
            raise RuntimeError("No channels collected")

        cached_m3u = m3u
        last_channels = channel_count
        last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        last_error = None

        print(f"[INFO] Refresh completed: {channel_count} channels")
        print("=" * 60)

    except Exception as e:

        last_error = str(e)
        print(f"[ERROR] Refresh failed: {e}")
        print("[INFO] Existing cached M3U will be kept.")
        print("=" * 60)

    finally:
        refresh_in_progress = False
        refresh_lock.release()


# ============================================================
# Automatic refresh loop
# ============================================================

def refresh_loop():
    while True:
        refresh()

        sleep_seconds = REFRESH_INTERVAL_HOURS * 3600

        print(
            f"[INFO] Next refresh in "
            f"{REFRESH_INTERVAL_HOURS} hours."
        )
        time.sleep(sleep_seconds)


# ============================================================
# FastAPI lifespan
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):

    print("[INFO] IPTV Aggregator starting...")
    print(f"[INFO] Spider URL: {SPIDER_URL}")
    print(f"[INFO] Refresh interval: {REFRESH_INTERVAL_HOURS} hours")
    print(f"[INFO] Max sources/channel: {MAX_SOURCES_PER_CHANNEL}")
    print(f"[INFO] Node fetch workers: {NODE_FETCH_WORKERS}")
    print(f"[INFO] Max nodes: {MAX_NODES}")
    print(f"[INFO] Refresh loop enabled: {ENABLE_REFRESH_LOOP}")

    if ENABLE_REFRESH_LOOP:
        t = threading.Thread(
            target=refresh_loop,
            daemon=True,
            name="iptv-refresh"
        )
    else:
        t = threading.Thread(
            target=refresh,
            daemon=True,
            name="iptv-initial-refresh"
        )

    t.start()

    yield

    print("[INFO] IPTV Aggregator shutting down...")


# ============================================================
# FastAPI application
# ============================================================

app = FastAPI(
    title="IPTV Aggregator Raymond",
    version="1.3.0",
    lifespan=lifespan
)


# ============================================================
# Root
# ============================================================

@app.get("/")
def root():
    return {
        "service": "IPTV Aggregator Raymond",
        "status": "OK",
        "spider_url": SPIDER_URL,
        "priority_keywords": PRIORITY_KEYWORDS,
        "refresh_interval_hours": REFRESH_INTERVAL_HOURS,
        "max_sources_per_channel": MAX_SOURCES_PER_CHANNEL,
        "node_fetch_workers": NODE_FETCH_WORKERS,
        "max_nodes": MAX_NODES,
        "refresh_loop_enabled": ENABLE_REFRESH_LOOP,
        "admin_token_required": bool(ADMIN_TOKEN),
        "last_update": last_update,
        "channels": last_channels,
        "nodes": last_nodes,
        "success_nodes": last_success_nodes,
        "failed_nodes": last_failed_nodes,
        "error": last_error,
        "iptv_url": "/iptv"
    }


# ============================================================
# Health
# ============================================================

@app.get("/health")
def health():
    return {
        "status": "OK",
        "last_update": last_update,
        "channels": last_channels,
        "nodes": last_nodes,
        "success_nodes": last_success_nodes,
        "failed_nodes": last_failed_nodes,
        "error": last_error,
        "refresh_in_progress": refresh_in_progress,
        "refresh_progress": f"{refresh_progress_done}/{refresh_progress_total}"
    }


# ============================================================
# M3U output
# ============================================================

@app.get("/iptv", response_class=PlainTextResponse)
def iptv():
    return cached_m3u


# ============================================================
# Manual refresh
# ============================================================

@app.get("/refresh")
def manual_refresh(
    authorization: str = Header(None),
    token: str = Query(None)
):
    if ADMIN_TOKEN:
        expected = f"Bearer {ADMIN_TOKEN}"
        if authorization != expected and token != ADMIN_TOKEN:
            raise HTTPException(status_code=403, detail="Forbidden")

    t = threading.Thread(
        target=refresh,
        daemon=True,
        name="manual-refresh"
    )
    t.start()

    return {"status": "refresh_started"}


# ============================================================
# Local startup
# ============================================================

if __name__ == "__main__":

    import uvicorn

    port = int(os.getenv("PORT", "8080"))

    print(f"[INFO] Starting Uvicorn on 0.0.0.0:{port}")

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port
    )
import os
import re
import time
import threading
from datetime import datetime
from contextlib import asynccontextmanager

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

# 每个频道最多保留多少个不同直播源
MAX_SOURCES_PER_CHANNEL = env_int(
    "MAX_SOURCES_PER_CHANNEL",
    5,
    min_value=1
)

# 手动刷新鉴权 token（可选）
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN")

# 是否启用后台自动刷新循环（多 worker 部署时建议关闭，改用外部定时调用 /refresh）
ENABLE_REFRESH_LOOP = os.getenv(
    "ENABLE_REFRESH_LOOP",
    "true"
).lower() in ("1", "true", "yes")


# ============================================================
# HTTP Session
# ============================================================

session = requests.Session()

session.headers.update({
    "User-Agent": "IPTV-Aggregator-Raymond/1.2"
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


# 防止多个 refresh 同时执行
refresh_lock = threading.Lock()


# ============================================================
# Get node list
# ============================================================

def get_nodes():
    url = f"{SPIDER_URL}/api/hotel/ips"

    response = session.get(
        url,
        params={
            "limit": 1000
        },
        timeout=TIMEOUT
    )

    response.raise_for_status()

    data = response.json()

    if not isinstance(data, list):
        raise RuntimeError(
            "Node API returned invalid data"
        )

    return data


# ============================================================
# Node score
# ============================================================

def node_score(node):
    text = " ".join(
        str(node.get(field) or "")
        for field in [
            "region",
            "city",
            "isp"
        ]
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

    response = session.get(
        url,
        timeout=TIMEOUT
    )

    if response.status_code != 200:
        print(
            f"[WARNING] Node {node_id} "
            f"HTTP {response.status_code}"
        )
        return ""

    # 使用 utf-8-sig 自动去除 BOM
    text = response.content.decode(
        "utf-8-sig",
        errors="ignore"
    ).strip()

    if not text:
        return ""

    if "#EXTM3U" not in text:
        print(
            f"[WARNING] Node {node_id} "
            f"returned non-M3U data"
        )
        return ""

    return text


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

            # 必须存在 EXTINF
            if not any(
                item.startswith("#EXTINF:")
                for item in current_block
            ):
                current_block = []
                continue

            blocks.append(
                (
                    current_block,
                    url
                )
            )

            current_block = []

        else:
            current_block.append(line)

    return blocks


# ============================================================
# Extract channel info from EXTINF
# ============================================================

def get_channel_info(block):
    """
    从 #EXTINF 中提取频道显示名和唯一键。

    返回 (display_name, key)

    display_name 优先使用 tvg-name，其次逗号后的名称。
    key 优先使用 tvg-id，其次 display_name。
    """

    display_name = ""
    key = ""

    for line in block:

        if not line.startswith("#EXTINF:"):
            continue

        # 提取 tvg-id
        m = re.search(r'tvg-id="([^"]*)"', line)
        if m and m.group(1).strip():
            key = m.group(1).strip()

        # 提取 tvg-name
        m = re.search(r'tvg-name="([^"]*)"', line)
        if m and m.group(1).strip():
            display_name = m.group(1).strip()
            if not key:
                key = display_name

        # 如果还没有 display_name，尝试逗号后的文本
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
# Build M3U
# ============================================================

def build_m3u():

    global last_nodes
    global last_success_nodes
    global last_failed_nodes

    nodes = get_nodes()

    last_nodes = len(nodes)

    # --------------------------------------------------------
    # Priority sorting
    # --------------------------------------------------------

    nodes.sort(
        key=node_score,
        reverse=True
    )

    print(
        f"[INFO] Nodes: {len(nodes)}"
    )

    print(
        f"[INFO] Priority keywords: "
        f"{PRIORITY_KEYWORDS}"
    )

    print(
        f"[INFO] Max sources per channel: "
        f"{MAX_SOURCES_PER_CHANNEL}"
    )

    # --------------------------------------------------------
    # Output
    # --------------------------------------------------------

    all_blocks = []

    # 全局 URL 去重
    seen_urls = set()

    # 每个频道已经保存多少个源（按 channel_key 计数）
    channel_source_count = {}

    # --------------------------------------------------------
    # Statistics
    # --------------------------------------------------------

    success_nodes = 0
    failed_nodes = 0

    total_new_channels = 0
    total_duplicate_urls = 0
    total_limit_skipped = 0

    # --------------------------------------------------------
    # Process nodes
    # --------------------------------------------------------

    for index, node in enumerate(
        nodes,
        start=1
    ):

        node_id = node.get("id")

        region = node.get("region") or ""
        city = node.get("city") or ""
        isp = node.get("isp") or ""

        score = node_score(node)

        print(
            f"[INFO] [{index}/{len(nodes)}] "
            f"Node {node_id} "
            f"{region} "
            f"{city} "
            f"{isp} "
            f"Score={score}"
        )

        try:

            # ------------------------------------------------
            # Get M3U
            # ------------------------------------------------

            m3u = get_node_m3u(node_id)

            if not m3u:

                print(
                    f"[WARNING] Node {node_id} "
                    f"returned 0 channels."
                )

                failed_nodes += 1
                continue

            # ------------------------------------------------
            # Parse M3U
            # ------------------------------------------------

            blocks = parse_m3u(m3u)

            if not blocks:

                print(
                    f"[WARNING] Node {node_id} "
                    f"parsed 0 channels."
                )

                failed_nodes += 1
                continue

            # 节点成功获取并解析出频道
            success_nodes += 1

            node_new_channels = 0
            node_duplicate_urls = 0
            node_limit_skipped = 0

            # ------------------------------------------------
            # Process channels
            # ------------------------------------------------

            for block, url in blocks:

                url = url.strip()

                if not url:
                    continue

                # --------------------------------------------
                # Global URL duplicate
                # --------------------------------------------

                if url in seen_urls:

                    node_duplicate_urls += 1
                    total_duplicate_urls += 1

                    continue

                # --------------------------------------------
                # Channel info
                # --------------------------------------------

                display_name, channel_key = get_channel_info(block)

                if not channel_key:
                    # 没有频道标识时，用 URL 作为唯一标识
                    channel_key = url

                # --------------------------------------------
                # Channel source limit
                # --------------------------------------------

                current_count = (
                    channel_source_count.get(
                        channel_key,
                        0
                    )
                )

                if (
                    current_count
                    >= MAX_SOURCES_PER_CHANNEL
                ):

                    node_limit_skipped += 1
                    total_limit_skipped += 1

                    continue

                # --------------------------------------------
                # Accept source
                # --------------------------------------------

                seen_urls.add(url)

                all_blocks.extend(block)
                all_blocks.append(url)

                channel_source_count[
                    channel_key
                ] = current_count + 1

                node_new_channels += 1
                total_new_channels += 1

            # ------------------------------------------------
            # Node statistics
            # ------------------------------------------------

            if node_new_channels > 0:

                print(
                    f"[INFO] Node {node_id}: "
                    f"{node_new_channels} new sources"
                )

            elif node_duplicate_urls > 0:

                print(
                    f"[INFO] Node {node_id}: "
                    f"all usable URLs duplicated"
                )

            else:

                print(
                    f"[INFO] Node {node_id}: "
                    f"no new sources"
                )

        except Exception as e:

            failed_nodes += 1

            print(
                f"[WARNING] Node {node_id} "
                f"failed: {e}"
            )

            continue

    # --------------------------------------------------------
    # Save statistics
    # --------------------------------------------------------

    last_success_nodes = success_nodes
    last_failed_nodes = failed_nodes

    # --------------------------------------------------------
    # Validate result
    # --------------------------------------------------------

    if not all_blocks:

        raise RuntimeError(
            "No channels collected "
            "from selected nodes"
        )

    # --------------------------------------------------------
    # Build final M3U
    # --------------------------------------------------------

    result = (
        "#EXTM3U\n"
        + "\n".join(all_blocks)
        + "\n"
    )

    channel_count = result.count(
        "#EXTINF:"
    )

    print("=" * 60)

    print(
        f"[INFO] Build completed"
    )

    print(
        f"[INFO] Nodes: "
        f"{len(nodes)}"
    )

    print(
        f"[INFO] Successful nodes: "
        f"{success_nodes}"
    )

    print(
        f"[INFO] Failed nodes: "
        f"{failed_nodes}"
    )

    print(
        f"[INFO] Unique channels/sources: "
        f"{channel_count}"
    )

    print(
        f"[INFO] Unique URLs: "
        f"{len(seen_urls)}"
    )

    print(
        f"[INFO] Duplicate URLs skipped: "
        f"{total_duplicate_urls}"
    )

    print(
        f"[INFO] Source-limit skipped: "
        f"{total_limit_skipped}"
    )

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

    # --------------------------------------------------------
    # Prevent concurrent refresh
    # --------------------------------------------------------

    if not refresh_lock.acquire(
        blocking=False
    ):

        print(
            "[WARNING] Refresh already running."
        )

        return

    try:

        print("=" * 60)

        print(
            "[INFO] Starting refresh..."
        )

        # ----------------------------------------------------
        # Build
        # ----------------------------------------------------

        m3u = build_m3u()

        # ----------------------------------------------------
        # Count channels
        # ----------------------------------------------------

        channel_count = m3u.count(
            "#EXTINF:"
        )

        if channel_count <= 0:

            raise RuntimeError(
                "No channels collected"
            )

        # ----------------------------------------------------
        # IMPORTANT:
        # Only replace cache after successful build
        # ----------------------------------------------------

        cached_m3u = m3u

        last_channels = channel_count

        last_update = (
            datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        )

        last_error = None

        print(
            f"[INFO] Refresh completed: "
            f"{channel_count} channels"
        )

        print("=" * 60)

    except Exception as e:

        last_error = str(e)

        print(
            f"[ERROR] Refresh failed: {e}"
        )

        print(
            "[INFO] Existing cached M3U "
            "will be kept."
        )

        print("=" * 60)

    finally:

        refresh_lock.release()


# ============================================================
# Automatic refresh loop
# ============================================================

def refresh_loop():

    while True:

        refresh()

        sleep_seconds = (
            REFRESH_INTERVAL_HOURS
            * 3600
        )

        print(
            f"[INFO] Next refresh in "
            f"{REFRESH_INTERVAL_HOURS} hours."
        )

        time.sleep(
            sleep_seconds
        )


# ============================================================
# FastAPI lifespan
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):

    print(
        "[INFO] IPTV Aggregator starting..."
    )

    print(
        f"[INFO] Spider URL: {SPIDER_URL}"
    )

    print(
        f"[INFO] Refresh interval: "
        f"{REFRESH_INTERVAL_HOURS} hours"
    )

    print(
        f"[INFO] Max sources/channel: "
        f"{MAX_SOURCES_PER_CHANNEL}"
    )

    print(
        f"[INFO] Refresh loop enabled: "
        f"{ENABLE_REFRESH_LOOP}"
    )

    # --------------------------------------------------------
    # Start background refresh
    # --------------------------------------------------------

    if ENABLE_REFRESH_LOOP:
        thread = threading.Thread(
            target=refresh_loop,
            daemon=True,
            name="iptv-refresh"
        )
        thread.start()
    else:
        # 如果不启用循环，至少启动时刷新一次
        thread = threading.Thread(
            target=refresh,
            daemon=True,
            name="iptv-initial-refresh"
        )
        thread.start()

    yield

    print(
        "[INFO] IPTV Aggregator shutting down..."
    )


# ============================================================
# FastAPI application
# ============================================================

app = FastAPI(
    title="IPTV Aggregator Raymond",
    version="1.2.0",
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
        "refresh_interval_hours":
            REFRESH_INTERVAL_HOURS,
        "max_sources_per_channel":
            MAX_SOURCES_PER_CHANNEL,
        "refresh_loop_enabled":
            ENABLE_REFRESH_LOOP,
        "admin_token_required":
            bool(ADMIN_TOKEN),
        "last_update": last_update,
        "channels": last_channels,
        "nodes": last_nodes,
        "success_nodes":
            last_success_nodes,
        "failed_nodes":
            last_failed_nodes,
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
        "success_nodes":
            last_success_nodes,
        "failed_nodes":
            last_failed_nodes,
        "error": last_error
    }


# ============================================================
# M3U output
# ============================================================

@app.get(
    "/iptv",
    response_class=PlainTextResponse
)
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

    # 如果设置了 ADMIN_TOKEN，则必须通过鉴权
    if ADMIN_TOKEN:
        expected = f"Bearer {ADMIN_TOKEN}"
        if authorization != expected and token != ADMIN_TOKEN:
            raise HTTPException(
                status_code=403,
                detail="Forbidden"
            )

    thread = threading.Thread(
        target=refresh,
        daemon=True,
        name="manual-refresh"
    )

    thread.start()

    return {
        "status": "refresh_started"
    }


# ============================================================
# Local startup
# ============================================================

if __name__ == "__main__":

    import uvicorn

    port = int(
        os.getenv(
            "PORT",
            "8080"
        )
    )

    print(
        f"[INFO] Starting Uvicorn "
        f"on 0.0.0.0:{port}"
    )

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port
    )
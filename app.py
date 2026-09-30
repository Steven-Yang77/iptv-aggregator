
import os
import time
import threading
from datetime import datetime
from contextlib import asynccontextmanager

import requests
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse


# ============================================================
# Configuration
# ============================================================

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

REFRESH_INTERVAL_HOURS = float(
    os.getenv(
        "REFRESH_INTERVAL_HOURS",
        "12"
    )
)

TIMEOUT = int(
    os.getenv(
        "TIMEOUT",
        "15"
    )
)

# 每个频道最多保留多少个不同直播源
MAX_SOURCES_PER_CHANNEL = int(
    os.getenv(
        "MAX_SOURCES_PER_CHANNEL",
        "5"
    )
)


# ============================================================
# HTTP Session
# ============================================================

session = requests.Session()

session.headers.update({
    "User-Agent": "IPTV-Aggregator-Raymond/1.0"
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

    text = response.text.strip()

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
# Extract channel name from EXTINF
# ============================================================

def get_channel_name(block):
    """
    从 #EXTINF 中提取频道名称。

    例如：

    #EXTINF:-1 tvg-name="CCTV1" group-title="央视",CCTV1

    返回：

    CCTV1
    """

    for line in block:

        if not line.startswith("#EXTINF:"):
            continue

        if "," in line:
            name = line.split(",", 1)[1].strip()

            if name:
                return name

    return ""


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

    # 每个频道已经保存多少个源
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
                # Channel name
                # --------------------------------------------

                channel_name = get_channel_name(
                    block
                )

                if not channel_name:

                    # 没有频道名时，用 URL 作为唯一标识
                    channel_name = url

                # --------------------------------------------
                # Channel source limit
                # --------------------------------------------

                current_count = (
                    channel_source_count.get(
                        channel_name,
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
                    channel_name
                ] = current_count + 1

                node_new_channels += 1
                total_new_channels += 1

            # ------------------------------------------------
            # Node statistics
            # ------------------------------------------------

            if node_new_channels > 0:

                success_nodes += 1

                print(
                    f"[INFO] Node {node_id}: "
                    f"{node_new_channels} new sources"
                )

            elif node_duplicate_urls > 0:

                # 节点正常，只是 URL 已经存在
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

    # --------------------------------------------------------
    # Start background refresh
    # --------------------------------------------------------

    thread = threading.Thread(
        target=refresh_loop,
        daemon=True,
        name="iptv-refresh"
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
    version="1.1.0",
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
def manual_refresh():

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

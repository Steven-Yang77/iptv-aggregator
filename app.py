```python
import os
import time
import threading
from datetime import datetime
from contextlib import asynccontextmanager

import requests
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse


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
    os.getenv("REFRESH_INTERVAL_HOURS", "12")
)

TIMEOUT = int(
    os.getenv("TIMEOUT", "15")
)


session = requests.Session()

session.headers.update({
    "User-Agent": "IPTV-Aggregator-Raymond/1.0"
})


cached_m3u = "#EXTM3U\n"

last_update = None
last_error = None

last_nodes = 0
last_success_nodes = 0
last_failed_nodes = 0
last_channels = 0


def get_nodes():
    url = f"{SPIDER_URL}/api/hotel/ips"

    response = session.get(
        url,
        params={"limit": 1000},
        timeout=TIMEOUT
    )

    response.raise_for_status()

    data = response.json()

    if not isinstance(data, list):
        raise RuntimeError(
            "Node API returned invalid data"
        )

    return data


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


def build_m3u():

    global last_nodes
    global last_success_nodes
    global last_failed_nodes

    nodes = get_nodes()

    last_nodes = len(nodes)

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

    all_blocks = []
    seen_urls = set()

    success_nodes = 0
    failed_nodes = 0

    for index, node in enumerate(
        nodes,
        start=1
    ):

        node_id = node.get("id")

        region = node.get("region") or ""
        isp = node.get("isp") or ""

        score = node_score(node)

        print(
            f"[INFO] [{index}/{len(nodes)}] "
            f"Node {node_id} "
            f"{region} "
            f"{isp} "
            f"Score={score}"
        )

        try:

            m3u = get_node_m3u(node_id)

            if not m3u:

                print(
                    f"[WARNING] Node {node_id} "
                    f"returned 0 channels."
                )

                failed_nodes += 1
                continue

            blocks = parse_m3u(m3u)

            if not blocks:

                print(
                    f"[WARNING] Node {node_id} "
                    f"parsed 0 channels."
                )

                failed_nodes += 1
                continue

            new_channels = 0

            for block, url in blocks:

                if url in seen_urls:
                    continue

                seen_urls.add(url)

                all_blocks.extend(block)
                all_blocks.append(url)

                new_channels += 1

            if new_channels > 0:

                success_nodes += 1

                print(
                    f"[INFO] Node {node_id}: "
                    f"{new_channels} new channels"
                )

            else:

                print(
                    f"[WARNING] Node {node_id}: "
                    f"all channels duplicated"
                )

        except Exception as e:

            failed_nodes += 1

            print(
                f"[WARNING] Node {node_id} "
                f"failed: {e}"
            )

            continue

    last_success_nodes = success_nodes
    last_failed_nodes = failed_nodes

    if not all_blocks:

        raise RuntimeError(
            "No channels collected "
            "from selected nodes"
        )

    return (
        "#EXTM3U\n"
        + "\n".join(all_blocks)
        + "\n"
    )


def refresh():

    global cached_m3u
    global last_update
    global last_error
    global last_channels

    try:

        print("=" * 60)
        print("[INFO] Starting refresh...")

        m3u = build_m3u()

        channel_count = m3u.count(
            "#EXTINF:"
        )

        if channel_count <= 0:

            raise RuntimeError(
                "No channels collected"
            )

        cached_m3u = m3u

        last_channels = channel_count

        last_update = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
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


def refresh_loop():

    while True:

        refresh()

        sleep_seconds = (
            REFRESH_INTERVAL_HOURS * 3600
        )

        print(
            f"[INFO] Next refresh in "
            f"{REFRESH_INTERVAL_HOURS} hours."
        )

        time.sleep(
            sleep_seconds
        )


@asynccontextmanager
async def lifespan(app: FastAPI):

    print(
        "[INFO] IPTV Aggregator starting..."
    )

    thread = threading.Thread(
        target=refresh_loop,
        daemon=True
    )

    thread.start()

    yield

    print(
        "[INFO] IPTV Aggregator shutting down..."
    )


app = FastAPI(
    title="IPTV Aggregator Raymond",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/")
def root():

    return {
        "service": "IPTV Aggregator Raymond",
        "status": "OK",
        "spider_url": SPIDER_URL,
        "priority_keywords": PRIORITY_KEYWORDS,
        "refresh_interval_hours":
            REFRESH_INTERVAL_HOURS,
        "last_update": last_update,
        "channels": last_channels,
        "nodes": last_nodes,
        "success_nodes": last_success_nodes,
        "failed_nodes": last_failed_nodes,
        "error": last_error,
        "iptv_url": "/iptv"
    }


@app.get("/health")
def health():

    return {
        "status": "OK",
        "last_update": last_update,
        "channels": last_channels,
        "nodes": last_nodes,
        "success_nodes": last_success_nodes,
        "failed_nodes": last_failed_nodes,
        "error": last_error
    }


@app.get(
    "/iptv",
    response_class=PlainTextResponse
)
def iptv():

    return cached_m3u


@app.get("/refresh")
def manual_refresh():

    thread = threading.Thread(
        target=refresh,
        daemon=True
    )

    thread.start()

    return {
        "status": "refresh_started"
    }


if __name__ == "__main__":

    import uvicorn

    port = int(
        os.getenv(
            "PORT",
            "8080"
        )
    )

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port
    )
```

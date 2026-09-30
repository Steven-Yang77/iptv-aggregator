import os
import re
import time
import threading
from datetime import datetime

import requests
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

app = FastAPI(title="IPTV Aggregator Raymond")

SPIDER_URL = os.getenv(
    "SPIDER_URL",
    "https://iptvs.910501.xyz"
)

PRIORITY_KEYWORDS = [
    x.strip()
    for x in os.getenv("PRIORITY_KEYWORDS", "北京,联通").split(",")
    if x.strip()
]

REFRESH_INTERVAL_HOURS = float(
    os.getenv("REFRESH_INTERVAL_HOURS", "12")
)

TIMEOUT = int(os.getenv("TIMEOUT", "15"))

session = requests.Session()

cached_m3u = "#EXTM3U\n"
last_update = None
last_error = None


def get_nodes():
    url = f"{SPIDER_URL}/api/hotel/ips"

    r = session.get(
        url,
        params={"limit": 1000},
        timeout=TIMEOUT,
    )
    r.raise_for_status()

    return r.json()


def node_score(node):
    text = " ".join(
        str(node.get(x) or "")
        for x in ["region", "city", "isp"]
    )

    score = 0

    for keyword in PRIORITY_KEYWORDS:
        if keyword in text:
            score += 30

    return score


def get_node_m3u(node_id):
    url = f"{SPIDER_URL}/api/hotel/ips/{node_id}/m3u"

    r = session.get(
        url,
        timeout=TIMEOUT,
    )

    if r.status_code != 200:
        return ""

    text = r.text.strip()

    if not text or "#EXTM3U" not in text:
        return ""

    return text


def normalize_m3u(text):
    if not text:
        return []

    lines = text.splitlines()

    result = []

    for line in lines:
        line = line.strip()

        if not line:
            continue

        if line.startswith("#EXTM3U"):
            continue

        result.append(line)

    return result


def build_m3u():
    nodes = get_nodes()

    nodes.sort(
        key=node_score,
        reverse=True
    )

    print(
        f"[INFO] Nodes: {len(nodes)}, "
        f"keywords: {PRIORITY_KEYWORDS}"
    )

    all_blocks = []
    seen_urls = set()

    for node in nodes:

        score = node_score(node)

        node_id = node.get("id")

        print(
            f"[INFO] Node {node_id} "
            f"{node.get('region')} "
            f"{node.get('isp')} "
            f"Score={score}"
        )

        try:
            m3u = get_node_m3u(node_id)

            if not m3u:
                print(
                    f"[WARNING] Node {node_id} "
                    f"returned 0 channels."
                )
                continue

            lines = m3u.splitlines()

            current_block = []

            for line in lines:

                line = line.strip()

                if not line or line == "#EXTM3U":
                    continue

                current_block.append(line)

                if not line.startswith("#"):
                    url = line

                    if url in seen_urls:
                        current_block = []
                        continue

                    seen_urls.add(url)

                    all_blocks.extend(current_block)

                    current_block = []

            print(
                f"[INFO] Node {node_id} "
                f"channels collected."
            )

        except Exception as e:

            print(
                f"[WARNING] Node {node_id} "
                f"failed: {e}"
            )

            continue

    result = "#EXTM3U\n"

    result += "\n".join(all_blocks)

    return result


def refresh():

    global cached_m3u
    global last_update
    global last_error

    try:

        print("[INFO] Starting refresh...")

        m3u = build_m3u()

        if m3u.count("#EXTINF") == 0:
            raise RuntimeError(
                "No channels collected"
            )

        cached_m3u = m3u

        last_update = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        last_error = None

        print(
            f"[INFO] Refresh completed: "
            f"{m3u.count('#EXTINF')} channels"
        )

    except Exception as e:

        last_error = str(e)

        print(
            f"[ERROR] Refresh failed: {e}"
        )


def refresh_loop():

    while True:

        refresh()

        time.sleep(
            REFRESH_INTERVAL_HOURS * 3600
        )


@app.get("/")
def root():

    return {
        "service": "IPTV Aggregator Raymond",
        "status": "OK",
        "last_update": last_update,
        "keywords": PRIORITY_KEYWORDS,
        "channels": cached_m3u.count("#EXTINF"),
        "error": last_error,
    }


@app.get("/health")
def health():

    return {
        "status": "OK",
        "last_update": last_update,
        "channels": cached_m3u.count("#EXTINF"),
    }


@app.get(
    "/iptv",
    response_class=PlainTextResponse
)
def iptv():

    return cached_m3u


@app.on_event("startup")
def startup():

    thread = threading.Thread(
        target=refresh_loop,
        daemon=True
    )

    thread.start()
```python
import os
import time
import threading
from datetime import datetime
from contextlib import asynccontextmanager

import requests
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse


# ============================================================
# 配置
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
    os.getenv("REFRESH_INTERVAL_HOURS", "12")
)

TIMEOUT = int(
    os.getenv("TIMEOUT", "15")
)


# ============================================================
# HTTP Session
# ============================================================

session = requests.Session()

session.headers.update({
    "User-Agent": "IPTV-Aggregator-Raymond/1.0"
})


# ============================================================
# 全局缓存
# ============================================================

cached_m3u = "#EXTM3U\n"

last_update = None
last_error = None

last_nodes = 0
last_success_nodes = 0
last_failed_nodes = 0
last_channels = 0


# ============================================================
# 获取节点列表
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
        raise RuntimeError("Node API returned invalid data")

    return data


# ============================================================
# 节点优先级
#
# 注意：
# PRIORITY_KEYWORDS 只用于排序。
# 不会因为节点不匹配关键词而被排除。
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
# 获取单个节点 M3U
# ============================================================

def get_node_m3u(node_id):
    url = f"{SPIDER_URL}/api/hotel/ips/{node_id}/m3u"

    response = session.get(
        url,
        timeout=TIMEOUT
    )

    if response.status_code != 200:
        print(
            f"[WARNING] Node {node_id} HTTP {response.status_code}"
        )
        return ""

    text = response.text.strip()

    if not text:
        return ""

    if "#EXTM3U" not in text:
        print(
            f"[WARNING] Node {node_id} returned non-M3U data"
        )
        return ""

    return text


# ============================================================
# 解析 M3U
#
# 一个节目通常是：
#
# #EXTINF:-1,...
# http://xxxx
#
# 保留完整节目块。
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

        # URL 行
        if not line.startswith("#"):

            if not current_block:
                continue

            url = line

            # 当前节目必须有 EXTINF
            if not any(
                x.startswith("#EXTINF:")
                for x in current_block
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
# 构建最终 M3U
# ============================================================

def build_m3u():

    global last_nodes
    global last_success_nodes
    global last_failed_nodes

    nodes = get_nodes()

    last_nodes = len(nodes)

    # 按优先级排序
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

    # ========================================================
    # 逐个尝试节点
    # ========================================================

    for index, node in enumerate(nodes, start=1):

        node_id = node.get("id")

        region = node.get("region") or ""
        city = node.get("city") or ""
        isp = node.get("isp") or ""

        score = node_score(node)

        print(
            f"[INFO] "
            f"[{index}/{len(nodes)}] "
            f"Node {node_id} "
            f"{region} "
            f"{isp} "
            f"Score={score}"
        )

        try:

            m3u = get_node_m3u(node_id)

            if not m3u:

                print(
                    f"[WARNING] "
                    f"Node {node_id} returned 0 channels."
                )

                failed_nodes += 1
                continue

            blocks = parse_m3u(m3u)

            if not blocks:

                print(
                    f"[WARNING] "
                    f"Node {node_id} parsed 0 channels."
                )

                failed_nodes += 1
                continue

            new_channels = 0

            for block, url in blocks:

                # URL 去重
                if url in seen_urls:
                    continue

                seen_urls.add(url)

                all_blocks.extend(block)
                all_blocks.append(url)

                new_channels += 1

            if new_channels > 0:

                success_nodes += 1

                print(
                    f"[INFO] "
                    f"Node {node_id}: "
                    f"{new_channels} new channels"
                )

            else:

                print(
                    f"[WARNING] "
                    f"Node {node_id}: "
                    f"all channels duplicated"
                )

        except Exception as e:

            failed_nodes += 1

            print(
                f"[WARNING] "
                f"Node {node_id} failed: {e}"
            )

            continue

    last_success_nodes = success_nodes
    last_failed_nodes = failed_nodes

    # ========================================================
    # 最终 M3U
    # ========================================================

    if not all_blocks:

        raise RuntimeError(
            "No channels collected from selected nodes"
        )

    result = (
        "#EXTM3U\n"
        + "\n".join(all_blocks)
        + "\n"
    )

    return result


# ============================================================
# 刷新
# ============================================================

def refresh():

    global cached_m3u
    global last_update
    global last_error
    global last_channels

    try:

        print("=" * 60)

        print(
            "[INFO] Starting refresh..."
        )

        m3u = build_m3u()

        channel_count = m3u.count(
            "#EXTINF:"
        )

        if channel_count <= 0:

            raise RuntimeError(
                "No channels collected"
            )

        # ====================================================
        # 只有新数据正常时才替换缓存
        #
        # 如果这次刷新失败：
        # Cinetry 继续使用旧数据。
        # ====================================================

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
            "[INFO] Existing cached M3U will be kept."
        )

        print("=" * 60)


# ============================================================
# 自动刷新线程
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
# FastAPI Lifespan
#
# 替代已经 deprecated 的:
#
# @app.on_event("startup")
# ============================================================

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


# ============================================================
# FastAPI App
# ============================================================

app = FastAPI(
    title="IPTV Aggregator Raymond",
    version="1.0.0",
    lifespan=lifespan
)


# ============================================================
# 首页
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

        "last_update":
            last_update,

        "channels":
            last_channels,

        "nodes":
            last_nodes,

        "success_nodes":
            last_success_nodes,

        "failed_nodes":
            last_failed_nodes,

        "error":
            last_error,

        "iptv_url":
            "/iptv"
    }


# ============================================================
# Health
# ============================================================

@app.get("/health")
def health():

    return {
        "status": "OK",

        "last_update":
            last_update,

        "channels":
            last_channels,

        "nodes":
            last_nodes,

        "success_nodes":
            last_success_nodes,

        "failed_nodes":
            last_failed_nodes,

        "error":
            last_error
    }


# ============================================================
# IPTV M3U
# ============================================================

@app.get(
    "/iptv",
    response_class=PlainTextResponse
)
def iptv():

    return cached_m3u


# ============================================================
# 手动刷新
# ============================================================

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


# ============================================================
# 启动
# ============================================================

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

### `requirements.txt`

同时确认 GitHub 里的 `requirements.txt` 是：

```text id="91720f"
fastapi==0.117.1
uvicorn==0.36.0
requests==2.32.5
```

### 然后这样操作

**GitHub：**

1. 替换 `app.py`
2. 确认 `requirements.txt`
3. `Commit changes`

**Blitz：**

1. 等 GitHub 新 commit 出现
2. `Build again`
3. 等启动

这次正常的话，日志应该首先出现：

```text
[INFO] IPTV Aggregator starting...
[INFO] Starting refresh...
[INFO] Nodes: ...
[INFO] Priority keywords: ['北京', '联通']
```

然后逐个测试节点。

最重要的是：**即使北京联通节点返回 0 个频道，也会继续往后跑，不会像原来的程序一样直接结束。**

启动成功后，我们再访问：

```text
/health
```

和：

```text
/iptv
```

确认最终 Cinetry 使用的 M3U。

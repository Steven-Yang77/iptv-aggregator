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
# Configuration helpers
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


def env_list(name, default):
    raw = os.getenv(name, default)
    return [x.strip() for x in raw.split(",") if x.strip()]


def env_bool(name, default):
    return os.getenv(name, str(default)).lower() in ("1", "true", "yes", "on")


# ============================================================
# Configuration
# ============================================================

# 上游 M3U 地址（akiralereal/iptv 的输出）
UPSTREAM_M3U_URL = os.getenv(
    "UPSTREAM_M3U_URL",
    "https://iptv.dockers.blitz.cloud/interface.m3u"
)

# 地区关键词过滤（匹配 group-title 或频道名，为空则不过滤）
LOCATION_KEYWORDS = env_list("LOCATION_KEYWORDS", "")

# 运营商关键词过滤（匹配 group-title 或频道名，为空则不过滤）
ISP_KEYWORDS = env_list("ISP_KEYWORDS", "")

# 是否要求频道必须同时匹配地区和运营商关键词
FILTER_REQUIRE_BOTH = env_bool("FILTER_REQUIRE_BOTH", False)

# 刷新间隔（小时）
REFRESH_INTERVAL_HOURS = env_float(
    "REFRESH_INTERVAL_HOURS", 12.0, min_value=0.1
)

# 拉取上游 M3U 的超时与重试
UPSTREAM_TIMEOUT = env_int("UPSTREAM_TIMEOUT", 30, min_value=5)
UPSTREAM_RETRIES = env_int("UPSTREAM_RETRIES", 3, min_value=1)

# 单个流验证超时（秒）
VERIFY_TIMEOUT = env_int("VERIFY_TIMEOUT", 8, min_value=1)

# 并发验证线程数
VERIFY_WORKERS = env_int("VERIFY_WORKERS", 32, min_value=1)

# 每个频道最多保留多少个可用源
MAX_SOURCES_PER_CHANNEL = env_int(
    "MAX_SOURCES_PER_CHANNEL", 5, min_value=1
)

# 是否启用源有效性验证
ENABLE_VERIFY = env_bool("ENABLE_VERIFY", True)

# 是否启用后台自动刷新循环
ENABLE_REFRESH_LOOP = env_bool("ENABLE_REFRESH_LOOP", True)

# 手动刷新鉴权 token（可选）
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN")


# ============================================================
# HTTP Session（线程独立）
# ============================================================

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

_thread_local = threading.local()


def get_session():
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        s.headers.update({"User-Agent": UA})
        _thread_local.session = s
    return _thread_local.session


main_session = requests.Session()
main_session.headers.update({"User-Agent": UA})


# ============================================================
# Runtime state
# ============================================================

cached_m3u = "#EXTM3U\n"

last_update = None
last_error = None

last_upstream_channels = 0
last_after_filter = 0
last_verified_sources = 0
last_failed_sources = 0
last_final_channels = 0

refresh_in_progress = False
refresh_progress_done = 0
refresh_progress_total = 0

refresh_lock = threading.Lock()


# ============================================================
# Fetch upstream M3U
# ============================================================

def fetch_upstream_m3u():
    last_exc = None
    for attempt in range(1, UPSTREAM_RETRIES + 1):
        try:
            print(
                f"[INFO] Fetching upstream M3U "
                f"(attempt {attempt}/{UPSTREAM_RETRIES}): "
                f"{UPSTREAM_M3U_URL}"
            )
            r = main_session.get(
                UPSTREAM_M3U_URL,
                timeout=UPSTREAM_TIMEOUT
            )
            r.raise_for_status()
            text = r.content.decode(
                "utf-8-sig", errors="ignore"
            ).strip()

            if not text:
                raise RuntimeError("Upstream returned empty body")
            if "#EXTM3U" not in text:
                raise RuntimeError("Upstream returned non-M3U data")

            return text

        except Exception as e:
            last_exc = e
            print(f"[WARNING] Attempt {attempt} failed: {e}")
            if attempt < UPSTREAM_RETRIES:
                time.sleep(5)

    raise RuntimeError(
        f"Failed to fetch upstream M3U "
        f"after {UPSTREAM_RETRIES} attempts: {last_exc}"
    )


# ============================================================
# Parse M3U
# ============================================================

def parse_m3u(m3u_text):
    lines = [line.strip() for line in m3u_text.splitlines() if line.strip()]
    blocks = []
    current_block = []

    for line in lines:
        if line == "#EXTM3U":
            continue

        if not line.startswith("#"):
            if not current_block:
                continue
            if not any(item.startswith("#EXTINF:") for item in current_block):
                current_block = []
                continue
            blocks.append((current_block, line))
            current_block = []
        else:
            current_block.append(line)

    return blocks


# ============================================================
# Extract channel info
# ============================================================

def get_channel_info(block):
    display_name = ""
    key = ""
    group = ""

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

        m = re.search(r'group-title="([^"]*)"', line)
        if m and m.group(1).strip():
            group = m.group(1).strip()

        if not display_name and "," in line:
            name = line.split(",", 1)[1].strip()
            if name:
                display_name = name
                if not key:
                    key = name

    if not display_name:
        display_name = key or "Unknown"
    if not key:
        key = display_name

    return display_name, key, group


# ============================================================
# Keyword filter
# ============================================================

def match_keywords(text, keywords):
    if not keywords:
        return False
    text_lower = text.lower()
    return any(kw.lower() in text_lower for kw in keywords)


def passes_filter(display_name, group):
    text = f"{display_name} {group}"

    has_loc = bool(LOCATION_KEYWORDS)
    has_isp = bool(ISP_KEYWORDS)

    if not has_loc and not has_isp:
        return True

    matched_loc = match_keywords(text, LOCATION_KEYWORDS)
    matched_isp = match_keywords(text, ISP_KEYWORDS)

    if FILTER_REQUIRE_BOTH and has_loc and has_isp:
        return matched_loc and matched_isp

    return matched_loc or matched_isp


# ============================================================
# Stream verification
# ============================================================

def verify_stream(url):
    """验证单个流是否可用。返回 (ok, reason)。"""
    try:
        session = get_session()
        r = session.get(
            url,
            timeout=VERIFY_TIMEOUT,
            headers={"Range": "bytes=0-1"},
            stream=True,
            allow_redirects=True
        )
        try:
            if r.status_code in (200, 206):
                try:
                    next(r.iter_content(chunk_size=1), None)
                except Exception:
                    pass
                return True, None
            return False, f"HTTP {r.status_code}"
        finally:
            r.close()

    except requests.exceptions.Timeout:
        return False, "timeout"
    except requests.exceptions.ConnectionError:
        return False, "connection error"
    except Exception as e:
        return False, str(e)[:80]


def verify_one_source(index, url):
    ok, reason = verify_stream(url)
    return index, ok, reason


# ============================================================
# Build M3U
# ============================================================

def build_m3u():
    global last_upstream_channels
    global last_after_filter
    global last_verified_sources
    global last_failed_sources
    global last_final_channels
    global refresh_progress_done
    global refresh_progress_total

    # 1. 拉取上游
    m3u_text = fetch_upstream_m3u()
    blocks = parse_m3u(m3u_text)

    if not blocks:
        raise RuntimeError("Upstream M3U parsed 0 channels")

    last_upstream_channels = len(blocks)

    print(f"[INFO] Upstream channels: {len(blocks)}")
    print(f"[INFO] Location keywords: {LOCATION_KEYWORDS}")
    print(f"[INFO] ISP keywords: {ISP_KEYWORDS}")
    print(f"[INFO] Filter require both: {FILTER_REQUIRE_BOTH}")
    print(f"[INFO] Enable verify: {ENABLE_VERIFY}")
    print(f"[INFO] Verify workers: {VERIFY_WORKERS}")
    print(f"[INFO] Verify timeout: {VERIFY_TIMEOUT}s")
    print(f"[INFO] Max sources per channel: {MAX_SOURCES_PER_CHANNEL}")

    # 2. 关键词过滤
    filtered = []
    for block, url in blocks:
        display_name, channel_key, group = get_channel_info(block)
        if passes_filter(display_name, group):
            filtered.append((block, url, display_name, channel_key, group))

    last_after_filter = len(filtered)
    print(f"[INFO] After keyword filter: {len(filtered)}")

    # 3. 源有效性验证
    if ENABLE_VERIFY and filtered:
        refresh_progress_total = len(filtered)
        refresh_progress_done = 0

        verify_results = [None] * len(filtered)

        with ThreadPoolExecutor(
            max_workers=VERIFY_WORKERS,
            thread_name_prefix="verify"
        ) as executor:
            future_to_index = {
                executor.submit(verify_one_source, i, item[1]): i
                for i, item in enumerate(filtered)
            }
            for future in as_completed(future_to_index):
                idx, ok, reason = future.result()
                verify_results[idx] = (ok, reason)
                refresh_progress_done += 1
                if refresh_progress_done % 20 == 0 or \
                   refresh_progress_done == len(filtered):
                    print(
                        f"[INFO] Verify progress: "
                        f"{refresh_progress_done}/{len(filtered)}"
                    )
    else:
        refresh_progress_total = len(filtered)
        refresh_progress_done = len(filtered)
        verify_results = [(True, None) for _ in filtered]

    # 4. 组装结果
    all_blocks = []
    seen_urls = set()
    channel_source_count = {}

    verified_sources = 0
    failed_sources = 0
    duplicate_urls = 0
    limit_skipped = 0

    for i, (block, url, display_name, channel_key, group) in enumerate(filtered):
        ok, reason = verify_results[i] if verify_results[i] else (False, "not verified")

        if not ok:
            failed_sources += 1
            continue

        url = url.strip()
        if not url:
            continue

        if url in seen_urls:
            duplicate_urls += 1
            continue

        current_count = channel_source_count.get(channel_key, 0)
        if current_count >= MAX_SOURCES_PER_CHANNEL:
            limit_skipped += 1
            continue

        seen_urls.add(url)
        all_blocks.extend(block)
        all_blocks.append(url)

        channel_source_count[channel_key] = current_count + 1
        verified_sources += 1

    last_verified_sources = verified_sources
    last_failed_sources = failed_sources

    if not all_blocks:
        raise RuntimeError(
            "No channels survived verification/filter"
        )

    result = "#EXTM3U\n" + "\n".join(all_blocks) + "\n"
    channel_count = result.count("#EXTINF:")
    last_final_channels = channel_count

    print("=" * 60)
    print("[INFO] Build completed")
    print(f"[INFO] Upstream channels: {last_upstream_channels}")
    print(f"[INFO] After keyword filter: {last_after_filter}")
    print(f"[INFO] Verified sources: {verified_sources}")
    print(f"[INFO] Failed sources: {failed_sources}")
    print(f"[INFO] Duplicate URLs: {duplicate_urls}")
    print(f"[INFO] Source-limit skipped: {limit_skipped}")
    print(f"[INFO] Final channels: {channel_count}")
    print("=" * 60)

    return result


# ============================================================
# Refresh
# ============================================================

def refresh():
    global cached_m3u
    global last_update
    global last_error
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

        if last_error:
            sleep_seconds = 300
            print("[INFO] Last refresh failed, retry in 5 minutes.")
        else:
            sleep_seconds = REFRESH_INTERVAL_HOURS * 3600
            print(f"[INFO] Next refresh in {REFRESH_INTERVAL_HOURS} hours.")

        time.sleep(sleep_seconds)


# ============================================================
# FastAPI lifespan
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[INFO] IPTV Aggregator starting...")
    print(f"[INFO] Upstream M3U URL: {UPSTREAM_M3U_URL}")
    print(f"[INFO] Location keywords: {LOCATION_KEYWORDS}")
    print(f"[INFO] ISP keywords: {ISP_KEYWORDS}")
    print(f"[INFO] Refresh interval: {REFRESH_INTERVAL_HOURS} hours")
    print(f"[INFO] Verify enabled: {ENABLE_VERIFY}")
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
    version="2.0.0",
    lifespan=lifespan
)


@app.get("/")
def root():
    return {
        "service": "IPTV Aggregator Raymond",
        "status": "OK",
        "upstream_m3u_url": UPSTREAM_M3U_URL,
        "location_keywords": LOCATION_KEYWORDS,
        "isp_keywords": ISP_KEYWORDS,
        "filter_require_both": FILTER_REQUIRE_BOTH,
        "refresh_interval_hours": REFRESH_INTERVAL_HOURS,
        "verify_enabled": ENABLE_VERIFY,
        "verify_workers": VERIFY_WORKERS,
        "verify_timeout": VERIFY_TIMEOUT,
        "max_sources_per_channel": MAX_SOURCES_PER_CHANNEL,
        "refresh_loop_enabled": ENABLE_REFRESH_LOOP,
        "admin_token_required": bool(ADMIN_TOKEN),
        "last_update": last_update,
        "upstream_channels": last_upstream_channels,
        "after_filter": last_after_filter,
        "verified_sources": last_verified_sources,
        "failed_sources": last_failed_sources,
        "final_channels": last_final_channels,
        "error": last_error,
        "iptv_url": "/iptv"
    }


@app.get("/health")
def health():
    return {
        "status": "OK",
        "last_update": last_update,
        "upstream_channels": last_upstream_channels,
        "after_filter": last_after_filter,
        "verified_sources": last_verified_sources,
        "failed_sources": last_failed_sources,
        "final_channels": last_final_channels,
        "error": last_error,
        "refresh_in_progress": refresh_in_progress,
        "refresh_progress": f"{refresh_progress_done}/{refresh_progress_total}"
    }


@app.get("/iptv", response_class=PlainTextResponse)
def iptv():
    return cached_m3u


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
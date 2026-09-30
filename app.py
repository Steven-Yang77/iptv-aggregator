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


def env_bool(name, default):
    return os.getenv(name, str(default)).lower() in ("1", "true", "yes", "on")


# ============================================================
# Configuration
# ============================================================

UPSTREAM_M3U_URL = os.getenv(
    "UPSTREAM_M3U_URL",
    "https://iptv.dockers.blitz.cloud/interface.m3u"
)

REFRESH_INTERVAL_HOURS = env_float(
    "REFRESH_INTERVAL_HOURS", 12.0, min_value=0.1
)

UPSTREAM_TIMEOUT = env_int("UPSTREAM_TIMEOUT", 30, min_value=5)
UPSTREAM_RETRIES = env_int("UPSTREAM_RETRIES", 3, min_value=1)

# 单个源验证超时（秒）
VERIFY_TIMEOUT = env_int("VERIFY_TIMEOUT", 8, min_value=1)

# 每个源验证时的重试次数
VERIFY_RETRIES = env_int("VERIFY_RETRIES", 2, min_value=1)

# 并发验证线程数
VERIFY_WORKERS = env_int("VERIFY_WORKERS", 32, min_value=1)

# 每个频道保留的性能最好的源数量
MAX_SOURCES_PER_CHANNEL = env_int(
    "MAX_SOURCES_PER_CHANNEL", 5, min_value=1
)

# 是否打印每个源的验证结果（调试用，源多时日志会很大）
VERBOSE_VERIFY = env_bool("VERBOSE_VERIFY", False)

ENABLE_REFRESH_LOOP = env_bool("ENABLE_REFRESH_LOOP", True)

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
    """每个线程独立 Session，避免 requests.Session 的线程安全问题。"""
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

last_upstream_sources = 0
last_verified_sources = 0
last_failed_sources = 0
last_unique_channels = 0
last_final_sources = 0
last_dropped_sources = 0

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
    """
    解析 M3U 内容，返回 [(block_lines, url), ...]。
    block_lines 是 #EXTINF 及其他 # 开头的行。
    """
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

def normalize_key(text):
    """规范化频道标识，用于分组。"""
    if not text:
        return ""
    text = re.sub(r'[\s_\-]+', '', text)
    return text.lower()


def get_channel_info(block):
    """
    从 #EXTINF 中提取频道信息。

    返回 (display_name, channel_key, group)
    """
    display_name = ""
    tvg_id = ""
    tvg_name = ""
    group = ""

    for line in block:
        if not line.startswith("#EXTINF:"):
            continue

        m = re.search(r'tvg-id="([^"]*)"', line)
        if m:
            tvg_id = m.group(1).strip()

        m = re.search(r'tvg-name="([^"]*)"', line)
        if m:
            tvg_name = m.group(1).strip()

        m = re.search(r'group-title="([^"]*)"', line)
        if m:
            group = m.group(1).strip()

        # 逗号后的显示名
        if "," in line:
            name = line.split(",", 1)[1].strip()
            if name:
                display_name = name

    # display_name 优先级：逗号后名称 > tvg-name > tvg-id
    if not display_name:
        display_name = tvg_name or tvg_id or "Unknown"

    # channel_key 优先级：tvg-name > tvg-id > display_name
    # tvg-name 通常比 tvg-id 更稳定（tvg-id 有时不同源写法不同）
    key_source = tvg_name or tvg_id or display_name
    channel_key = normalize_key(key_source)

    if not channel_key:
        channel_key = normalize_key(display_name)

    return display_name, channel_key, group


# ============================================================
# Stream verification
# ============================================================

# 判定为可播放的 Content-Type 关键字
VIDEO_CONTENT_TYPES = (
    "video/",
    "audio/",
    "application/vnd.apple.mpegurl",
    "application/x-mpegurl",
    "application/mpegurl",
    "application/octet-stream",
    "application/x-mpegts",
    "mpegurl",
)


def is_media_content(chunk, content_type):
    """
    判断响应内容是否是媒体流。

    返回 True 表示可播放。
    """
    ct = (content_type or "").lower()

    # 1. 明确的视频/音频 Content-Type
    if any(k in ct for k in VIDEO_CONTENT_TYPES):
        return True

    # 2. 内容本身是 m3u8
    if chunk:
        # 尝试解码头部判断
        head = chunk[:4096].decode("utf-8", errors="ignore")
        if "#EXTM3U" in head or "#EXT-X-" in head:
            return True

        # 3. TS 流：以 0x47 同步字节开头，或包含 PAT/PMT 表
        if len(chunk) >= 1 and chunk[0:1] == b"\x47":
            return True

        # 4. 二进制流，包含 TS 包标志
        if b"\x47" in chunk[:188] and b"PAT" in chunk[:1024]:
            return True

    return False


def verify_stream(url):
    """
    验证单个源地址是否可用，并返回响应耗时。

    返回 (ok, score, reason, content_type, size)
      ok: 是否可用
      score: 首字节耗时（秒），失败时为 inf
      reason: 失败原因
      content_type: 响应的 Content-Type
      size: 已读取字节数
    """
    start = time.time()
    last_reason = "unknown"

    for attempt in range(1, VERIFY_RETRIES + 1):
        try:
            session = get_session()
            r = session.get(
                url,
                timeout=VERIFY_TIMEOUT,
                stream=True,
                allow_redirects=True,
                headers={"User-Agent": UA}
            )

            try:
                # 跟随重定向后仍是 3xx，说明是最终跳转地址
                if r.status_code in (301, 302, 303, 307, 308):
                    # 保守起见认为有效
                    return (
                        True,
                        time.time() - start,
                        None,
                        r.headers.get("Content-Type", ""),
                        0
                    )

                if r.status_code not in (200, 206):
                    last_reason = f"HTTP {r.status_code}"
                    if attempt < VERIFY_RETRIES:
                        time.sleep(1)
                        continue
                    return False, float("inf"), last_reason, "", 0

                # 读取一小段内容判断是否是媒体
                chunk = b""
                max_read = 8192
                for piece in r.iter_content(chunk_size=1024):
                    if piece:
                        chunk += piece
                    if len(chunk) >= max_read:
                        break
                    # 读取足够判断即可停止
                    if len(chunk) >= 4096:
                        break

                elapsed = time.time() - start
                content_type = r.headers.get("Content-Type", "")

                if not chunk:
                    last_reason = "empty body"
                    if attempt < VERIFY_RETRIES:
                        time.sleep(1)
                        continue
                    return False, float("inf"), last_reason, content_type, 0

                if is_media_content(chunk, content_type):
                    return True, elapsed, None, content_type, len(chunk)

                # 内容看起来不是媒体
                head_preview = chunk[:100].decode("utf-8", errors="ignore")
                last_reason = f"not media ({content_type or 'no ct'}, head={head_preview[:40]!r})"

                if attempt < VERIFY_RETRIES:
                    time.sleep(1)
                    continue
                return False, float("inf"), last_reason, content_type, len(chunk)

            finally:
                r.close()

        except requests.exceptions.Timeout:
            last_reason = "timeout"
            if attempt < VERIFY_RETRIES:
                time.sleep(1)
                continue
            return False, float("inf"), last_reason, "", 0

        except requests.exceptions.ConnectionError as e:
            last_reason = f"connection error: {str(e)[:60]}"
            if attempt < VERIFY_RETRIES:
                time.sleep(1)
                continue
            return False, float("inf"), last_reason, "", 0

        except Exception as e:
            last_reason = f"{type(e).__name__}: {str(e)[:60]}"
            if attempt < VERIFY_RETRIES:
                time.sleep(1)
                continue
            return False, float("inf"), last_reason, "", 0

    return False, float("inf"), last_reason, "", 0


def verify_one_source(index, url):
    ok, score, reason, content_type, size = verify_stream(url)
    return index, ok, score, reason, content_type, size


# ============================================================
# Build M3U
# ============================================================

def build_m3u():
    global last_upstream_sources
    global last_verified_sources
    global last_failed_sources
    global last_unique_channels
    global last_final_sources
    global last_dropped_sources
    global refresh_progress_done
    global refresh_progress_total

    # 1. 拉取上游 M3U
    m3u_text = fetch_upstream_m3u()
    blocks = parse_m3u(m3u_text)

    if not blocks:
        raise RuntimeError("Upstream M3U parsed 0 channels")

    last_upstream_sources = len(blocks)

    print(f"[INFO] Upstream sources: {len(blocks)}")
    print(f"[INFO] Verify workers: {VERIFY_WORKERS}")
    print(f"[INFO] Verify timeout: {VERIFY_TIMEOUT}s")
    print(f"[INFO] Verify retries: {VERIFY_RETRIES}")
    print(f"[INFO] Max sources per channel: {MAX_SOURCES_PER_CHANNEL}")

    # 2. 并发验证所有源
    refresh_progress_total = len(blocks)
    refresh_progress_done = 0

    verify_results = [None] * len(blocks)

    with ThreadPoolExecutor(
        max_workers=VERIFY_WORKERS,
        thread_name_prefix="verify"
    ) as executor:
        future_to_index = {
            executor.submit(verify_one_source, i, block_item[1]): i
            for i, block_item in enumerate(blocks)
        }
        for future in as_completed(future_to_index):
            idx, ok, score, reason, content_type, size = future.result()
            verify_results[idx] = (ok, score, reason, content_type, size)
            refresh_progress_done += 1

            if VERBOSE_VERIFY:
                url = blocks[idx][1]
                status = "OK " if ok else "FAIL"
                print(
                    f"[VERIFY] {status} {url} "
                    f"({reason if reason else f'{score:.2f}s, {content_type}'})"
                )

            if refresh_progress_done % 50 == 0 or \
               refresh_progress_done == len(blocks):
                print(
                    f"[INFO] Verify progress: "
                    f"{refresh_progress_done}/{len(blocks)}"
                )

    # 3. 按频道分组，收集可用源
    #    channel_map: channel_key -> {
    #        "block": block,
    #        "display_name": display_name,
    #        "group": group,
    #        "sources": [(score, url), ...]
    #    }
    channel_map = {}

    verified_sources = 0
    failed_sources = 0
    failure_reasons = {}

    for i, (block, url) in enumerate(blocks):
        result = verify_results[i] if verify_results[i] else (False, float("inf"), "not verified", "", 0)
        ok, score, reason, content_type, size = result

        if not ok:
            failed_sources += 1
            # 统计失败原因
            rkey = reason.split("(")[0].strip() if reason else "unknown"
            failure_reasons[rkey] = failure_reasons.get(rkey, 0) + 1
            continue

        verified_sources += 1

        display_name, channel_key, group = get_channel_info(block)

        if not channel_key:
            channel_key = normalize_key(url) or url

        if channel_key not in channel_map:
            channel_map[channel_key] = {
                "block": block,
                "display_name": display_name,
                "group": group,
                "sources": []
            }

        channel_map[channel_key]["sources"].append((score, url))

    last_verified_sources = verified_sources
    last_failed_sources = failed_sources

    print(f"[INFO] Verified OK: {verified_sources}")
    print(f"[INFO] Failed: {failed_sources}")

    if failure_reasons:
        print("[INFO] Failure reasons:")
        for reason, count in sorted(
            failure_reasons.items(),
            key=lambda x: -x[1]
        ):
            print(f"       {count:5d}  {reason}")

    if not channel_map:
        raise RuntimeError("No sources survived verification")

    # 4. 每个频道按性能排序，只保留前 N 个源
    all_blocks = []
    total_kept = 0
    total_dropped = 0

    for channel_key, info in channel_map.items():
        sources = info["sources"]

        # 按耗时升序排序，耗时小的性能好
        sources.sort(key=lambda x: x[0])

        # 只保留前 MAX_SOURCES_PER_CHANNEL 个
        kept = sources[:MAX_SOURCES_PER_CHANNEL]
        dropped = len(sources) - len(kept)

        total_kept += len(kept)
        total_dropped += dropped

        block = info["block"]

        for score, url in kept:
            all_blocks.extend(block)
            all_blocks.append(url)

    last_unique_channels = len(channel_map)
    last_final_sources = total_kept
    last_dropped_sources = total_dropped

    # 5. 组装结果
    result = "#EXTM3U\n" + "\n".join(all_blocks) + "\n"
    channel_count = result.count("#EXTINF:")

    print("=" * 60)
    print("[INFO] Build completed")
    print(f"[INFO] Upstream sources: {last_upstream_sources}")
    print(f"[INFO] Verified OK: {verified_sources}")
    print(f"[INFO] Failed: {failed_sources}")
    print(f"[INFO] Unique channels: {last_unique_channels}")
    print(f"[INFO] Sources kept: {total_kept}")
    print(f"[INFO] Sources dropped (over limit): {total_dropped}")
    print(f"[INFO] Final EXTINF count: {channel_count}")
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
    print(f"[INFO] Refresh interval: {REFRESH_INTERVAL_HOURS} hours")
    print(f"[INFO] Verify workers: {VERIFY_WORKERS}")
    print(f"[INFO] Verify timeout: {VERIFY_TIMEOUT}s")
    print(f"[INFO] Verify retries: {VERIFY_RETRIES}")
    print(f"[INFO] Max sources per channel: {MAX_SOURCES_PER_CHANNEL}")
    print(f"[INFO] Verbose verify: {VERBOSE_VERIFY}")
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
    version="2.2.0",
    lifespan=lifespan
)


@app.get("/")
def root():
    return {
        "service": "IPTV Aggregator Raymond",
        "status": "OK",
        "upstream_m3u_url": UPSTREAM_M3U_URL,
        "refresh_interval_hours": REFRESH_INTERVAL_HOURS,
        "verify_workers": VERIFY_WORKERS,
        "verify_timeout": VERIFY_TIMEOUT,
        "verify_retries": VERIFY_RETRIES,
        "max_sources_per_channel": MAX_SOURCES_PER_CHANNEL,
        "verbose_verify": VERBOSE_VERIFY,
        "refresh_loop_enabled": ENABLE_REFRESH_LOOP,
        "admin_token_required": bool(ADMIN_TOKEN),
        "last_update": last_update,
        "upstream_sources": last_upstream_sources,
        "verified_sources": last_verified_sources,
        "failed_sources": last_failed_sources,
        "unique_channels": last_unique_channels,
        "final_sources": last_final_sources,
        "dropped_sources": last_dropped_sources,
        "error": last_error,
        "iptv_url": "/iptv"
    }


@app.get("/health")
def health():
    return {
        "status": "OK",
        "last_update": last_update,
        "upstream_sources": last_upstream_sources,
        "verified_sources": last_verified_sources,
        "failed_sources": last_failed_sources,
        "unique_channels": last_unique_channels,
        "final_sources": last_final_sources,
        "dropped_sources": last_dropped_sources,
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
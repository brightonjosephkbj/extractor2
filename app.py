"""
app.py — Railway version, adapted from extract_server.py v4
- /health: health check
- /search: metadata-only search by song/video name (no download)
- /extract: runs yt-dlp, returns a stream_url instead of the file itself
- /media/<job_id>: serves the extracted file with HTTP range support
- /info: fast metadata-only lookup for a direct URL
- Background cleanup of job folders older than 1 hour
"""

import os
import json
import shutil
import subprocess
import tempfile
import threading
import time
import uuid
import hmac
import secrets
import urllib.parse
import re
from flask import Flask, request, jsonify, send_file

app = Flask(__name__)

PORT = int(os.environ.get("PORT", 8000))
SHARED_SECRET = os.environ.get("EXTRACT_SHARED_SECRET", "change-me")
if SHARED_SECRET == "change-me":
    raise RuntimeError("Set EXTRACT_SHARED_SECRET env var")
MAX_AGE_SECONDS = 3600
CLEANUP_INTERVAL = 600

AUDIO_QUALITY_MAP = {"low": "128", "medium": "192", "high": "256", "max": "320"}
VIDEO_QUALITY_MAP = {"low": 480, "medium": 720, "high": 1080, "max": 2160}

JOBS_ROOT = tempfile.gettempdir()

EXTRACT_SLOTS = threading.Semaphore(int(os.environ.get("MAX_EXTRACTS", "3")))
SEARCH_SLOTS = threading.Semaphore(int(os.environ.get("MAX_SEARCHES", "4")))

# Cookies not set up yet - only used if this file happens to exist later
YTDLP_COOKIES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cookies.txt")
_ck = os.environ.get("YT_COOKIES")
if _ck:
    YTDLP_COOKIES = "/tmp/yt_cookies.txt"
    with open(YTDLP_COOKIES, "w") as _f:
        _f.write(_ck.replace("~T~", "\t").replace("~N~", "\n"))
_b64 = os.environ.get("YT_COOKIES_B64")
if _b64:
    import base64
    YTDLP_COOKIES = "/tmp/yt_cookies.txt"
    with open(YTDLP_COOKIES, "wb") as _f:
        _f.write(base64.b64decode(_b64))
_gz = os.environ.get("YT_COOKIES_GZ_B64")
if _gz:
    import base64, gzip
    YTDLP_COOKIES = "/tmp/yt_cookies.txt"
    with open(YTDLP_COOKIES, "wb") as _f:
        _f.write(gzip.decompress(base64.b64decode(_gz)))
YTDLP_POT_ARGS = []

# YouTube now requires running some of its player JS to derive stream
# signatures. yt-dlp needs a JS runtime for this - Deno is installed via
# the Dockerfile. Without this flag yt-dlp still tries to auto-detect one,
# but being explicit avoids "No supported JavaScript runtime could be
# found" if detection ever fails silently.
JS_RUNTIME_ARGS = ["--js-runtimes", "deno"]


def cookie_args(work_dir=None):
    if not os.path.exists(YTDLP_COOKIES):
        return []
    path = YTDLP_COOKIES
    if work_dir:
        path = os.path.join(work_dir, "cookies.txt")
        shutil.copy(YTDLP_COOKIES, path)
    return ["--cookies", path]


def enhance_audio(input_path, output_path):
    af = (
        "equalizer=f=2500:t=q:w=0.7:g=3,"
        "volume=6dB,"
        "alimiter=limit=0.891:release=100"
    )
    ext = output_path.rsplit(".", 1)[-1].lower()
    if ext == "mp3":
        codec = ["-c:a", "libmp3lame", "-q:a", "2"]
    elif ext in ("webm", "opus", "ogg"):
        codec = ["-c:a", "libopus", "-b:a", "160k"]
    else:
        codec = ["-c:a", "aac", "-b:a", "192k"]
    cmd = ["ffmpeg", "-y", "-i", input_path, "-vn", "-af", af, *codec, output_path]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if r.returncode != 0:
        raise RuntimeError(f"audio enhance failed: {r.stderr[-1000:]}")


def enhance_video(input_path, output_path, target_height=2160):
    target_width = int(target_height * 16 / 9)
    vf = (
        f"scale={target_width}:{target_height}:flags=lanczos,"
        f"unsharp=5:5:1.0:5:5:0.0"
    )
    cmd = ["ffmpeg", "-y", "-i", input_path, "-vf", vf, "-c:a", "copy", output_path]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if result.returncode != 0:
        raise RuntimeError(f"video enhance failed: {result.stderr[-1000:]}")


def check_auth(req):
    return req.headers.get("X-Auth-Token") == SHARED_SECRET


def cleanup_old_jobs():
    while True:
        try:
            now = time.time()
            for name in os.listdir(JOBS_ROOT):
                if not name.startswith("extract_"):
                    continue
                full_path = os.path.join(JOBS_ROOT, name)
                if not os.path.isdir(full_path):
                    continue
                age = now - os.path.getmtime(full_path)
                if age > MAX_AGE_SECONDS:
                    shutil.rmtree(full_path, ignore_errors=True)
                    print(f"[cleanup] removed stale job dir: {name} (age={int(age)}s)")
        except Exception as e:
            print(f"[cleanup] error during sweep: {e}")
        time.sleep(CLEANUP_INTERVAL)


# Starts on import so it also runs under gunicorn, not just `python app.py`
threading.Thread(target=cleanup_old_jobs, daemon=True).start()


# --- optional keep-busy: off unless KEEP_BUSY is set (self,outbound,cpu) ----
KEEP_BUSY = {m.strip() for m in os.environ.get("KEEP_BUSY", "").split(",") if m.strip()}
KEEP_BUSY_EVERY = int(os.environ.get("KEEP_BUSY_EVERY", "240"))
KEEP_BUSY_QUIET = int(os.environ.get("KEEP_BUSY_QUIET", "60"))
KEEP_BUSY_CPU_SECONDS = int(os.environ.get("KEEP_BUSY_CPU_SECONDS", "5"))
_active_requests = 0
_last_request_at = time.time()
_busy_lock = threading.Lock()


@app.before_request
def _note_request_start():
    global _active_requests, _last_request_at
    if request.headers.get("X-Keep-Busy"):
        return
    with _busy_lock:
        _active_requests += 1
        _last_request_at = time.time()


@app.teardown_request
def _note_request_end(exc=None):
    global _active_requests, _last_request_at
    if request.headers.get("X-Keep-Busy"):
        return
    with _busy_lock:
        _active_requests = max(0, _active_requests - 1)
        _last_request_at = time.time()


def _is_idle():
    with _busy_lock:
        return _active_requests == 0 and time.time() - _last_request_at >= KEEP_BUSY_QUIET


def _busy_loop():
    import urllib.request
    public_url = os.environ.get("PUBLIC_URL", "").rstrip("/")
    while True:
        time.sleep(KEEP_BUSY_EVERY)
        if not _is_idle():
            continue
        try:
            if "self" in KEEP_BUSY and public_url:
                req = urllib.request.Request(public_url + "/health",
                                             headers={"X-Keep-Busy": "1", "User-Agent": "Mozilla/5.0"})
                urllib.request.urlopen(req, timeout=20).read()
            if "outbound" in KEEP_BUSY:
                urllib.request.urlopen("https://example.com", timeout=15).read()
            if "cpu" in KEEP_BUSY:
                end = time.time() + KEEP_BUSY_CPU_SECONDS
                while time.time() < end and _is_idle():
                    sum(i * i for i in range(20000))
            print(f"[keep-busy] tick {sorted(KEEP_BUSY)}", flush=True)
        except Exception as e:
            print(f"[keep-busy] {type(e).__name__}: {e}", flush=True)


if KEEP_BUSY:
    threading.Thread(target=_busy_loop, daemon=True).start()


@app.route("/health")
def health():
    return jsonify({"status": "ok"}), 200


def _search_work():
    if not check_auth(request):
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    query = data.get("query")
    limit = min(int(data.get("limit", 5)), 10)

    if not query:
        return jsonify({"error": "missing 'query'"}), 400

    cmd = [
        "yt-dlp", f"ytsearch{limit}:{query}",
        *cookie_args(), *YTDLP_POT_ARGS, *JS_RUNTIME_ARGS,
        "--flat-playlist", "--dump-json", "--no-warnings",
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    except subprocess.TimeoutExpired:
        return jsonify({"error": "search timed out"}), 504

    if result.returncode != 0:
        return jsonify({"error": "search failed", "stderr": result.stderr[-1000:]}), 502

    results = []
    for line in result.stdout.strip().splitlines():
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        results.append({
            "id": entry.get("id"),
            "title": entry.get("title"),
            "uploader": entry.get("uploader") or entry.get("channel"),
            "duration": entry.get("duration"),
            "thumbnail": entry.get("thumbnails", [{}])[-1].get("url") if entry.get("thumbnails") else None,
            "url": entry.get("url") or f"https://youtu.be/{entry.get('id')}",
        })

    return jsonify({"results": results})


@app.route("/search", methods=["POST"])
def search():
    if not check_auth(request):
        return jsonify({"error": "unauthorized"}), 401
    if not SEARCH_SLOTS.acquire(blocking=False):
        return jsonify({"error": "server busy, try again shortly"}), 429
    try:
        return _search_work()
    finally:
        SEARCH_SLOTS.release()


def _extract_work(data=None):

    if data is None:
        data = request.get_json(silent=True) or {}
    url = data.get("url")
    mode = data.get("mode", "audio")
    quality = data.get("quality", "medium")

    if not url:
        return jsonify({"error": "missing 'url'"}), 400
    if mode not in ("audio", "video"):
        return jsonify({"error": "mode must be 'audio' or 'video'"}), 400

    job_id = uuid.uuid4().hex
    work_dir = os.path.join(JOBS_ROOT, f"extract_{job_id}")
    os.makedirs(work_dir, exist_ok=True)
    out_template = os.path.join(work_dir, "output.%(ext)s")
    info_json_path = os.path.join(work_dir, "output.info.json")

    audio_format = (data.get("audio_format") or "m4a").lower()
    if mode == "audio" and audio_format == "mp3":
        bitrate = AUDIO_QUALITY_MAP.get(quality, "192")
        cmd = [
            "yt-dlp", "-x", "--audio-format", "mp3",
            *cookie_args(work_dir), *YTDLP_POT_ARGS, *JS_RUNTIME_ARGS,
            "--audio-quality", bitrate + "K",
            "--write-info-json", "-o", out_template, url,
        ]
        expected_path = os.path.join(work_dir, "output.mp3")
        mimetype = "audio/mpeg"
        download_ext = "mp3"
    elif mode == "audio":
        cmd = [
            "yt-dlp", "-f", "bestaudio[ext=m4a]/bestaudio",
            *cookie_args(work_dir), *YTDLP_POT_ARGS, *JS_RUNTIME_ARGS,
            "--write-info-json", "-o", out_template, url,
        ]
        expected_path = None
        mimetype = "audio/mp4"
        download_ext = "m4a"
    else:
        height = VIDEO_QUALITY_MAP.get(quality, 720)
        fmt = f"bv*[height<={height}]+ba/b[height<={height}]/b"
        cmd = [
            "yt-dlp", "-f", fmt,
            *cookie_args(work_dir), *YTDLP_POT_ARGS, *JS_RUNTIME_ARGS,
            "--merge-output-format", "mp4",
            "--write-info-json", "-o", out_template, url,
        ]
        expected_path = os.path.join(work_dir, "output.mp4")
        mimetype = "video/mp4"
        download_ext = "mp4"

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    except subprocess.TimeoutExpired:
        return jsonify({"error": "extraction timed out"}), 504

    if result.returncode != 0:
        print(f"[extract] yt-dlp failed for mode={mode} url={url}: {result.stderr[-1000:]}")
        return jsonify({"error": "yt-dlp failed", "stderr": result.stderr[-2000:]}), 422

    if expected_path is None:
        found = [f for f in os.listdir(work_dir)
                 if f.startswith("output.") and not f.endswith((".json", ".part", ".ytdl"))]
        if found:
            expected_path = os.path.join(work_dir, found[0])
            download_ext = found[0].rsplit(".", 1)[-1].lower()
            mimetype = {"m4a": "audio/mp4", "mp4": "audio/mp4", "webm": "audio/webm",
                        "opus": "audio/ogg", "ogg": "audio/ogg",
                        "mp3": "audio/mpeg"}.get(download_ext, "application/octet-stream")

    if not expected_path or not os.path.exists(expected_path):
        return jsonify({"error": f"extraction succeeded but {download_ext} not found"}), 500

    if data.get("enhance"):
        enhanced_path = os.path.join(work_dir, f"enhanced.{download_ext}")
        try:
            if mode == "audio":
                enhance_audio(expected_path, enhanced_path)
            else:
                enhance_video(expected_path, enhanced_path)
            os.replace(enhanced_path, expected_path)
        except Exception as e:
            print(f"[extract] enhance failed, serving original: {e}")

    title = "download"
    try:
        if os.path.exists(info_json_path):
            with open(info_json_path, "r") as f:
                title = json.load(f).get("title", "download")
    except Exception as e:
        print(f"[extract] could not read info.json: {e}")

    job_token = secrets.token_urlsafe(24)
    media_meta = {"mimetype": mimetype, "ext": download_ext, "title": title, "token": job_token}
    with open(os.path.join(work_dir, "media.json"), "w") as f:
        json.dump(media_meta, f)

    host = request.host_url.rstrip("/").replace("http://", "https://")
    stream_url = f"{host}/media/{job_id}?token={job_token}"

    return jsonify({
        "job_id": job_id,
        "stream_url": stream_url,
        "title": title,
        "mimetype": mimetype,
        "ext": download_ext,
    })


@app.route("/extract", methods=["POST"])
def extract():
    if not check_auth(request):
        return jsonify({"error": "unauthorized"}), 401
    if not EXTRACT_SLOTS.acquire(timeout=60):
        return jsonify({"error": "server busy, try again shortly"}), 429
    try:
        return _extract_work()
    finally:
        EXTRACT_SLOTS.release()


import re as _re

_BAD_WORDS = ("remix", "sped up", "speed up", "slowed", "nightcore", "cover",
              "mashup", "bass boosted", "8d", "reverb", "karaoke",
              "instrumental", "live")


def _words(text):
    return [w for w in _re.findall(r"[a-z0-9]+", (text or "").lower()) if len(w) > 1]


def _flat_search(prefix, query, n):
    cmd = ["yt-dlp", f"{prefix}{n}:{query}", "--flat-playlist", "--dump-json",
           "--no-warnings", "--ignore-errors", "--playlist-end", str(n)]
    if prefix == "ytsearch":
        cmd += [*cookie_args(), *YTDLP_POT_ARGS, *JS_RUNTIME_ARGS]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    except Exception:
        return []
    out = []
    for line in r.stdout.splitlines():
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    return out


def _pick_soundcloud(query):
    qw = set(_words(query))
    ql = query.lower()
    for e in _flat_search("scsearch", query, 5):
        url = e.get("webpage_url") or e.get("original_url") or e.get("url")
        dur = e.get("duration")
        title = e.get("title") or ""
        if not url or dur is None or not (90 <= dur <= 600):
            continue
        have = set(_words(title + " " + (e.get("uploader") or "")))
        if qw and len(qw & have) / len(qw) < 0.6:
            continue
        tl = title.lower()
        if any(_re.search(r"\\b" + _re.escape(b) + r"\\b", tl) and b not in ql
               for b in _BAD_WORDS):
            continue
        return url
    return None


def _pick_youtube(query):
    for e in _flat_search("ytsearch", query, 1):
        url = e.get("webpage_url") or e.get("url")
        if not url and e.get("id"):
            url = "https://www.youtube.com/watch?v=" + e["id"]
        if url:
            return url
    return None


def _parts(resp):
    if isinstance(resp, tuple):
        return resp[0], resp[1]
    return resp, resp.status_code


@app.route("/extract/smart", methods=["POST"])
def extract_smart():
    if not check_auth(request):
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    query = str(data.get("query") or "").strip()
    if not query:
        return jsonify({"error": "missing 'query'"}), 400
    if not EXTRACT_SLOTS.acquire(timeout=120):
        return jsonify({"error": "server busy, try again shortly"}), 429
    try:
        base = {"mode": "audio",
                "quality": data.get("quality", "medium"),
                "audio_format": data.get("audio_format", "m4a"),
                "enhance": data.get("enhance", False)}
        sc = _pick_soundcloud(query)
        if sc:
            r, code = _parts(_extract_work({**base, "url": sc}))
            if code == 200:
                body = r.get_json()
                body["source"] = "soundcloud"
                body["matched_url"] = sc
                return jsonify(body)
        yt = _pick_youtube(query)
        if not yt:
            return jsonify({"error": "no results found for that query"}), 404
        r, code = _parts(_extract_work({**base, "url": yt}))
        if code == 200:
            body = r.get_json()
            body["source"] = "youtube"
            body["matched_url"] = yt
            return jsonify(body)
        return r, code
    finally:
        EXTRACT_SLOTS.release()


@app.route("/media/<job_id>")
def media(job_id):
    token = request.args.get("token", "")

    work_dir = os.path.join(JOBS_ROOT, f"extract_{job_id}")
    meta_path = os.path.join(work_dir, "media.json")
    if not os.path.exists(meta_path):
        return jsonify({"error": "job not found or expired"}), 404

    with open(meta_path) as f:
        meta = json.load(f)

    expected = str(meta.get("token", ""))
    if not expected or not hmac.compare_digest(token.encode(), expected.encode()):
        return jsonify({"error": "unauthorized"}), 401

    file_path = os.path.join(work_dir, f"output.{meta['ext']}")
    if not os.path.exists(file_path):
        return jsonify({"error": "media file missing"}), 404

    return send_file(file_path, mimetype=meta["mimetype"], conditional=True)


@app.route("/info", methods=["POST"])
def info():
    if not check_auth(request):
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    url = data.get("url")
    if not url:
        return jsonify({"error": "missing 'url'"}), 400

    cmd = ["yt-dlp", "--dump-json", "--no-warnings", *cookie_args(), *YTDLP_POT_ARGS, *JS_RUNTIME_ARGS, "--skip-download", url]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
    except subprocess.TimeoutExpired:
        return jsonify({"error": "info lookup timed out"}), 504

    if result.returncode != 0:
        return jsonify({"error": "info lookup failed", "stderr": result.stderr[-1000:]}), 502

    try:
        info_data = json.loads(result.stdout.strip().splitlines()[0])
    except (json.JSONDecodeError, IndexError):
        return jsonify({"error": "could not parse yt-dlp output"}), 502

    return jsonify({
        "title": info_data.get("title"),
        "uploader": info_data.get("uploader") or info_data.get("channel"),
        "duration": info_data.get("duration"),
        "thumbnail": info_data.get("thumbnail"),
        "webpage_url": info_data.get("webpage_url", url),
        "extractor": info_data.get("extractor"),
    })



import requests

SPOTIFY_ID_RE = re.compile(r"playlist/([a-zA-Z0-9]+)")


def _deep_find_tracks(obj):
    """Fallback: recursively search the JSON for anything shaped like a track list,
    in case Spotify's structure differs from the known path (region/A-B testing)."""
    if isinstance(obj, dict):
        if "trackList" in obj and isinstance(obj["trackList"], list):
            return obj["trackList"]
        for v in obj.values():
            found = _deep_find_tracks(v)
            if found:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = _deep_find_tracks(item)
            if found:
                return found
    return None


@app.route("/spotify", methods=["POST"])
def spotify_playlist():
    if not check_auth(request):
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    url = data.get("url")
    if not url:
        return jsonify({"error": "missing 'url'"}), 400

    match = SPOTIFY_ID_RE.search(url)
    if not match:
        return jsonify({"error": "could not extract playlist id from url"}), 400
    playlist_id = match.group(1)

    embed_url = f"https://open.spotify.com/embed/playlist/{playlist_id}"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}

    try:
        resp = requests.get(embed_url, headers=headers, timeout=15)
    except requests.RequestException as e:
        return jsonify({"error": f"fetch failed: {e}"}), 502

    next_data_match = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.+?)</script>',
        resp.text,
    )
    if not next_data_match:
        return jsonify({
            "error": "could not find __NEXT_DATA__ in embed page",
            "html_snippet": resp.text[:1000],
        }), 502

    try:
        next_data = json.loads(next_data_match.group(1))
    except json.JSONDecodeError:
        return jsonify({"error": "could not parse playlist json"}), 502

    entity = (
        next_data.get("props", {})
        .get("pageProps", {})
        .get("state", {})
        .get("data", {})
        .get("entity", {})
    )

    if not entity:
        return jsonify({
            "error": "unexpected embed page structure",
            "top_level_keys": list(next_data.keys()),
        }), 502

    tracks = entity.get("trackList") or _deep_find_tracks(next_data) or []

    results = []
    for t in tracks:
        results.append({
            "title": t.get("title"),
            "subtitle": t.get("subtitle"),
            "duration_ms": t.get("duration"),
            "uri": t.get("uri"),
        })

    return jsonify({
        "playlist_title": entity.get("title") or entity.get("name"),
        "cover_art": entity.get("coverArt", {}).get("sources", [{}])[-1].get("url") if entity.get("coverArt") else None,
        "track_count": len(results),
        "tracks": results,
    })



@app.route("/spotify/debug", methods=["POST"])
def spotify_debug():
    if not check_auth(request):
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    url = data.get("url")
    if not url:
        return jsonify({"error": "missing 'url'"}), 400

    match = SPOTIFY_ID_RE.search(url)
    if not match:
        return jsonify({"error": "could not extract playlist id"}), 400
    playlist_id = match.group(1)

    embed_url = f"https://open.spotify.com/embed/playlist/{playlist_id}"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    resp = requests.get(embed_url, headers=headers, timeout=15)

    next_data_match = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.+?)</script>',
        resp.text,
    )
    if not next_data_match:
        return jsonify({"error": "no next_data found"}), 502

    next_data = json.loads(next_data_match.group(1))
    entity = (
        next_data.get("props", {})
        .get("pageProps", {})
        .get("state", {})
        .get("data", {})
        .get("entity", {})
    )

    # Return entity's top-level keys and any non-list values (skip dumping full trackList)
    summary = {}
    for k, v in entity.items():
        if isinstance(v, list):
            summary[k] = f"<list of {len(v)} items>"
        elif isinstance(v, dict):
            summary[k] = f"<dict with keys: {list(v.keys())}>"
        else:
            summary[k] = v

    return jsonify(summary)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, threaded=True)

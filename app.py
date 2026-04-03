# app.py
from flask import Flask, request, jsonify, Response, send_file
from flask_cors import CORS
from flask_compress import Compress  # <--- NEW IMPORT
import gc  # <--- NEW IMPORT
import sys
import os
import time
from threading import Lock
from pathlib import Path
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timezone, timedelta
import xml.etree.ElementTree as ET
import traceback
import numpy as np
import re
from collections import OrderedDict
import hashlib
from urllib.parse import quote
from requests.exceptions import ChunkedEncodingError, ConnectionError
from email.utils import parsedate_to_datetime
import json
import struct
import gzip
import shutil
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Event

try:
    import xarray as xr
except Exception:
    xr = None

app = Flask(__name__)
CORS(app)
Compress(app)  # <--- Enable GZIP compression for all routes

NEXRAD_BUCKET_BASE = "https://unidata-nexrad-level3.s3.amazonaws.com"
NEXRAD_LEVEL2_BASE = "https://nomads.ncep.noaa.gov/pub/data/nccf/radar/nexrad_level2"
S3_NS = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
TIMESTAMP_PATTERN = re.compile(r"_(\d{4})_(\d{2})_(\d{2})_(\d{2})_(\d{2})_(\d{2})")
LEVEL2_FILENAME_PATTERN = re.compile(r"^[A-Z]{4}_(\d{8})_(\d{6})\.bz2$")

REQUEST_TIMEOUT = (2.5, 20)
RADAR_KEY_CACHE_TTL = 10  # seconds before reusing cached key without hitting S3
RADAR_KEY_CACHE_MAX_AGE = 180  # hard cap before forcing a full refresh
RADAR_KEY_LOOKBACK_DAYS = 2  # when date isn't provided, scan today + recent days
LEVEL2_DIRLIST_CACHE_TTL = 8  # seconds
STREAM_CHUNK_SIZE = 64 * 1024
HRRR_BASE_URL = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod"
RRFS_A_BASE_URL = "https://noaa-rrfs-pds.s3.amazonaws.com"
NAM3K_BASE_URL = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/nam/prod"
MRMS_BUCKET_BASE = "https://noaa-mrms-pds.s3.amazonaws.com"
MRMS_PRODUCT_PATH = "CONUS/SeamlessHSR_00.00"
HRRR_IDX_CACHE_TTL = 60
HRRR_MAX_FORECAST_HOUR = 48
HRRR_PROCESSED_CACHE_MAX_SIZE = 2
HRRR_REFC_MIN_DBZ = 0.0
HRRR_DEFAULT_LOOKBACK_HOURS = 48
HRRR_DEFAULT_RUNS_MAX = 12
HRRR_RUNS_MAX_LIMIT = 48
HRRR_PTYPE_FLAG_THRESHOLD = 0.5
HRRR_PTYPE_ENCODE_OFFSETS = {
    "rain": np.float32(0.0),
    "frzr": np.float32(100.0),
    "icep": np.float32(200.0),
    "snow": np.float32(300.0),
}

hrrr_idx_cache = {}
hrrr_idx_cache_lock = Lock()
hrrr_processed_cache = OrderedDict()
hrrr_processed_cache_lock = Lock()
model_last_successful_runs = {
    "hrrr": None,
    "rrfs-a": None,
    "nam3k": None,
    "mrms": None,
}
model_last_successful_runs_lock = Lock()
mrms_latest_cache = {}
mrms_latest_cache_lock = Lock()
MRMS_LATEST_CACHE_TTL = 20


def _log_hrrr(message):
    print(f"[HRRR] {message}")

def _build_http_session():
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=("GET", "HEAD")
    )
    adapter = HTTPAdapter(pool_connections=8, pool_maxsize=16, max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": "RadarApp/1.0"})
    return session

http_session = _build_http_session()
latest_key_cache = {}
latest_key_lock = Lock()
level2_dirlist_cache = {}
level2_dirlist_cache_lock = Lock()
level2_download_locks = {}
level2_download_locks_lock = Lock()

# Cache for processed WebGL data to avoid reprocessing
processed_data_cache = OrderedDict()
processed_data_lock = Lock()
PROCESSED_CACHE_MAX_SIZE = 64
RADAR_BATCH_MAX_KEYS = 48
RADAR_BATCH_MAX_WORKERS = 6
radar_blob_inflight = {}
radar_blob_inflight_lock = Lock()
TEMP_CLEANUP_INTERVAL_SEC = 60
last_temp_cleanup_ts = 0.0

LEVEL2_SWEEP_REQUIRED_COVERAGE_DEG = 359.0  # require near-complete 360° sweep

# IEM Mesonet API base for NWS text products
IEM_BASE = "https://mesonet.agron.iastate.edu/api/1"

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import nexrad  # your existing radar parser

def _http_get(url, **kwargs):
    timeout = kwargs.pop("timeout", REQUEST_TIMEOUT)
    return http_session.get(url, timeout=timeout, **kwargs)

def _http_get_with_retry(url, max_retries=3, **kwargs):
    """HTTP GET with retry on ChunkedEncodingError and connection errors."""
    for attempt in range(max_retries):
        try:
            return _http_get(url, **kwargs)
        except (ChunkedEncodingError, ConnectionError) as e:
            if attempt < max_retries - 1:
                wait_time = 1.5 ** (attempt + 1)
                print(f"⚠️  Download interrupted (attempt {attempt + 1}/{max_retries}): {type(e).__name__}")
                print(f"⏳ Retrying in {wait_time:.1f}s...")
                time.sleep(wait_time)
            else:
                print(f"❌ Download failed after {max_retries} attempts")
                raise

# --- IEM NWS Text utilities ---

# AFOS text parsing functions removed - using IEM SBW GeoJSON API instead

# Text parsing patterns no longer needed - using IEM SBW GeoJSON API instead

# Text parsing functions removed - using IEM SBW GeoJSON API instead

def _list_radar_keys(prefix, max_keys=1000, start_after=None):
    params = {
        'list-type': '2',
        'prefix': prefix,
        'max-keys': max_keys,
    }
    if start_after:
        params['start-after'] = start_after

    response = _http_get(NEXRAD_BUCKET_BASE, params=params)
    response.raise_for_status()
    root = ET.fromstring(response.content)
    keys = [elem.text for elem in root.findall('s3:Contents/s3:Key', S3_NS) if elem.text]
    return keys


def _list_s3_objects(base_url, prefix, max_keys=1000, start_after=None, continuation_token=None, delimiter=None):
    params = {
        'list-type': '2',
        'prefix': prefix,
        'max-keys': max_keys,
    }
    if start_after:
        params['start-after'] = start_after
    if continuation_token:
        params['continuation-token'] = continuation_token
    if delimiter:
        params['delimiter'] = delimiter

    response = _http_get(base_url, params=params)
    response.raise_for_status()

    root = ET.fromstring(response.content)
    objects = []
    for content in root.findall('s3:Contents', S3_NS):
        key = content.findtext('s3:Key', default='', namespaces=S3_NS)
        if not key:
            continue

        last_modified_raw = content.findtext('s3:LastModified', default='', namespaces=S3_NS)
        size_raw = content.findtext('s3:Size', default='0', namespaces=S3_NS)
        try:
            size = int(size_raw)
        except Exception:
            size = 0

        last_modified = None
        if last_modified_raw:
            try:
                last_modified = datetime.fromisoformat(last_modified_raw.replace('Z', '+00:00')).astimezone(timezone.utc)
            except Exception:
                last_modified = None

        objects.append({
            'key': key,
            'last_modified': last_modified,
            'size': size,
        })

    common_prefixes = [
        p.text
        for p in root.findall('s3:CommonPrefixes/s3:Prefix', S3_NS)
        if p.text
    ]

    is_truncated = root.findtext('s3:IsTruncated', default='false', namespaces=S3_NS).lower() == 'true'
    next_token = root.findtext('s3:NextContinuationToken', default=None, namespaces=S3_NS)

    return {
        'objects': objects,
        'common_prefixes': common_prefixes,
        'is_truncated': is_truncated,
        'next_token': next_token,
    }


def _extract_mrms_timestamp_from_key(key):
    # Example: MRMS_SeamlessHSR_00.00_20260216-032600.grib2.gz
    match = re.search(r"_(\d{8})-(\d{6})\.grib2\.gz$", key or "")
    if not match:
        return None
    date_part, time_part = match.groups()
    try:
        return datetime.strptime(f"{date_part}{time_part}", "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _list_mrms_objects_for_date(date_str, max_pages=4):
    prefix = f"{MRMS_PRODUCT_PATH}/{date_str}/"
    token = None
    pages = 0
    objects = []

    while pages < max_pages:
        payload = _list_s3_objects(
            MRMS_BUCKET_BASE,
            prefix=prefix,
            max_keys=1000,
            continuation_token=token,
        )
        pages += 1

        for obj in payload['objects']:
            key = obj.get('key', '')
            if key.endswith('.grib2.gz') and 'MRMS_SeamlessHSR_00.00_' in key:
                objects.append(obj)

        if not payload['is_truncated']:
            break
        token = payload['next_token']
        if not token:
            break

    return objects


def _get_latest_mrms_object():
    now = time.monotonic()
    with mrms_latest_cache_lock:
        entry = mrms_latest_cache.get('latest')
        if entry and (now - entry['checked_at']) < MRMS_LATEST_CACHE_TTL:
            return entry['object']

    now_utc = datetime.now(timezone.utc)
    candidate_dates = [
        (now_utc - timedelta(days=offset)).strftime('%Y%m%d')
        for offset in range(0, 4)
    ]

    best = None
    for date_str in candidate_dates:
        objects = _list_mrms_objects_for_date(date_str)
        if not objects:
            continue

        for obj in objects:
            key = obj.get('key', '')
            obj_lm = obj.get('last_modified') or _extract_mrms_timestamp_from_key(key)
            if obj_lm is None:
                continue
            if best is None or obj_lm > best['last_modified']:
                best = {
                    'key': key,
                    'last_modified': obj_lm,
                    'size': obj.get('size', 0),
                    'url': f"{MRMS_BUCKET_BASE}/{quote(key)}",
                }

        # If we found something for today, that's usually the fastest/most relevant stop point.
        if best is not None and date_str == now_utc.strftime('%Y%m%d'):
            break

    if best is None:
        raise FileNotFoundError('Unable to find latest MRMS SeamlessHSR file.')

    with mrms_latest_cache_lock:
        mrms_latest_cache['latest'] = {
            'object': best,
            'checked_at': time.monotonic(),
        }

    return best


def _download_mrms_gz(grib_url, out_path):
    out_tmp = Path(str(out_path) + '.part')
    out_tmp.unlink(missing_ok=True)

    response = _http_get(grib_url, stream=True, timeout=(5, 90))
    try:
        response.raise_for_status()
        with open(out_tmp, 'wb') as f:
            for chunk in response.iter_content(STREAM_CHUNK_SIZE):
                if chunk:
                    f.write(chunk)
    finally:
        response.close()

    out_tmp.replace(out_path)


def _ensure_mrms_grib2_from_gz(gz_path, grib2_path):
    if grib2_path.exists() and grib2_path.stat().st_size > 0:
        if gz_path.exists() and grib2_path.stat().st_mtime >= gz_path.stat().st_mtime:
            return

    out_tmp = Path(str(grib2_path) + '.part')
    out_tmp.unlink(missing_ok=True)

    with gzip.open(gz_path, 'rb') as src, open(out_tmp, 'wb') as dst:
        shutil.copyfileobj(src, dst, length=STREAM_CHUNK_SIZE)

    out_tmp.replace(grib2_path)

def _cache_latest_key(cache_key, key):
    with latest_key_lock:
        latest_key_cache[cache_key] = {
            "key": key,
            "checked_at": time.monotonic()
        }


def _normalize_radar_source(source):
    normalized = (source or "level3").strip().lower()
    if normalized in ("l2", "level2"):
        return "level2"
    return "level3"


def _normalize_level2_site_id(site_id):
    normalized = (site_id or "").strip().upper()
    if len(normalized) == 3:
        return f"K{normalized}"
    return normalized


def _parse_level2_timestamp(filename):
    match = LEVEL2_FILENAME_PATTERN.match(filename or "")
    if not match:
        return None
    date_part, time_part = match.groups()
    try:
        return datetime.strptime(f"{date_part}{time_part}", "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _compute_azimuth_coverage_degrees(azimuths):
    """Compute circular azimuth coverage in degrees from an azimuth array."""
    if azimuths is None:
        return 0.0

    arr = np.asarray(azimuths, dtype=np.float32)
    arr = arr[np.isfinite(arr)]
    if arr.size < 2:
        return 0.0

    arr = np.mod(arr, 360.0)
    arr.sort()
    gaps = np.diff(arr)
    wrap_gap = (arr[0] + 360.0) - arr[-1]
    max_gap = max(float(np.max(gaps)) if gaps.size else 0.0, float(wrap_gap))
    coverage = max(0.0, min(360.0, 360.0 - max_gap))
    return coverage


def _is_level2_sweep_complete(file_path, product='N0B'):
    """Return (is_complete, coverage_deg, ray_count) for a Level 2 file."""
    try:
        import nexrad_level2
        radar_data = nexrad_level2.NEXRADLevel2File(file_path)
        try:
            azimuths, _, _ = _extract_level2_radar_data(radar_data, product)
        finally:
            try:
                radar_data.close()
            except Exception:
                pass

        if azimuths is None:
            return False, 0.0, 0

        coverage = _compute_azimuth_coverage_degrees(azimuths)
        ray_count = int(len(azimuths))
        is_complete = coverage >= LEVEL2_SWEEP_REQUIRED_COVERAGE_DEG
        return is_complete, coverage, ray_count
    except Exception:
        return False, 0.0, 0


def _build_level2_tilt_update_token(site_id, product, filename):
    """
    FIXED: Simplified logic that relies on the smart download function.
    """
    normalized_site = _normalize_level2_site_id(site_id)
    normalized_name = os.path.basename(filename or "")

    try:
        # This function requires the latest bytes for accurate tokens; force full
        # download when remote size differs so token reflects exact byte count.
        file_path = download_level2_file_by_name(normalized_site, normalized_name, force_full_download=True)
    except Exception:
        # If download fails, return a temporary token so the frontend tries again soon.
        return f"pending|{time.time()}", False, 0.0, 0, 0

    stats = os.stat(file_path)

    # Arc-Sync: compute ray growth/coverage for update tokens, but do not gate on completion.
    _, coverage, rays = _is_level2_sweep_complete(file_path, product)
    is_complete = False

    # The token is now a hash of the filename + its exact size + ray count.
    # When the file grows, the size changes, the hash changes, and the frontend knows to update.
    token = hashlib.md5(f"{normalized_name}-{stats.st_size}-{rays}".encode()).hexdigest()

    return token, is_complete, float(coverage), int(rays), int(stats.st_size)


def _fetch_level2_dirlist(site_id, log_details=False):
    normalized_site = _normalize_level2_site_id(site_id)
    cache_key = (normalized_site,)
    now = time.monotonic()

    with level2_dirlist_cache_lock:
        entry = level2_dirlist_cache.get(cache_key)
        if entry and (now - entry["checked_at"]) < LEVEL2_DIRLIST_CACHE_TTL:
            if log_details:
                print(f"[L2-Monitor] Using cached directory list for {normalized_site} (age: {now - entry['checked_at']:.1f}s)")
            return entry["files"]

    if log_details:
        print(f"[L2-Monitor] Fetching fresh directory list for {normalized_site}...")
    
    dir_url = f"{NEXRAD_LEVEL2_BASE}/{quote(normalized_site)}/dir.list"
    response = _http_get(dir_url)
    response.raise_for_status()

    files = []
    for line in response.text.splitlines():
        parts = line.strip().split()
        if len(parts) < 2:
            continue
        filename = parts[-1].strip()
        file_ts = _parse_level2_timestamp(filename)
        if not filename.endswith(".bz2") or file_ts is None:
            continue
        files.append({
            "key": filename,
            "timestamp": file_ts,
            "url": f"{NEXRAD_LEVEL2_BASE}/{normalized_site}/{quote(filename)}",
        })

    files.sort(key=lambda item: item["timestamp"])
    
    if log_details:
        print(f"[L2-Monitor] Found {len(files)} Level 2 files for {normalized_site}")
    
    with level2_dirlist_cache_lock:
        level2_dirlist_cache[cache_key] = {
            "files": files,
            "checked_at": time.monotonic(),
        }
    return files


def _get_level2_remote_last_modified(site_id, filename):
    """Return server Last-Modified timestamp (UTC) for a Level 2 file, or None."""
    normalized_site = _normalize_level2_site_id(site_id)
    normalized_name = os.path.basename(filename or "")
    file_url = f"{NEXRAD_LEVEL2_BASE}/{quote(normalized_site)}/{quote(normalized_name)}"

    response = None
    try:
        response = http_session.head(file_url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        if response.status_code >= 400 or not response.headers.get("Last-Modified"):
            if response is not None:
                response.close()
            response = _http_get(file_url, stream=True)

        last_modified = response.headers.get("Last-Modified") if response is not None else None
        if not last_modified:
            return None

        parsed = parsedate_to_datetime(last_modified)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None
    finally:
        if response is not None:
            try:
                response.close()
            except Exception:
                pass


def get_latest_level2_file(site_id):
    files = _fetch_level2_dirlist(site_id)
    if not files:
        raise FileNotFoundError(f"No Level 2 files found for site {site_id}")
    return files[-1]["key"]


def get_latest_radar_key(site_id, product, date=None):
    site_id = site_id.upper()
    product = product.upper()

    if date is not None:
        prefix = f"{site_id}_{product}_{date}"
        cache_key = (prefix,)
    else:
        # Keep a stable "latest" cache key across UTC date rollovers.
        prefix = None
        cache_key = (site_id, product, "latest")

    now = time.monotonic()

    with latest_key_lock:
        entry = latest_key_cache.get(cache_key)
        if entry and (now - entry["checked_at"]) < RADAR_KEY_CACHE_TTL:
            return entry["key"]
        last_known_key = entry["key"] if entry else None
        last_checked = entry["checked_at"] if entry else 0

    latest_key = None

    if last_known_key:
        last_known_ts = _parse_timestamp_from_key(last_known_key)
        if last_known_ts is not None:
            last_date = last_known_ts.strftime("%Y_%m_%d")
            incremental_prefix = f"{site_id}_{product}_{last_date}"
        elif date is not None:
            incremental_prefix = f"{site_id}_{product}_{date}"
        else:
            incremental_prefix = None

        try:
            incremental = _list_radar_keys(incremental_prefix, max_keys=200, start_after=last_known_key) if incremental_prefix else []
        except Exception:
            # If incremental listing fails, force a full refresh below.
            incremental = None

        if incremental:
            latest_key = incremental[-1]
        elif incremental == [] and (now - last_checked) < RADAR_KEY_CACHE_MAX_AGE:
            _cache_latest_key(cache_key, last_known_key)
            return last_known_key

    if latest_key is None:
        if date is not None:
            keys = _list_radar_keys(f"{site_id}_{product}_{date}")
            if not keys:
                raise FileNotFoundError(f"No radar files found for prefix {site_id}_{product}_{date}")
            latest_key = keys[-1]
        else:
            now_utc = datetime.now(timezone.utc)
            candidates = []
            for day_offset in range(0, RADAR_KEY_LOOKBACK_DAYS + 1):
                day = (now_utc - timedelta(days=day_offset)).strftime("%Y_%m_%d")
                day_prefix = f"{site_id}_{product}_{day}"
                keys = _list_radar_keys(day_prefix)
                if keys:
                    candidates.append(keys[-1])

            if not candidates:
                raise FileNotFoundError(f"No radar files found for {site_id} {product} in recent dates")

            latest_key = max(
                candidates,
                key=lambda k: _parse_timestamp_from_key(k) or datetime.min.replace(tzinfo=timezone.utc)
            )

    _cache_latest_key(cache_key, latest_key)
    return latest_key

def download_radar_file(site_id, product, *, use_cache=True):
    key = get_latest_radar_key(site_id, product)
    return download_radar_file_by_key(key, use_cache=use_cache)


def download_level2_file(site_id, *, use_cache=True, skip_corrupted=False):
    filename = get_latest_level2_file(site_id)
    return download_level2_file_by_name(site_id, filename, use_cache=use_cache, skip_corrupted=skip_corrupted)


def download_level2_file_by_name(site_id, filename, *, use_cache=True, skip_corrupted=False, force_full_download=False):
    """
    Download a Level2 file by name.

    By default this function will reuse a local cached copy even if the remote
    file has grown. Set `force_full_download=True` to force a full redownload
    when the remote size differs (used for archive/arc-sync operations).
    """
    normalized_site = _normalize_level2_site_id(site_id)
    normalized_name = os.path.basename(filename or "")
    if not LEVEL2_FILENAME_PATTERN.match(normalized_name):
        raise ValueError("Invalid Level 2 filename")

    file_url = f"{NEXRAD_LEVEL2_BASE}/{quote(normalized_site)}/{quote(normalized_name)}"
    temp_dir = Path("./temp") / "level2" / normalized_site
    temp_dir.mkdir(parents=True, exist_ok=True)
    temp_path = temp_dir / normalized_name

    # Check if we should use the cached file or redownload
    if use_cache and temp_path.exists():
        local_size = temp_path.stat().st_size
        if local_size > 0:
            try:
                # HEAD request is very fast and just gets metadata like file size
                head_resp = http_session.head(file_url, timeout=2.0, allow_redirects=True)
                remote_size = int(head_resp.headers.get('Content-Length', 0))
                head_resp.close()

                # If local file size matches remote, we have the complete, up-to-date file.
                if local_size == remote_size:
                    print(f"♻️  Cache hit (size match): {normalized_name}")
                    return str(temp_path)
                else:
                    # If caller requested a forced full download (archive/arc-sync),
                    # fall through to redownload. Otherwise return cached file to
                    # avoid re-downloading a multi-MB file just because it grew.
                    if force_full_download:
                        print(f"🔄 File grew (forced redownload): Local: {local_size} -> Remote: {remote_size}. Redownloading...")
                    else:
                        print(f"🔁 File grew but skipping full redownload (not arc-sync): Local: {local_size} -> Remote: {remote_size}. Using cached file.")
                        return str(temp_path)
            except Exception as e:
                # If HEAD request fails, we'll cautiously use the cache to avoid errors
                print(f"⚠️  HEAD request failed ({e}), using cached file.")
                return str(temp_path)

    # --- If we reach here, we need to download ---
    print(f"📥 Downloading: {normalized_name}")

    lock_key = (normalized_site, normalized_name)
    with level2_download_locks_lock:
        file_lock = level2_download_locks.get(lock_key)
        if file_lock is None:
            file_lock = Lock()
            level2_download_locks[lock_key] = file_lock

    with file_lock:
        temp_path_tmp = temp_path.parent / f"{temp_path.name}.part.{os.getpid()}"

        # Retry the entire download+streaming operation to handle incomplete reads
        # (file may still be writing on remote server)
        max_download_attempts = 5
        for attempt in range(1, max_download_attempts + 1):
            response = None
            try:
                response = _http_get_with_retry(file_url, stream=True, max_retries=2)
                response.raise_for_status()
                with open(temp_path_tmp, 'wb') as f:
                    for chunk in response.iter_content(STREAM_CHUNK_SIZE):
                        f.write(chunk)
                temp_path_tmp.replace(temp_path)
                return str(temp_path)  # Success!
            except (requests.exceptions.ChunkedEncodingError, Exception) as e:
                if response is not None:
                    response.close()
                    response = None
                if temp_path_tmp.exists():
                    try:
                        temp_path_tmp.unlink(missing_ok=True)
                    except PermissionError:
                        time.sleep(0.1)
                        try:
                            temp_path_tmp.unlink(missing_ok=True)
                        except Exception:
                            pass

                is_incomplete_read = 'IncompleteRead' in str(e) or isinstance(e, requests.exceptions.ChunkedEncodingError)

                if is_incomplete_read and attempt < max_download_attempts:
                    wait_time = 0.5 * (attempt + 0.5)  # 0.75s, 1.25s, 1.75s, 2.25s
                    print(f"⚠️  Incomplete download (attempt {attempt}/{max_download_attempts}), retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue

                if is_incomplete_read and use_cache and temp_path.exists() and temp_path.stat().st_size > 0:
                    print(f"⚠️  Using last cached file after incomplete download: {normalized_name}")
                    return str(temp_path)

                print(f"❌ Download failed for {normalized_name}: {e}")
                raise
            finally:
                if response is not None:
                    response.close()

        raise Exception(f"Failed to download {normalized_name} after {max_download_attempts} attempts")

def download_radar_file_by_key(key, *, use_cache=True):
    """Download a specific radar file by its full S3 key with aggressive caching."""
    file_url = f"{NEXRAD_BUCKET_BASE}/{key}"
    temp_dir = Path("./temp")
    temp_dir.mkdir(exist_ok=True)
    temp_path = temp_dir / key
    temp_path.parent.mkdir(parents=True, exist_ok=True)

    if use_cache and temp_path.exists() and temp_path.stat().st_size > 0:
        # Level-3 keys include timestamps and are effectively immutable.
        # Avoiding a HEAD check removes one network round trip per frame.
        return str(temp_path)

    print(f"✅ Fetching radar file: {file_url}")

    temp_path_tmp = temp_path.parent / f"{temp_path.name}.part"
    if temp_path_tmp.exists():
        temp_path_tmp.unlink(missing_ok=True)

    response = _http_get(file_url, stream=True)
    try:
        response.raise_for_status()
        with open(temp_path_tmp, 'wb') as f:
            for chunk in response.iter_content(STREAM_CHUNK_SIZE):
                if chunk:
                    f.write(chunk)
    except Exception:
        if temp_path_tmp.exists():
            temp_path_tmp.unlink(missing_ok=True)
        raise
    finally:
        response.close()

    temp_path_tmp.replace(temp_path)
    return str(temp_path)


def _parse_timestamp_from_key(key):
    """Extract a UTC datetime from an S3 key if possible."""
    match = TIMESTAMP_PATTERN.search(key)
    if not match:
        return None

    year, month, day, hour, minute, second = map(int, match.groups())
    return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)


def fetch_archive_scans(site_id, product, date_str):
    """List all archive scans for a site/product/date combination."""
    try:
        target_date = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as err:
        raise ValueError("Invalid date format. Use YYYY-MM-DD.") from err

    prefix = f"{site_id}_{product}_{target_date.strftime('%Y_%m_%d')}"
    scans = []
    continuation_token = None

    while True:
        params = {
            'list-type': '2',
            'prefix': prefix,
            'max-keys': 1000,
        }
        if continuation_token:
            params['continuation-token'] = continuation_token

        response = _http_get(NEXRAD_BUCKET_BASE, params=params)
        response.raise_for_status()
        root = ET.fromstring(response.content)

        contents = root.findall('s3:Contents', S3_NS)
        if not contents and not continuation_token:
            break

        for content in contents:
            key_element = content.find('s3:Key', S3_NS)
            if key_element is None or not key_element.text:
                continue

            key_text = key_element.text
            timestamp = _parse_timestamp_from_key(key_text)
            if not timestamp:
                continue

            size_elem = content.find('s3:Size', S3_NS)
            last_modified_elem = content.find('s3:LastModified', S3_NS)

            scans.append({
                "key": key_text,
                "timestamp": timestamp,
                "timeString": timestamp.strftime("%H:%M:%S UTC"),
                "sizeBytes": int(size_elem.text) if size_elem is not None and size_elem.text else None,
                "lastModified": last_modified_elem.text if last_modified_elem is not None else None,
                "fileName": key_text.split('/')[-1],
            })

        is_truncated_text = root.findtext('s3:IsTruncated', default='false', namespaces=S3_NS) or 'false'
        is_truncated = is_truncated_text.strip().lower() == 'true'
        if is_truncated:
            token_elem = root.find('s3:NextContinuationToken', S3_NS)
            continuation_token = token_elem.text if token_elem is not None else None
            if not continuation_token:
                break
        else:
            break

    scans.sort(key=lambda s: s["timestamp"])

    # Convert timestamp objects to ISO8601 strings once sorted
    for scan in scans:
        iso_ts = scan["timestamp"].isoformat().replace('+00:00', 'Z')
        scan["timestamp"] = iso_ts

    return scans


@app.route('/api/archive/timestamps/<site_id>', methods=['GET'])
def get_archive_timestamps(site_id):
    product = request.args.get('product', 'N0B')
    date_str = request.args.get('date')

    if not date_str:
        return jsonify({"error": "Missing required 'date' query parameter (YYYY-MM-DD)."}), 400

    try:
        scans = fetch_archive_scans(site_id, product, date_str)
        return jsonify({
            "siteId": site_id,
            "product": product,
            "date": date_str,
            "count": len(scans),
            "scans": scans
        })
    except ValueError as err:
        return jsonify({"error": str(err)}), 400
    except Exception:
        print(traceback.format_exc())
        return jsonify({"error": "Failed to fetch archive scans."}), 500

# --- IEM SBW GeoJSON warnings endpoint ---
@app.route('/api/archive/warnings', methods=['GET'])
def get_archive_warnings():
    """Fetch NWS warnings from IEM SBW GeoJSON endpoint (much faster!).
    Query params: 
      - date=YYYY-MM-DD (required)
      - time=HH:MM:SS (required - must match radar scan timestamp)
      - phenomena=TO,SV,FF,etc (optional, comma-separated filter for warning types)
    """
    date_str = request.args.get('date')
    time_str = request.args.get('time')
    phenomena_filter = request.args.get('phenomena')  # e.g., "TO,SV" for tornado and severe thunderstorm
    
    if not date_str:
        return jsonify({"error": "Missing required 'date' query parameter (YYYY-MM-DD)."}), 400
    
    if not time_str:
        return jsonify({"error": "Missing required 'time' query parameter (HH:MM:SS). Must match radar scan timestamp."}), 400
    
    # Build timestamp for IEM API
    timestamp = f"{date_str}T{time_str}Z"
    
    print(f"\n{'='*60}")
    print(f"🌩️ [WARNINGS] Fetching warnings from IEM SBW GeoJSON")
    print(f"   Timestamp: {timestamp}")
    print(f"   Phenomena filter: {phenomena_filter or 'ALL'}")
    print(f"{'='*60}")
    
    try:
        # Fetch GeoJSON from IEM (single fast request!)
        url = "https://mesonet.agron.iastate.edu/geojson/sbw.geojson"
        params = {"ts": timestamp}
        
        response = _http_get(url, params=params)
        response.raise_for_status()
        geojson_data = response.json()
        
        features = geojson_data.get('features', [])
        print(f"📋 Retrieved {len(features)} warning features from IEM")
        
        # Filter by phenomena if specified
        if phenomena_filter:
            phenomena_set = set(p.strip().upper() for p in phenomena_filter.split(','))
            filtered_features = [
                f for f in features 
                if f.get('properties', {}).get('phenomena') in phenomena_set
            ]
            print(f"🔍 Filtered to {len(filtered_features)} features matching phenomena: {phenomena_filter}")
            features = filtered_features
        
        # Transform to frontend-friendly format
        alerts = []
        for feature in features:
            props = feature.get('properties', {})
            geom = feature.get('geometry', {})
            
            # Extract polygon coordinates (IEM uses MultiPolygon format)
            polygon = None
            if geom.get('type') == 'MultiPolygon':
                coords = geom.get('coordinates', [])
                if coords and len(coords) > 0:
                    # Take first polygon from MultiPolygon
                    polygon = coords[0][0] if coords[0] else None
            elif geom.get('type') == 'Polygon':
                coords = geom.get('coordinates', [])
                polygon = coords[0] if coords else None
            
            # Only include warnings with valid polygons
            if not polygon:
                continue
            
            alert = {
                "id": feature.get('id') or props.get('product_id'),
                "phenomena": props.get('phenomena'),
                "significance": props.get('significance'),
                "event": props.get('ps'),  # Product string like "Tornado Warning"
                "wfo": props.get('wfo'),
                "eventid": props.get('eventid'),
                "onset": props.get('polygon_begin'),
                "expires": props.get('expire_utc') or props.get('expire'),
                "polygon": polygon,
                "product_id": props.get('product_id'),
                "issue": props.get('issue'),
                # Enhanced threat tags
                "windtag": props.get('max_windtag') or props.get('windtag'),
                "hailtag": props.get('max_hailtag') or props.get('hailtag'),
                "tornadotag": props.get('tornadotag'),
                "is_emergency": props.get('max_is_emergency', False) or props.get('is_emergency', False),
                "is_pds": props.get('max_is_pds', False) or props.get('is_pds', False),
                "floodtag_damage": props.get('max_floodtag_damage') or props.get('floodtag_damage'),
                "damagetag": props.get('damagetag'),
                "windthreat": props.get('windthreat'),
                "hailthreat": props.get('hailthreat'),
                "href": props.get('href'),
            }
            alerts.append(alert)
        
        print(f"\n{'='*60}")
        print(f"📊 [WARNINGS] Summary:")
        print(f"   ✅ Valid warnings: {len(alerts)}")
        print(f"{'='*60}\n")
        
        return jsonify({
            "date": date_str,
            "time": time_str,
            "timestamp": timestamp,
            "count": len(alerts),
            "alerts": alerts,
        })
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({"error": f"Failed to fetch warnings from IEM: {str(e)}"}), 500


@app.route('/api/archive/file', methods=['GET'])
def get_archive_file():
    key = request.args.get('key')
    if not key:
        return jsonify({"error": "Missing required 'key' query parameter."}), 400

    try:
        file_path = download_radar_file_by_key(key)
        filename = Path(file_path).name
        return send_file(file_path, as_attachment=True, download_name=filename)
    except FileNotFoundError:
        return jsonify({"error": "Radar file not found."}), 404
    except Exception as err:
        print(traceback.format_exc())
        return jsonify({"error": str(err)}), 500


def extract_radar_data(radar_data):
    if hasattr(radar_data, 'level2_arrays'):
        try:
            azimuths, ranges, radar_values = radar_data.level2_arrays
            return azimuths, ranges, radar_values
        except Exception:
            return None, None, None

    azimuths = None
    ranges = None
    radar_values = None

    if hasattr(radar_data, 'sym_block') and radar_data.sym_block:
        for layer in radar_data.sym_block:
            if not isinstance(layer, (list, tuple)):
                continue
            for packet in layer:
                if isinstance(packet, dict) and 'start_az' in packet and 'data' in packet:
                    azimuths = packet['start_az']

                    if hasattr(radar_data, 'ij_to_km'):
                        gate_km = radar_data.ij_to_km
                    else:
                        gate_km = packet.get('gate_scale', 1000) / 1000.0

                    first_bin = packet.get('first', 0)
                    first_km = first_bin * gate_km

                    num_bins = len(packet['data'][0]) if packet['data'] else 0
                    ranges = np.linspace(first_km, first_km + (num_bins - 1) * gate_km, num_bins).tolist()

                    try:
                        radar_values = [
                            radar_data.map_data(np.frombuffer(radial_bytes, dtype=np.uint8))
                            for radial_bytes in packet['data']
                        ]
                        break
                    except Exception as e:
                        print(f"❌ Error mapping radar values: {e}")
                        continue
            if azimuths is not None:
                break
    return azimuths, ranges, radar_values


def _extract_level2_radar_data(level2_data, product='N0B'):
    """Extract Level 2 radar data, ensuring full 360° sweep coverage."""
    return _extract_level2_radar_data_with_sweep(level2_data, product=product, sweep_index=None)


def _extract_level2_radar_data_with_sweep(level2_data, product='N0B', sweep_index=None):
    """Extract Level 2 radar data for a specific sweep index (or default tilt logic)."""
    product_code = (product or 'N0B').upper()

    product_moment_map = {
        'B': 'REF',
        'G': 'VEL',
        'V': 'VEL',
        'S': 'VEL',
        'W': 'SW',
        'C': 'RHO',
        'X': 'ZDR',
        'P': 'PHI',
        'H': 'RHO',
    }
    suffix = product_code[-1] if product_code else 'B'
    desired_moment = product_moment_map.get(suffix, 'REF')

    if sweep_index is None:
        tilt_idx = 0
        if len(product_code) >= 2 and product_code[1].isdigit():
            tilt_idx = max(0, min(3, int(product_code[1])))
        scan_idx = min(tilt_idx, max(0, level2_data.nscans - 1))
    else:
        scan_idx = max(0, min(int(sweep_index), max(0, level2_data.nscans - 1)))
    scan_info = level2_data.scan_info([scan_idx])
    if not scan_info:
        return None, None, None

    available_moments = scan_info[0].get('moments', [])
    moment = desired_moment if desired_moment in available_moments else ('REF' if 'REF' in available_moments else None)
    if not moment and available_moments:
        moment = available_moments[0]
    if not moment:
        return None, None, None

    ranges_m = level2_data.get_range(scan_idx, moment)
    if ranges_m is None or len(ranges_m) == 0:
        return None, None, None

    # Get azimuths and ensure full 360° sweep
    azimuths = np.asarray(level2_data.get_azimuth_angles([scan_idx]), dtype=np.float32)
    
    # Verify we have substantial azimuth coverage (should be close to 360°)
    if len(azimuths) > 0:
        az_min = np.nanmin(azimuths)
        az_max = np.nanmax(azimuths)
        az_range = az_max - az_min
        # Log if we're missing significant portions of the sweep
        if az_range < 300:
            print(f"[L2-Extract] ⚠️  WARNING: Partial sweep detected for {product_code} - azimuth range: {az_range:.1f}° (expected ~360°)")
        else:
            print(f"[L2-Extract] ✓ Full sweep coverage: {az_range:.1f}° ({len(azimuths)} rays)")
    
    ngates = len(ranges_m)
    data = level2_data.get_data(moment, ngates, scans=[scan_idx], raw_data=False)
    values = np.ma.asarray(data, dtype=np.float32).filled(np.nan)
    ranges_km = (np.asarray(ranges_m, dtype=np.float32) / 1000.0)

    return azimuths, ranges_km, values


class Level2StreamMonitor:
    """Monitor a Level 2 file and yield new lowest-elevation scans as they arrive."""

    def __init__(self, file_url, site_id, filename):
        self.file_url = file_url
        self.site_id = site_id
        self.filename = filename
        self.downloaded_bytes = 0
        self.sweep_ray_tracker = {}  # { sweep_index: last_seen_ray_count } - tracks growth per sweep
        self._radar = None
        self._radar_size = 0
        self._radar_needs_reload = False
        self.lock = Lock()
        self.remote_size = 0
        temp_dir = Path("./temp/level2_stream") / _normalize_level2_site_id(site_id)
        temp_dir.mkdir(parents=True, exist_ok=True)
        self.local_path = temp_dir / f"{os.path.basename(filename)}.ar2v"
        
        if self.local_path.exists():
            self.local_path.unlink()

    def fetch_new_bytes(self):
        """Fetch any new bytes that have been appended to the file."""
        response = None
        with self.lock:
            try:
                head_resp = http_session.head(self.file_url, timeout=3.0, allow_redirects=True)
                current_size = int(head_resp.headers.get("Content-Length", 0))
                head_resp.close()
                
                self.remote_size = current_size

                if current_size <= self.downloaded_bytes:
                    return 0

                headers = {
                    "Range": f"bytes={self.downloaded_bytes}-"
                }
                response = http_session.get(
                    self.file_url, 
                    headers=headers, 
                    timeout=10.0,
                    stream=False
                )
                
                if response.status_code not in (200, 206):
                    print(f"⚠️  Unexpected status: {response.status_code}")
                    return 0

                new_data = response.content
                if not new_data:
                    return 0

                with open(self.local_path, "ab") as f:
                    f.write(new_data)

                bytes_added = len(new_data)
                self.downloaded_bytes += bytes_added
                # Mark radar for reload so subsequent parsing reopens file
                self._radar_needs_reload = True
                
                print(f"📥 Fetched {bytes_added:,} new bytes (total: {self.downloaded_bytes:,} / remote: {self.remote_size:,})")
                return bytes_added
                
            except Exception as e:
                print(f"⚠️  Error fetching new bytes: {e}")
                return 0
            finally:
                if response is not None:
                    try:
                        response.close()
                    except Exception:
                        pass

    def get_new_lowest_elevation_scans(self, product='N0B', target_elevation=0.5):
        """
        Parse file and return scans at the target elevation that have GROWN since last check.
        This enables minimum-latency partial scans: as soon as 100 rays arrive, we send them.
        When 300 rays arrive, we send the updated scan. Frontend overwrites old with new.
        
        Args:
            product: Product type (e.g., 'N0B')
            target_elevation: Elevation angle to track (default 0.5° for lowest)
        """
        with self.lock:
            try:
                if not self.local_path.exists():
                    return None
                
                file_size = self.local_path.stat().st_size
                if file_size < 10240:
                    print(f"  ⏭️  File too small to parse: {file_size} bytes")
                    return None
                
                import nexrad_level2

                # Reuse cached radar object when possible to avoid repeated open/parsing
                radar = None
                try:
                    file_size = self.local_path.stat().st_size
                except Exception:
                    file_size = 0

                try:
                    if self._radar and not self._radar_needs_reload and file_size == self._radar_size:
                        radar = self._radar
                    else:
                        # Create new radar instance and cache it
                        if self._radar:
                            try:
                                self._radar.close()
                            except Exception:
                                pass
                        radar = nexrad_level2.NEXRADLevel2File(str(self.local_path))
                        self._radar = radar
                        self._radar_size = file_size
                        self._radar_needs_reload = False
                except (EOFError, OSError, ValueError, struct.error) as e:
                    print(f"  ⏭️  File not ready for parsing: {type(e).__name__}")
                    return None

                if radar.nscans == 0:
                    # If we created a radar instance during this call, don't close cached one
                    if radar is not self._radar:
                        try:
                            radar.close()
                        except Exception:
                            pass

                    # Initialize tracker for a new file if empty
                    if not self.sweep_ray_tracker:
                        print(f"🆕 Initializing ray tracker for {self.filename}")

                    return None

                # FIX: Check ALL sweeps in the file for growth, not just new indices
                new_scans = []
                for sweep_index in range(radar.nscans):
                    try:
                        # 1. Check elevation first
                        elevation = None
                        if sweep_index < len(radar.scan_msgs) and len(radar.scan_msgs[sweep_index]) > 0:
                            first_msg_idx = radar.scan_msgs[sweep_index][0]
                            first_msg = radar.radial_records[first_msg_idx]
                            if "msg_header" in first_msg:
                                elevation = first_msg["msg_header"].get("elevation_angle", None)
                        
                        # Skip if not our target elevation (allow 0.1° tolerance)
                        if elevation is None or abs(elevation - target_elevation) > 0.1:
                            continue

                        # 2. FIX: Check if this specific sweep has NEW RAYS since last check
                        current_ray_count = len(radar.scan_msgs[sweep_index]) if sweep_index < len(radar.scan_msgs) else 0
                        last_ray_count = self.sweep_ray_tracker.get(sweep_index, 0)

                        if current_ray_count > last_ray_count:
                            # This sweep has grown! Extract the whole thing (including the new parts)
                            azimuths, ranges, values = _extract_level2_radar_data_with_sweep(
                                radar,
                                product,
                                sweep_index=sweep_index
                            )
                            
                            if azimuths is not None and ranges is not None and values is not None:
                                # Update the tracker so we only process if it grows further
                                self.sweep_ray_tracker[sweep_index] = current_ray_count
                                
                                # Get timestamp
                                timestamp = None
                                if sweep_index < len(radar.scan_msgs) and len(radar.scan_msgs[sweep_index]) > 0:
                                    first_msg_idx = radar.scan_msgs[sweep_index][0]
                                    first_msg = radar.radial_records[first_msg_idx]
                                    if "msg_header" in first_msg:
                                        msg_hdr = first_msg["msg_header"]
                                        if "collect_date" in msg_hdr and "collect_time_ms" in msg_hdr:
                                            timestamp = (msg_hdr["collect_date"], msg_hdr["collect_time_ms"])
                                
                                new_scans.append({
                                    "elevation": elevation,
                                    "sweep_index": sweep_index,
                                    "azimuths": azimuths,
                                    "ranges": ranges,
                                    "values": values,
                                    "ray_count": current_ray_count,
                                    "timestamp": timestamp,
                                })
                                
                                print(f"    ✨ Sweep {sweep_index}: {elevation:.1f}° ({current_ray_count} rays, was {last_ray_count})")
                    
                    except Exception as e:
                        print(f"    ⚠️  Failed to extract sweep {sweep_index}: {e}")
                        continue

                radar.close()
                
                if not new_scans:
                    return None
                
                print(f"  📦 Returning {len(new_scans)} scan(s) at {target_elevation:.1f}° with growth, file: {file_size:,} bytes")
                return {"scans": new_scans}
                
            except (EOFError, OSError, ValueError, struct.error) as e:
                return None
            except Exception as e:
                print(f"⚠️  Error parsing new scans: {e}")
                import traceback
                print(traceback.format_exc())
                return None

    def cleanup(self):
        """Remove temporary file."""
        try:
            if self.local_path.exists():
                self.local_path.unlink(missing_ok=True)
        except Exception:
            pass
active_monitors = {}
active_monitors_lock = Lock()

def format_nexrad_timestamp(timestamp_tuple):
    """Convert NEXRAD timestamp tuple (day_of_year, milliseconds) to ISO format string."""
    if not timestamp_tuple or len(timestamp_tuple) != 2:
        return None
    try:
        day_of_year, time_ms = timestamp_tuple
        # NEXRAD day_of_year is 1-based (1-366)
        # We don't have year info, so assume current year
        now = datetime.now(timezone.utc)
        year = now.year
        
        # Convert day of year to datetime
        dt = datetime(year, 1, 1, tzinfo=timezone.utc) + timedelta(days=int(day_of_year) - 1, milliseconds=int(time_ms))
        return dt.isoformat()
    except Exception:
        return None

def get_site_coordinates(site_id):
    # Remove leading K if present
    if site_id.startswith('K') and len(site_id) > 1:
        site_id = site_id[1:]
    
    sites = {
        # NEXRAD - Continental US (K-prefix stripped)
        "ABR": {"lat": 45.455833, "lon": -98.413333},
        "ABX": {"lat": 35.149722, "lon": -106.823889},
        "AKQ": {"lat": 36.984050, "lon": -77.007361},
        "AMA": {"lat": 35.233333, "lon": -101.709278},
        "AMX": {"lat": 25.611083, "lon": -80.412667},
        "APX": {"lat": 44.906350, "lon": -84.719533},
        "ARX": {"lat": 43.822778, "lon": -91.191111},
        "ATX": {"lat": 48.194611, "lon": -122.495694},
        "BBX": {"lat": 39.495639, "lon": -121.631611},
        "BGM": {"lat": 42.199694, "lon": -75.984722},
        "BHX": {"lat": 40.498583, "lon": -124.292167},
        "BIS": {"lat": 46.770833, "lon": -100.760556},
        "BLX": {"lat": 45.853778, "lon": -108.606806},
        "BMX": {"lat": 33.172417, "lon": -86.770167},
        "BOX": {"lat": 41.955778, "lon": -71.136861},
        "BRO": {"lat": 25.916000, "lon": -97.418967},
        "BUF": {"lat": 42.948789, "lon": -78.736781},
        "BYX": {"lat": 24.597500, "lon": -81.703167},
        "CAE": {"lat": 33.948722, "lon": -81.118278},
        "CBW": {"lat": 46.039250, "lon": -67.806431},
        "CBX": {"lat": 43.490217, "lon": -116.236028},
        "CCX": {"lat": 40.923167, "lon": -78.003722},
        "CLE": {"lat": 41.413217, "lon": -81.859867},
        "CLX": {"lat": 32.655528, "lon": -81.042194},
        "CRI": {"lat": 35.238333, "lon": -97.460000},
        "CRP": {"lat": 27.784017, "lon": -97.511250},
        "CXX": {"lat": 44.511000, "lon": -73.166431},
        "CYS": {"lat": 41.151919, "lon": -104.806028},
        "DAX": {"lat": 38.501111, "lon": -121.677833},
        "DDC": {"lat": 37.760833, "lon": -99.968889},
        "DFX": {"lat": 29.273139, "lon": -100.280333},
        "DGX": {"lat": 32.279944, "lon": -89.984444},
        "DIX": {"lat": 39.947089, "lon": -74.410731},
        "DLH": {"lat": 46.836944, "lon": -92.209722},
        "DMX": {"lat": 41.731200, "lon": -93.722869},
        "DOX": {"lat": 38.825767, "lon": -75.440117},
        "DTX": {"lat": 42.700000, "lon": -83.471667},
        "DVN": {"lat": 41.611667, "lon": -90.580833},
        "DYX": {"lat": 32.538500, "lon": -99.254333},
        "EAX": {"lat": 38.810250, "lon": -94.264472},
        "EMX": {"lat": 31.893650, "lon": -110.630250},
        "ENX": {"lat": 42.586556, "lon": -74.064083},
        "EOX": {"lat": 31.460556, "lon": -85.459389},
        "EPZ": {"lat": 31.873056, "lon": -106.698000},
        "ESX": {"lat": 35.701350, "lon": -114.891647},
        "EVX": {"lat": 30.565033, "lon": -85.921667},
        "EWX": {"lat": 29.704056, "lon": -98.028611},
        "EYX": {"lat": 35.097850, "lon": -117.560750},
        "FCX": {"lat": 37.024400, "lon": -80.273969},
        "FDR": {"lat": 34.362194, "lon": -98.976667},
        "FDX": {"lat": 34.634167, "lon": -103.618889},
        "FFC": {"lat": 33.363550, "lon": -84.565944},
        "FSD": {"lat": 43.587778, "lon": -96.729444},
        "FSX": {"lat": 34.574333, "lon": -111.198444},
        "FTG": {"lat": 39.786639, "lon": -104.545806},
        "FWS": {"lat": 32.573000, "lon": -97.303150},
        "GGW": {"lat": 48.206361, "lon": -106.624694},
        "GJX": {"lat": 39.062169, "lon": -108.213764},
        "GLD": {"lat": 39.366944, "lon": -101.700278},
        "GRB": {"lat": 44.498633, "lon": -88.111111},
        "GRK": {"lat": 30.721833, "lon": -97.382944},
        "GRR": {"lat": 42.893889, "lon": -85.544889},
        "GSP": {"lat": 34.883306, "lon": -82.219833},
        "GWX": {"lat": 33.896917, "lon": -88.329194},
        "GYX": {"lat": 43.891306, "lon": -70.256361},
        "HDC": {"lat": 30.519300, "lon": -90.407400},
        "HDX": {"lat": 33.077000, "lon": -106.120028},
        "HGX": {"lat": 29.471900, "lon": -95.078733},
        "HNX": {"lat": 36.314181, "lon": -119.632128},
        "HPX": {"lat": 36.736972, "lon": -87.285583},
        "HTX": {"lat": 34.930556, "lon": -86.083611},
        "ICT": {"lat": 37.654444, "lon": -97.443056},
        "ICX": {"lat": 37.591050, "lon": -112.862181},
        "ILN": {"lat": 39.420483, "lon": -83.821450},
        "ILX": {"lat": 40.150500, "lon": -89.336792},
        "IND": {"lat": 39.707500, "lon": -86.280278},
        "INX": {"lat": 36.175131, "lon": -95.564161},
        "IWA": {"lat": 33.289233, "lon": -111.669908},
        "IWX": {"lat": 41.358611, "lon": -85.700000},
        "JAX": {"lat": 30.484633, "lon": -81.701900},
        "JGX": {"lat": 32.675683, "lon": -83.350833},
        "JKL": {"lat": 37.590833, "lon": -83.313056},
        "LBB": {"lat": 33.654139, "lon": -101.814167},
        "LCH": {"lat": 30.125306, "lon": -93.215889},
        "LGX": {"lat": 47.116944, "lon": -124.106667},
        "LIX": {"lat": 30.336667, "lon": -89.825417},
        "LNX": {"lat": 41.957944, "lon": -100.576222},
        "LOT": {"lat": 41.604444, "lon": -88.084444},
        "LRX": {"lat": 40.739550, "lon": -116.802700},
        "LSX": {"lat": 38.698611, "lon": -90.682778},
        "LTX": {"lat": 33.989150, "lon": -78.429108},
        "LVX": {"lat": 37.975278, "lon": -85.943889},
        "LWX": {"lat": 38.976111, "lon": -77.487500},
        "LZK": {"lat": 34.836500, "lon": -92.262194},
        "MAF": {"lat": 31.943461, "lon": -102.189250},
        "MAX": {"lat": 42.081169, "lon": -122.717361},
        "MBX": {"lat": 48.393056, "lon": -100.864444},
        "MHX": {"lat": 34.775908, "lon": -76.876189},
        "MKX": {"lat": 42.967800, "lon": -88.550667},
        "MLB": {"lat": 28.113194, "lon": -80.654083},
        "MOB": {"lat": 30.679444, "lon": -88.240000},
        "MPX": {"lat": 44.848889, "lon": -93.565528},
        "MQT": {"lat": 46.531111, "lon": -87.548333},
        "MRX": {"lat": 36.168611, "lon": -83.401944},
        "MSX": {"lat": 47.041000, "lon": -113.986222},
        "MTX": {"lat": 41.262778, "lon": -112.447778},
        "MUX": {"lat": 37.155222, "lon": -121.898444},
        "MVX": {"lat": 47.527778, "lon": -97.325556},
        "MXX": {"lat": 32.536650, "lon": -85.789750},
        "NKX": {"lat": 32.919017, "lon": -117.041800},
        "NQA": {"lat": 35.344722, "lon": -89.873333},
        "OAX": {"lat": 41.320369, "lon": -96.366819},
        "OHX": {"lat": 36.247222, "lon": -86.562500},
        "OKX": {"lat": 40.865528, "lon": -72.863917},
        "OTX": {"lat": 47.680417, "lon": -117.626775},
        "OUN": {"lat": 35.236058, "lon": -97.462350},
        "PAH": {"lat": 37.068333, "lon": -88.771944},
        "PBZ": {"lat": 40.531717, "lon": -80.217967},
        "PDT": {"lat": 45.690650, "lon": -118.852931},
        "POE": {"lat": 31.155278, "lon": -92.976111},
        "PUX": {"lat": 38.459550, "lon": -104.181350},
        "RAX": {"lat": 35.665519, "lon": -78.489750},
        "RGX": {"lat": 39.754056, "lon": -119.462022},
        "RIW": {"lat": 43.066089, "lon": -108.477300},
        "RLX": {"lat": 38.311111, "lon": -81.722778},
        "RTX": {"lat": 45.715039, "lon": -122.965000},
        "SFX": {"lat": 43.105600, "lon": -112.686131},
        "SGF": {"lat": 37.235239, "lon": -93.400419},
        "SHV": {"lat": 32.450833, "lon": -93.841250},
        "SJT": {"lat": 31.371278, "lon": -100.492500},
        "SOX": {"lat": 33.817733, "lon": -117.636000},
        "SRX": {"lat": 35.290417, "lon": -94.361889},
        "TBW": {"lat": 27.705500, "lon": -82.401778},
        "TFX": {"lat": 47.459583, "lon": -111.385333},
        "TLH": {"lat": 30.397583, "lon": -84.328944},
        "TLX": {"lat": 35.333361, "lon": -97.277761},
        "TWX": {"lat": 38.996950, "lon": -96.232550},
        "TYX": {"lat": 43.755694, "lon": -75.679861},
        "UDX": {"lat": 44.124722, "lon": -102.830000},
        "UEX": {"lat": 40.320833, "lon": -98.441944},
        "VAX": {"lat": 30.890278, "lon": -83.001806},
        "VBX": {"lat": 34.838550, "lon": -120.397917},
        "VNX": {"lat": 36.740617, "lon": -98.127717},
        "VTX": {"lat": 34.412017, "lon": -119.178750},
        "VWX": {"lat": 38.260250, "lon": -87.724528},
        "YUX": {"lat": 32.495281, "lon": -114.656708},
        # NEXRAD - Alaska
        "PABC": {"lat": 60.791944, "lon": -161.876389},
        "PACG": {"lat": 56.852778, "lon": -135.529167},
        "PAEC": {"lat": 64.511389, "lon": -165.295000},
        "PAHG": {"lat": 60.725914, "lon": -151.351464},
        "PAIH": {"lat": 59.460767, "lon": -146.303444},
        "PAKC": {"lat": 58.679444, "lon": -156.629444},
        "PAPD": {"lat": 65.035114, "lon": -147.501431},
        # NEXRAD - Hawaii
        "PHKI": {"lat": 21.893889, "lon": -159.552500},
        "PHKM": {"lat": 20.125278, "lon": -155.777778},
        "PHMO": {"lat": 21.132778, "lon": -157.180278},
        "PHWA": {"lat": 19.095000, "lon": -155.568889},
        # NEXRAD - Guam
        "PGUA": {"lat": 13.455833, "lon": 144.811111},
        # NEXRAD - Korea
        "RKJK": {"lat": 35.924167, "lon": 126.622222},
        "RKSG": {"lat": 37.207569, "lon": 127.285561},
        # NEXRAD - Japan
        "RODN": {"lat": 26.307800, "lon": 127.903469},
        # NEXRAD - Azores
        "LPLA": {"lat": 38.730280, "lon": -27.321670},
        # NEXRAD - Puerto Rico
        "TJUA": {"lat": 18.115667, "lon": -66.078167},
        # TDWR sites  #I want you to completely revise how our nexrad data loops. Right now, it is using a really slow approach. Instead of doing each file one by one, use a multithreading approach to get it done almost instantly, and make the play button on the bottom center quick access bar instead of in the menu. No extra settings, just a play button that turns into a pause button that goes back to the latest radar frame then resumes fetching.
        "TADW": {"lat": 38.695000, "lon": -76.845000},
        "TATL": {"lat": 33.646944, "lon": -84.261944},
        "TBNA": {"lat": 35.980000, "lon": -86.661944},
        "TBOS": {"lat": 42.158056, "lon": -70.933056},
        "TBWI": {"lat": 39.090000, "lon": -76.630000},
        "TCLT": {"lat": 35.336944, "lon": -80.885000},
        "TCMH": {"lat": 40.006111, "lon": -82.715000},
        "TCVG": {"lat": 38.898056, "lon": -84.580000},
        "TDAL": {"lat": 32.926111, "lon": -96.968056},
        "TDAY": {"lat": 40.021944, "lon": -84.123056},
        "TDCA": {"lat": 38.758889, "lon": -76.961944},
        "TDEN": {"lat": 39.728056, "lon": -104.526111},
        "TDFW": {"lat": 33.065000, "lon": -96.918056},
        "TDTW": {"lat": 42.111111, "lon": -83.515000},
        "TEWR": {"lat": 40.593056, "lon": -74.270000},
        "TFLL": {"lat": 26.143056, "lon": -80.343889},
        "THOU": {"lat": 29.516111, "lon": -95.241944},
        "TIAD": {"lat": 39.083889, "lon": -77.528889},
        "TIAH": {"lat": 30.065000, "lon": -95.566944},
        "TICH": {"lat": 37.506944, "lon": -97.436944},
        "TIDS": {"lat": 39.636944, "lon": -86.436111},
        "TJBQ": {"lat": 18.485000, "lon": -67.143000},
        "TJFK": {"lat": 40.588889, "lon": -73.881111},
        "TJRV": {"lat": 18.256000, "lon": -65.637000},
        "TLAS": {"lat": 36.143889, "lon": -115.006944},
        "TLVE": {"lat": 41.290000, "lon": -82.008056},
        "TMCI": {"lat": 39.498056, "lon": -94.741944},
        "TMCO": {"lat": 28.343889, "lon": -81.326111},
        "TMDW": {"lat": 41.651111, "lon": -87.730000},
        "TMEM": {"lat": 34.896111, "lon": -89.993056},
        "TMIA": {"lat": 25.758056, "lon": -80.491111},
        "TMKE": {"lat": 42.818889, "lon": -88.046111},
        "TMSP": {"lat": 44.871111, "lon": -92.933056},
        "TMSY": {"lat": 30.021944, "lon": -90.403056},
        "TOKC": {"lat": 35.276111, "lon": -97.510000},
        "TORD": {"lat": 41.796944, "lon": -87.858056},
        "TPBI": {"lat": 26.688056, "lon": -80.273056},
        "TPHL": {"lat": 39.948889, "lon": -75.068889},
        "TPHX": {"lat": 33.421111, "lon": -112.163056},
        "TPIT": {"lat": 40.501111, "lon": -80.486111},
        "TRDU": {"lat": 36.001944, "lon": -78.696944},
        "TSDF": {"lat": 38.046111, "lon": -85.610000},
        "TSJU": {"lat": 18.473889, "lon": -66.178889},
        "TSLC": {"lat": 40.966944, "lon": -111.930000},
        "TSTL": {"lat": 38.805000, "lon": -90.488889},
        "TTPA": {"lat": 27.860000, "lon": -82.518056},
        "TTUL": {"lat": 36.071111, "lon": -95.826944},
    }
    
    return sites.get(site_id, {"lat": 39.8333333, "lon": -98.585522})

EARTH_RADIUS_KM = 6371.0088


def _polar_to_latlon_geodesic_vec(azimuths_deg, ranges_km, origin):
    """
    Vectorized great-circle destination from radar origin.
    Inputs are arrays of azimuth (deg clockwise from north) and distance (km).
    Returns tuple (lon_deg, lat_deg) as float32 arrays.
    """
    lat0 = np.radians(np.float64(origin['lat']))
    lon0 = np.radians(np.float64(origin['lon']))

    az = np.radians(np.asarray(azimuths_deg, dtype=np.float64))
    delta = np.asarray(ranges_km, dtype=np.float64) / EARTH_RADIUS_KM

    sin_lat0 = np.sin(lat0)
    cos_lat0 = np.cos(lat0)
    sin_delta = np.sin(delta)
    cos_delta = np.cos(delta)

    sin_lat2 = sin_lat0 * cos_delta + cos_lat0 * sin_delta * np.cos(az)
    lat2 = np.arcsin(np.clip(sin_lat2, -1.0, 1.0))

    y = np.sin(az) * sin_delta * cos_lat0
    x = cos_delta - sin_lat0 * sin_lat2
    lon2 = lon0 + np.arctan2(y, x)

    lat_deg = np.degrees(lat2)
    lon_deg = (np.degrees(lon2) + 540.0) % 360.0 - 180.0

    return lon_deg.astype(np.float32), lat_deg.astype(np.float32)


def fast_polar_to_latlon_vec(azimuths, ranges, origin):
    """
    Fast vectorized polar to lat/lon conversion using geodesic math.
    Inputs are 1D numpy arrays.
    Returns Nx2 array of [lon, lat].
    """
    lon, lat = _polar_to_latlon_geodesic_vec(azimuths, ranges, origin)
    return np.column_stack([lon, lat])

def calculate_vertices_batch(az_start_arr, az_end_arr, r1_arr, r2_arr, origin):
    """
    Fast vectorized polygon vertices calc using geodesic destination math.
    Reuses trig terms across corners to reduce total compute.
    Returns array shape (N, 4, 2) of lon/lat corners.
    """
    N = len(az_start_arr)
    if N == 0:
        return np.empty((0, 4, 2), dtype=np.float32)

    # Normalize input dtypes once to avoid repeated conversions.
    az_start_arr = np.asarray(az_start_arr, dtype=np.float32)
    az_end_arr = np.asarray(az_end_arr, dtype=np.float32)
    r1_arr = np.asarray(r1_arr, dtype=np.float32)
    r2_arr = np.asarray(r2_arr, dtype=np.float32)

    lat0 = np.radians(np.float32(origin['lat']))
    lon0 = np.radians(np.float32(origin['lon']))
    sin_lat0 = np.sin(lat0)
    cos_lat0 = np.cos(lat0)

    # Precompute azimuth trig terms once each.
    az_start_rad = np.radians(az_start_arr)
    az_end_rad = np.radians(az_end_arr)
    sin_az_start = np.sin(az_start_rad)
    cos_az_start = np.cos(az_start_rad)
    sin_az_end = np.sin(az_end_rad)
    cos_az_end = np.cos(az_end_rad)

    # Precompute distance trig terms once each.
    d1 = r1_arr / np.float32(EARTH_RADIUS_KM)
    d2 = r2_arr / np.float32(EARTH_RADIUS_KM)
    sin_d1 = np.sin(d1)
    cos_d1 = np.cos(d1)
    sin_d2 = np.sin(d2)
    cos_d2 = np.cos(d2)

    # Corner ordering: (az_start,r1), (az_start,r2), (az_end,r2), (az_end,r1)
    sin_az = np.stack([sin_az_start, sin_az_start, sin_az_end, sin_az_end], axis=1)
    cos_az = np.stack([cos_az_start, cos_az_start, cos_az_end, cos_az_end], axis=1)
    sin_d = np.stack([sin_d1, sin_d2, sin_d2, sin_d1], axis=1)
    cos_d = np.stack([cos_d1, cos_d2, cos_d2, cos_d1], axis=1)

    sin_lat2 = sin_lat0 * cos_d + cos_lat0 * sin_d * cos_az
    lat2 = np.arcsin(np.clip(sin_lat2, -1.0, 1.0))

    y = sin_az * sin_d * cos_lat0
    x = cos_d - sin_lat0 * sin_lat2
    lon2 = lon0 + np.arctan2(y, x)

    coords = np.empty((N, 4, 2), dtype=np.float32)
    coords[:, :, 0] = (np.degrees(lon2) + 540.0) % 360.0 - 180.0
    coords[:, :, 1] = np.degrees(lat2)
    return coords

@app.route('/api/radar/<site_id>', methods=['GET'])
def get_radar_data(site_id):
    product = request.args.get('product', 'N0B')
    source = _normalize_radar_source(request.args.get('source', 'level3'))
    try:
        if source == 'level2':
            import nexrad_level2
            specific_key = request.args.get('key')
            if specific_key:
                radar_file_path = download_level2_file_by_name(site_id, os.path.basename(specific_key))
            else:
                radar_file_path = download_level2_file(site_id)
            radar_data = nexrad_level2.NEXRADLevel2File(radar_file_path)
            azimuths, ranges, radar_values = _extract_level2_radar_data(radar_data, product)
            if azimuths is None or ranges is None or radar_values is None:
                return jsonify({"type": "FeatureCollection", "features": []})

            class _Level2Adapter:
                pass

            adapter = _Level2Adapter()
            adapter.level2_arrays = (azimuths, ranges, radar_values)
            geojson = convert_radar_to_geojson(adapter, site_id)
        else:
            radar_file_path = download_radar_file(site_id, product)
            if product.startswith('N0'):
                radar_data = nexrad.Level3File(radar_file_path)
            else:
                radar_data = nexrad.Level2File(radar_file_path)
            geojson = convert_radar_to_geojson(radar_data, site_id)
        return jsonify(geojson)
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

def convert_radar_to_geojson(radar_data, site_id):
    site_coords = get_site_coordinates(site_id)
    azimuths, ranges, radar_values = extract_radar_data(radar_data)
    if azimuths is None or ranges is None or radar_values is None:
        return {"type": "FeatureCollection", "features": []}

    azimuths = np.array(azimuths)
    ranges = np.array(ranges)
    radar_values = np.array(radar_values)

    # Wrap azimuths for polygon edges
    az_start = azimuths
    az_end = np.roll(azimuths, -1)
    az_diff = (az_end - az_start) % 360
    valid_az = az_diff < 10
    if not np.any(valid_az):
        return {"type": "FeatureCollection", "features": []}

    r1 = ranges[:-1]
    r2 = ranges[1:]
    valid_az_indices = np.where(valid_az)[0]

    features = []
    dbz_threshold = 0 # increase threshold to skip weak/noise signals

    for idx in valid_az_indices:
        az_s = az_start[idx]
        az_e = az_end[idx]
        vals = radar_values[idx, :-1]

        # Filter vals above threshold, drop tiny/noise pixels — huge perf gain
        valid_mask = (vals > dbz_threshold) & ~np.isnan(vals)
        if not np.any(valid_mask):
            continue

        valid_rng_indices = np.where(valid_mask)[0]

        az_start_arr = np.full(len(valid_rng_indices), az_s)
        az_end_arr = np.full(len(valid_rng_indices), az_e)
        r1_arr = r1[valid_rng_indices]
        r2_arr = r2[valid_rng_indices]

        verts = calculate_vertices_batch(az_start_arr, az_end_arr, r1_arr, r2_arr, site_coords)

        for i, rng_idx in enumerate(valid_rng_indices):
            poly_coords = verts[i].tolist()
            poly_coords.append(poly_coords[0])  # close polygon

            features.append({
                "type": "Feature",
                "geometry": {"type": "Polygon", "coordinates": [poly_coords]},
                "properties": {"dbz": float(vals[rng_idx])}
            })

    print(f"⚡ Built {len(features)} polygons after filtering weak signals")
    return {"type": "FeatureCollection", "features": features}


# --- NEW: Function to process data for WebGL ---
def convert_radar_to_webgl_data(radar_data, site_id, product='N0B'):
    """
    Processes radar data into flat arrays of vertices and dBZ/velocity values
    for efficient WebGL rendering.
    """
    t_start = time.time()
    site_coords = get_site_coordinates(site_id)
    
    t_extract = time.time()
    azimuths, ranges, radar_values = extract_radar_data(radar_data)
    if azimuths is None or ranges is None or radar_values is None:
        return np.array([], dtype=np.float32), np.array([], dtype=np.float32)
    print(f"  ⏱️  extract_radar_data: {(time.time() - t_extract)*1000:.1f}ms")

    azimuths = np.asarray(azimuths, dtype=np.float32)
    ranges = np.asarray(ranges, dtype=np.float32)
    radar_values = np.asarray(radar_values, dtype=np.float32)

    az_start = azimuths
    az_end = np.roll(azimuths, -1)
    az_diff = (az_end - az_start) % 360
    valid_az = az_diff < 10
    if not np.any(valid_az):
        return np.array([], dtype=np.float32), np.array([], dtype=np.float32)

    r1 = ranges[:-1]
    r2 = ranges[1:]

    # Fully vectorized valid-bin extraction (avoids Python loop/list overhead).
    is_velocity_product = product in ['N0G', 'N0S', 'N1G', 'N1S', 'N2G', 'N2S', 'N3G', 'N3S']
    vals_grid = radar_values[valid_az, :-1]
    if vals_grid.size == 0:
        return np.array([], dtype=np.float32), np.array([], dtype=np.float32)

    if is_velocity_product:
        valid_mask = ~np.isnan(vals_grid) & (vals_grid != -999.0)
    else:
        valid_mask = ~np.isnan(vals_grid) & (vals_grid > 0.0)

    row_idx, col_idx = np.nonzero(valid_mask)
    if row_idx.size == 0:
        return np.array([], dtype=np.float32), np.array([], dtype=np.float32)

    valid_az_start = az_start[valid_az]
    valid_az_end = az_end[valid_az]

    all_az_starts = valid_az_start[row_idx]
    all_az_ends = valid_az_end[row_idx]
    all_r1s = r1[col_idx]
    all_r2s = r2[col_idx]
    all_vals = vals_grid[row_idx, col_idx].astype(np.float32, copy=False)
    
    # Calculate all vertices at once
    t_vertices = time.time()
    verts_batch = calculate_vertices_batch(all_az_starts, all_az_ends, all_r1s, all_r2s, site_coords)
    print(f"  ⏱️  calculate_vertices_batch: {(time.time() - t_vertices)*1000:.1f}ms")
    
    # VECTORIZED: Build triangles using NumPy array operations (100x faster than Python loops!)
    t_triangles = time.time()
    n_quads = len(verts_batch)
    
    # verts_batch is already shape (n_quads, 4, 2)
    verts_array = verts_batch
    
    # Pre-allocate output arrays
    vertices_flat = np.empty(n_quads * 12, dtype=np.float32)  # 6 vertices * 2 coords
    values_flat = np.repeat(all_vals, 6)  # Each value repeated 6 times
    
    # Vectorized triangle construction - reshape and interleave
    # Triangle 1: v0, v1, v2
    vertices_flat[0::12] = verts_array[:, 0, 0]  # v0 lon
    vertices_flat[1::12] = verts_array[:, 0, 1]  # v0 lat
    vertices_flat[2::12] = verts_array[:, 1, 0]  # v1 lon
    vertices_flat[3::12] = verts_array[:, 1, 1]  # v1 lat
    vertices_flat[4::12] = verts_array[:, 2, 0]  # v2 lon
    vertices_flat[5::12] = verts_array[:, 2, 1]  # v2 lat
    
    # Triangle 2: v0, v2, v3
    vertices_flat[6::12] = verts_array[:, 0, 0]  # v0 lon
    vertices_flat[7::12] = verts_array[:, 0, 1]  # v0 lat
    vertices_flat[8::12] = verts_array[:, 2, 0]  # v2 lon
    vertices_flat[9::12] = verts_array[:, 2, 1]  # v2 lat
    vertices_flat[10::12] = verts_array[:, 3, 0] # v3 lon
    vertices_flat[11::12] = verts_array[:, 3, 1] # v3 lat
    
    print(f"  ⏱️  build_triangles (vectorized): {(time.time() - t_triangles)*1000:.1f}ms")
    
    elapsed = (time.time() - t_start) * 1000
    print(f"⚡ Built {n_quads * 2} triangles for WebGL ({product}) in {elapsed:.1f}ms")
    
    # Return NumPy arrays directly to avoid costly list conversion
    return vertices_flat, values_flat

@app.route('/api/radar-latest-key/<site_id>', methods=['GET'])
def get_latest_radar_key_api(site_id):
    product = request.args.get('product', 'N0B')
    source = _normalize_radar_source(request.args.get('source', 'level3'))
    try:
        if source == 'level2':
            key = get_latest_level2_file(site_id)
            try:
                token, complete, coverage, rays, prod_bytes = _build_level2_tilt_update_token(site_id, product, key)
            except Exception as token_err:
                print(f"[L2-Token] ⚠️  Falling back to key token due to error: {token_err}")
                token, complete, coverage, rays, prod_bytes = key, False, 0.0, 0, 0
            return jsonify({
                "key": key,
                "updateToken": token,
                "sweepComplete": complete,
                "sweepCoverageDeg": coverage,
                "sweepRays": rays,
                "prodBytes": prod_bytes,
            })
        else:
            key = get_latest_radar_key(site_id, product)
        return jsonify({"key": key, "updateToken": key})
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route('/api/radar-level2-files/<site_id>', methods=['GET'])
def get_level2_file_list(site_id):
    try:
        limit = request.args.get('limit', default=200, type=int)
        limit = max(1, min(2000, int(limit)))
        files = _fetch_level2_dirlist(site_id)
        tail = files[-limit:]
        return jsonify({
            "site": _normalize_level2_site_id(site_id),
            "count": len(tail),
            "files": [
                {
                    "key": item["key"],
                    "timestamp": item["timestamp"].isoformat(),
                }
                for item in tail
            ],
        })
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route('/api/radar/level2-incremental', methods=['GET'])
def get_level2_incremental():
    """Get new lowest-elevation scans (including SAILS) since last request."""
    site_id = request.args.get('site', 'KDMX').upper()
    product = request.args.get('product', 'N0B').upper()
    elevation = float(request.args.get('elevation', '0.5'))  # Target elevation
    site_id = _normalize_level2_site_id(site_id)

    try:
        files = _fetch_level2_dirlist(site_id)
        if not files:
            return jsonify({'error': 'No files found'}), 404
        latest_file = files[-1]
        file_url = latest_file['url']
        filename = latest_file['key']
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    with active_monitors_lock:
        monitor = active_monitors.get(site_id)
        if monitor is None or monitor.filename != filename:
            if monitor is not None:
                monitor.cleanup()
            monitor = Level2StreamMonitor(file_url, site_id, filename)
            active_monitors[site_id] = monitor

    monitor.fetch_new_bytes()
    new_payload = monitor.get_new_lowest_elevation_scans(product, target_elevation=elevation)

    if not new_payload or 'scans' not in new_payload:
        return jsonify({
            'site': site_id,
            'newScans': [],
            'totalBytes': monitor.downloaded_bytes,
            'scanCount': monitor.last_scan_count,
        })

    # Convert all new scans to WebGL format
    scans_data = []
    for scan in new_payload['scans']:
        try:
            class _Level2Adapter:
                pass

            adapter = _Level2Adapter()
            adapter.level2_arrays = (
                scan['azimuths'],
                scan['ranges'],
                scan['values'],
            )
            vertices, values = convert_radar_to_webgl_data(adapter, site_id, product)
            
            scans_data.append({
                'elevation': scan.get('elevation'),
                'sweepIndex': scan.get('sweep_index'),
                'timestamp': scan.get('timestamp'),
                'vertices': vertices.tolist() if hasattr(vertices, 'tolist') else list(vertices),
                'values': values.tolist() if hasattr(values, 'tolist') else list(values),
                'rayCount': scan.get('ray_count', 0),
            })
        except Exception as e:
            print(f"⚠️  Failed to convert scan to WebGL: {e}")
            continue

    return jsonify({
        'site': site_id,
        'newScans': scans_data,
        'totalBytes': monitor.downloaded_bytes,
        'scanCount': monitor.last_scan_count,
    })



@app.route('/api/radar/level2-stream', methods=['GET'])
def stream_level2_rays():
    """SSE endpoint that pushes new rays and automatically switches to new files."""
    site_id = request.args.get('site', 'KDMX').upper()
    product = request.args.get('product', 'N0B').upper()
    site_id = _normalize_level2_site_id(site_id)
    elevation = float(request.args.get('elevation', '0.5'))
    # Default to 0.5s polling for lower-latency updates (can be overriden with ?interval=...)
    check_interval = max(0.5, float(request.args.get('interval', 3)))
    # Rotate long-lived SSE responses before sync-worker timeout kills the worker.
    # EventSource clients will reconnect automatically.
    stream_max_seconds = float(os.getenv('SSE_STREAM_MAX_SECONDS', '45'))
    stream_max_seconds = max(10.0, min(300.0, stream_max_seconds))

    def _should_rotate(started_at):
        return (time.monotonic() - started_at) >= stream_max_seconds

    def generate():
        started_at = time.monotonic()
        try:
            while True:
                if _should_rotate(started_at):
                    yield "event: stream-rotate\ndata: {\"reason\":\"pre-timeout-rotate\"}\n\n"
                    return

                # 1. ALWAYS check for the latest filename inside the loop
                try:
                    files = _fetch_level2_dirlist(site_id)
                    if not files:
                        time.sleep(check_interval)
                        continue
                    
                    latest_file = files[-1]
                    file_url = latest_file['url']
                    filename = latest_file['key']
                except Exception as e:
                    print(f"⚠️ Directory fetch error: {e}")
                    time.sleep(check_interval)
                    continue

                # 2. Get or Create monitor (Switching if filename changed)
                with active_monitors_lock:
                    monitor = active_monitors.get(site_id)
                    if monitor is None or monitor.filename != filename:
                        print(f"🔄 SWITCHING TO NEW FILE: {filename}")
                        if monitor is not None:
                            monitor.cleanup()
                        monitor = Level2StreamMonitor(file_url, site_id, filename)
                        active_monitors[site_id] = monitor

                # 3. Fetch data from the current monitor
                monitor.fetch_new_bytes()
                new_payload = monitor.get_new_lowest_elevation_scans(
                    product,
                    target_elevation=elevation,
                )

                if new_payload and new_payload.get('scans'):
                    # For each new/grown scan, convert and stream to clients. If the
                    # scan appears partial (coverage < 360°), enter a short rapid-fetch
                    # mode to pull the remaining rays quickly.
                    for scan in new_payload['scans']:
                        # Compute azimuth coverage (client uses this to animate sweep)
                        try:
                            coverage_deg = float(_compute_azimuth_coverage_degrees(scan.get('azimuths')))
                        except Exception:
                            coverage_deg = 0.0

                        formatted_timestamp = format_nexrad_timestamp(scan.get('timestamp'))

                        # determine current sweep azimuth (head) if available
                        try:
                            sweep_az = float(scan.get('azimuths', [])[-1]) if scan.get('azimuths') else None
                        except Exception:
                            sweep_az = None

                        data = {
                            'site': site_id,
                            'product': product,
                            'sessionKey': filename,
                            'sweepIndex': scan.get('sweep_index'),
                            'elevation': scan.get('elevation'),
                            'timestamp': formatted_timestamp,
                            'rayCount': int(len(scan.get('azimuths', []))),
                            'sweepAzimuth': sweep_az,
                            'sweepCoverageDeg': coverage_deg,
                            'sweepComplete': bool(coverage_deg >= 359.5),
                            'sweepRays': int(len(scan.get('azimuths', []))),
                            'totalBytes': int(monitor.downloaded_bytes),
                            # Heavy payload removed to save egress and memory.
                            # Client should fetch binary blob via /api/radar-webgl/<site>
                            'fetchRequired': True,
                        }

                        yield f"data: {json.dumps(data)}\n\n"

                        # If this scan is partial, initialize/refresh rapid attempts counter
                        try:
                            if coverage_deg > 0 and coverage_deg < 360:
                                monitor._rapid_attempts_left = 5
                        except Exception:
                            pass
                else:
                    yield ": keepalive\n\n"

                # Rapid-fetch mode: perform short-interval polls up to N attempts
                rapid_left = int(getattr(monitor, '_rapid_attempts_left', 0) or 0)
                while rapid_left > 0:
                    if _should_rotate(started_at):
                        yield "event: stream-rotate\ndata: {\"reason\":\"pre-timeout-rotate\"}\n\n"
                        return

                    # short wait to allow more bytes to arrive
                    time.sleep(0.18)
                    monitor.fetch_new_bytes()
                    rapid_payload = monitor.get_new_lowest_elevation_scans(
                        product,
                        target_elevation=elevation,
                    )

                    if rapid_payload and rapid_payload.get('scans'):
                        # Stream any newly grown scans immediately and refresh rapid counter
                        for scan in rapid_payload['scans']:
                            try:
                                coverage_deg = float(_compute_azimuth_coverage_degrees(scan.get('azimuths')))
                            except Exception:
                                coverage_deg = 0.0

                            formatted_timestamp = format_nexrad_timestamp(scan.get('timestamp'))

                            try:
                                sweep_az2 = float(scan.get('azimuths', [])[-1]) if scan.get('azimuths') else None
                            except Exception:
                                sweep_az2 = None

                            rdata = {
                                'site': site_id,
                                'product': product,
                                'sessionKey': filename,
                                'sweepIndex': scan.get('sweep_index'),
                                'elevation': scan.get('elevation'),
                                'timestamp': formatted_timestamp,
                                'rayCount': int(len(scan.get('azimuths', []))),
                                'sweepAzimuth': sweep_az2,
                                'sweepCoverageDeg': coverage_deg,
                                'sweepComplete': bool(coverage_deg >= 359.5),
                                'sweepRays': int(len(scan.get('azimuths', []))),
                                'totalBytes': int(monitor.downloaded_bytes),
                                # Heavy payload removed; client should call /api/radar-webgl to fetch blob
                                'fetchRequired': True,
                            }

                            yield f"data: {json.dumps(rdata)}\n\n"

                        # refresh rapid attempts after receiving growth
                        monitor._rapid_attempts_left = 5
                        rapid_left = monitor._rapid_attempts_left
                    else:
                        # decrement attempts when nothing new
                        monitor._rapid_attempts_left = max(0, getattr(monitor, '_rapid_attempts_left', 0) - 1)
                        rapid_left = monitor._rapid_attempts_left

                time.sleep(check_interval)
        except GeneratorExit:
            pass
        except Exception as e:
            print(f"❌ Stream Error: {e}")
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

    headers = {'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'}
    return Response(generate(), mimetype='text/event-stream', headers=headers)


# --- NEW: API Endpoint for WebGL data ---
@app.route('/api/radar-webgl/<site_id>', methods=['GET'])
def get_radar_data_webgl(site_id):
    product = request.args.get('product', 'N0B')
    format_type = request.args.get('format', 'json')  # 'json' or 'binary'
    gzip_enabled = request.args.get('gzip', '0') == '1'
    specific_key = request.args.get('key', None)  # Optional: specific radar file key
    source = _normalize_radar_source(request.args.get('source', 'level3'))

    try:
        # Aggressive temp cleanup to avoid disk bloat on constrained hosts
        cleanup_temp_files()
    except Exception:
        pass

    try:
        _, compressed_blob, _ = _get_or_build_radar_blob(
            site_id=site_id,
            product=product,
            source=source,
            specific_key=specific_key,
            gzip_enabled=gzip_enabled,
        )

        if format_type == 'binary':
            return create_binary_response_from_blob(compressed_blob, is_gzipped=gzip_enabled)
        else:
            return jsonify({"error": "JSON format disabled for memory optimization. Use format=binary"})
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route('/api/radar-webgl-batch/<site_id>', methods=['POST'])
def get_radar_data_webgl_batch(site_id):
    payload = request.get_json(silent=True) or {}
    product = str(payload.get('product') or 'N0B').strip().upper()
    source = _normalize_radar_source(payload.get('source', 'level3'))
    gzip_enabled = bool(payload.get('gzip', False))
    include_payload = bool(payload.get('includePayload', True))
    keys = payload.get('keys') or []
    max_workers = int(payload.get('maxWorkers') or RADAR_BATCH_MAX_WORKERS)

    if not isinstance(keys, list) or not keys:
        return jsonify({"error": "Request body must include a non-empty keys array."}), 400

    keys = [str(k).strip() for k in keys if str(k).strip()]
    if not keys:
        return jsonify({"error": "No valid keys were provided."}), 400

    if len(keys) > RADAR_BATCH_MAX_KEYS:
        return jsonify({"error": f"Too many keys. Max {RADAR_BATCH_MAX_KEYS}."}), 400

    max_workers = max(1, min(RADAR_BATCH_MAX_WORKERS, max_workers, len(keys)))
    started = time.time()

    try:
        cleanup_temp_files()
    except Exception:
        pass

    def _process(single_key):
        item_start = time.time()
        try:
            resolved_key, blob, cache_hit = _get_or_build_radar_blob(
                site_id=site_id,
                product=product,
                source=source,
                specific_key=single_key,
                gzip_enabled=gzip_enabled,
            )

            result = {
                "key": resolved_key,
                "status": "ok",
                "cacheHit": bool(cache_hit),
                "byteLength": len(blob),
                "elapsedMs": int((time.time() - item_start) * 1000),
                "isGzipped": bool(gzip_enabled),
            }
            if include_payload:
                result["payloadBase64"] = base64.b64encode(blob).decode('ascii')
            return result
        except Exception as err:
            return {
                "key": single_key,
                "status": "error",
                "error": str(err),
                "elapsedMs": int((time.time() - item_start) * 1000),
            }

    results = [None] * len(keys)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_process, key): idx
            for idx, key in enumerate(keys)
        }
        for future in as_completed(futures):
            idx = futures[future]
            try:
                results[idx] = future.result()
            except Exception as err:
                results[idx] = {
                    "key": keys[idx],
                    "status": "error",
                    "error": str(err),
                    "elapsedMs": 0,
                }

    ok_count = sum(1 for r in results if r and r.get("status") == "ok")
    return jsonify({
        "siteId": site_id,
        "product": product,
        "source": source,
        "count": len(results),
        "okCount": ok_count,
        "errorCount": len(results) - ok_count,
        "elapsedMs": int((time.time() - started) * 1000),
        "results": results,
    })

def generate_binary_blob(vertices, values, use_gzip=False):
    """
    Generates the compressed binary blob from numpy arrays.
    Returns: bytes (gzipped)
    """
    import struct
    import gzip

    # Ensure float32 without unnecessary copies.
    vertices_array = np.asarray(vertices, dtype=np.float32)
    values_array = np.asarray(values, dtype=np.float32)

    vertex_count = len(values_array)

    # 1. Vertex count
    binary_data = bytearray(struct.pack('<I', vertex_count))
    # 2. Vertices
    binary_data.extend(vertices_array.tobytes())
    # 3. Values
    binary_data.extend(values_array.tobytes())

    payload = bytes(binary_data)
    if use_gzip:
        # Favor speed over max compression ratio for lower request latency.
        return gzip.compress(payload, compresslevel=1)
    return payload


def create_binary_response_from_blob(payload_bytes, extra_headers=None, is_gzipped=False):
    """Wraps cached compressed bytes into a Flask Response."""
    response_headers = {
        'Content-Type': 'application/octet-stream',
        'Content-Length': str(len(payload_bytes))
    }
    if is_gzipped:
        response_headers['Content-Encoding'] = 'gzip'
    if extra_headers:
        response_headers.update(extra_headers)

    return Response(
        payload_bytes,
        mimetype='application/octet-stream',
        headers=response_headers
    )


def _build_radar_cache_key(site_id, product, source, key, file_size, file_mtime, gzip_enabled):
    return f"{source}:{site_id}:{product}:{key}:{int(file_size)}:{float(file_mtime):.6f}:gzip{int(bool(gzip_enabled))}"


def _resolve_radar_file(site_id, product, source, specific_key):
    if source == 'level2':
        if specific_key:
            key = os.path.basename(specific_key)
            radar_file_path = download_level2_file_by_name(site_id, key, skip_corrupted=True)
        else:
            key = get_latest_level2_file(site_id)
            radar_file_path = download_level2_file_by_name(site_id, key, skip_corrupted=True)
    else:
        if specific_key:
            key = specific_key
            radar_file_path = download_radar_file_by_key(specific_key)
        else:
            key = get_latest_radar_key(site_id, product)
            radar_file_path = download_radar_file_by_key(key)
    return key, radar_file_path


def _decode_to_webgl_arrays(radar_file_path, site_id, product, source):
    if source == 'level2':
        import nexrad_level2
        radar_data = nexrad_level2.NEXRADLevel2File(radar_file_path)
        try:
            azimuths, ranges, radar_values = _extract_level2_radar_data(radar_data, product)
            if azimuths is None or ranges is None or radar_values is None:
                raise ValueError(f"No Level 2 radar data available for {site_id} ({product})")

            class _Level2Adapter:
                pass

            adapter = _Level2Adapter()
            adapter.level2_arrays = (azimuths, ranges, radar_values)
            return convert_radar_to_webgl_data(adapter, site_id, product)
        finally:
            try:
                radar_data.close()
            except Exception:
                pass

    if product.startswith('N0'):
        radar_data = nexrad.Level3File(radar_file_path)
    else:
        radar_data = nexrad.Level2File(radar_file_path)
    return convert_radar_to_webgl_data(radar_data, site_id, product)


def _get_or_build_radar_blob(site_id, product, source, specific_key=None, gzip_enabled=False):
    key, radar_file_path = _resolve_radar_file(site_id, product, source, specific_key)

    file_stats = os.stat(radar_file_path)
    cache_key = _build_radar_cache_key(
        site_id=site_id,
        product=product,
        source=source,
        key=key,
        file_size=file_stats.st_size,
        file_mtime=file_stats.st_mtime,
        gzip_enabled=gzip_enabled,
    )

    with processed_data_lock:
        cached = processed_data_cache.get(cache_key)
        if cached is not None:
            return key, cached, True

    inflight_event = None
    is_owner = False
    with radar_blob_inflight_lock:
        inflight_event = radar_blob_inflight.get(cache_key)
        if inflight_event is None:
            inflight_event = Event()
            radar_blob_inflight[cache_key] = inflight_event
            is_owner = True

    if not is_owner:
        inflight_event.wait(timeout=15.0)
        with processed_data_lock:
            cached = processed_data_cache.get(cache_key)
            if cached is not None:
                return key, cached, True

    try:
        vertices, values = _decode_to_webgl_arrays(
            radar_file_path=radar_file_path,
            site_id=site_id,
            product=product,
            source=source,
        )
        blob = generate_binary_blob(vertices, values, use_gzip=gzip_enabled)

        try:
            del vertices
            del values
        except Exception:
            pass

        with processed_data_lock:
            processed_data_cache[cache_key] = blob
            processed_data_cache.move_to_end(cache_key)
            while len(processed_data_cache) > PROCESSED_CACHE_MAX_SIZE:
                processed_data_cache.popitem(last=False)

        gc.collect()
        return key, blob, False
    finally:
        if is_owner:
            with radar_blob_inflight_lock:
                event = radar_blob_inflight.pop(cache_key, None)
                if event is not None:
                    event.set()


def cleanup_temp_files():
    """Removes files in ./temp older than 5 minutes."""
    global last_temp_cleanup_ts
    now = time.time()
    if (now - last_temp_cleanup_ts) < TEMP_CLEANUP_INTERVAL_SEC:
        return
    last_temp_cleanup_ts = now

    temp_path = Path("./temp")
    if not temp_path.exists():
        return

    try:
        for p in temp_path.rglob("*"):
            if p.is_file():
                # Delete if older than 5 mins
                if now - p.stat().st_mtime > 300:
                    try:
                        p.unlink()
                    except Exception:
                        pass
    except Exception:
        pass


# Backwards-compatible helper for existing call sites: builds blob then response
def create_binary_response(vertices, values, extra_headers=None):
    payload = generate_binary_blob(vertices, values)
    return create_binary_response_from_blob(payload, extra_headers=extra_headers)


def _normalize_hrrr_date(date_str):
    if not date_str:
        return None
    normalized = date_str.strip().replace("-", "")
    if not re.fullmatch(r"\d{8}", normalized):
        raise ValueError("Invalid date format. Use YYYYMMDD or YYYY-MM-DD.")
    return normalized


def _normalize_model_name(model):
    normalized = (model or "hrrr").strip().lower()
    aliases = {
        "hrrr": "hrrr",
        "rrfs": "rrfs-a",
        "rrfs-a": "rrfs-a",
        "rrfsa": "rrfs-a",
        "nam": "nam3k",
        "nam3k": "nam3k",
        "namnest": "nam3k",
        "nam-nest": "nam3k",
        "mrms": "mrms",
    }
    resolved = aliases.get(normalized)
    if not resolved:
        raise ValueError("Unsupported model. Supported models: hrrr, rrfs-a, nam3k, mrms.")
    return resolved


def _build_model_run_candidates(target_date=None, target_run_hour=None, lookback_hours=HRRR_DEFAULT_LOOKBACK_HOURS):
    if target_date and target_run_hour is not None:
        return [(target_date, int(target_run_hour))]

    now_utc = datetime.now(timezone.utc)
    candidates = []
    for hours_back in range(lookback_hours + 1):
        dt = now_utc - timedelta(hours=hours_back)
        date_str = dt.strftime("%Y%m%d")
        hour = dt.hour
        pair = (date_str, hour)
        if pair not in candidates:
            candidates.append(pair)
    return candidates


def _build_model_file_urls(model, date_str, run_hour, forecast_hour):
    model_name = _normalize_model_name(model)
    run_hour_int = int(run_hour)
    forecast_hour_int = int(forecast_hour)

    if model_name == "hrrr":
        file_name = f"hrrr.t{run_hour_int:02d}z.wrfsubhf{forecast_hour_int:02d}.grib2"
        grib_url = f"{HRRR_BASE_URL}/hrrr.{date_str}/conus/{file_name}"
    elif model_name == "rrfs-a":
        file_name = f"rrfs.t{run_hour_int:02d}z.natlev.3km.f{forecast_hour_int:03d}.na.grib2"
        grib_url = f"{RRFS_A_BASE_URL}/rrfs_a/rrfs.{date_str}/{run_hour_int:02d}/{file_name}"
    else:
        file_name = f"nam.t{run_hour_int:02d}z.conusnest.hiresf{forecast_hour_int:02d}.tm00.grib2"
        grib_url = f"{NAM3K_BASE_URL}/nam.{date_str}/{file_name}"

    return grib_url, f"{grib_url}.idx", file_name


def _get_hrrr_inventory(idx_url):
    now = time.monotonic()
    with hrrr_idx_cache_lock:
        entry = hrrr_idx_cache.get(idx_url)
        if entry and (now - entry["time"]) <= HRRR_IDX_CACHE_TTL:
            print(f"[HRRR] .idx cache hit: {idx_url}")
            return entry["inventory"]

    print(f"[HRRR] Fetching .idx: {idx_url}")
    response = _http_get(idx_url, timeout=(4, 20))
    response.raise_for_status()
    lines = response.text.splitlines()

    inventory = []
    for raw_line in lines:
        parts = raw_line.split(":")
        if len(parts) < 5:
            continue
        try:
            start_offset = int(parts[1])
        except Exception:
            continue

        inventory.append({
            "start": start_offset,
            "var": parts[3].strip(),
            "level": parts[4].strip() if len(parts) > 4 else "",
            "line": raw_line,
        })

    for i in range(len(inventory)):
        next_start = inventory[i + 1]["start"] if i + 1 < len(inventory) else None
        inventory[i]["end"] = (next_start - 1) if next_start is not None else None

    print(f"[HRRR] .idx parsed: {len(inventory)} entries from {idx_url}")

    with hrrr_idx_cache_lock:
        hrrr_idx_cache[idx_url] = {
            "time": now,
            "inventory": inventory,
        }

    return inventory


def _match_model_inventory_entry(inventory, variable_key):
    variable_specs = {
        "tmp2m": [("TMP", "2 m above ground")],
        "rh2m": [("RH", "2 m above ground")],
        "refc": [("REFC", "entire atmosphere")],
        "ugrd10m": [("UGRD", "10 m above ground")],
        "vgrd10m": [("VGRD", "10 m above ground")],
        "crain": [("CRAIN", "surface")],
        "cfrzr": [("CFRZR", "surface")],
        "cicep": [("CICEP", "surface")],
        "csnow": [("CSNOW", "surface")],
        "ptype": [("CRAIN", "surface")],
        "refc_ptype": [("REFC", "entire atmosphere")],
    }
    specs = variable_specs.get(variable_key)
    if not specs:
        raise ValueError(f"Unsupported HRRR variable '{variable_key}'.")

    for var_name, level_match in specs:
        for item in inventory:
            if item["var"] == var_name and level_match.lower() in item["level"].lower():
                return item

    for var_name, _ in specs:
        for item in inventory:
            if item["var"] == var_name:
                return item

    return None


def _match_model_precip_entries(inventory, include_refc=False):
    required_vars = ["crain", "cfrzr", "cicep", "csnow"]
    if include_refc:
        required_vars.insert(0, "refc")

    entries = {}
    for variable_key in required_vars:
        entry = _match_model_inventory_entry(inventory, variable_key)
        if entry is None:
            return None
        entries[variable_key] = entry

    return entries


def _download_hrrr_message(grib_url, start, end, out_path):
    out_tmp = Path(str(out_path) + ".part")
    out_tmp.unlink(missing_ok=True)

    headers = {"Range": f"bytes={start}-{end}" if end is not None else f"bytes={start}-"}
    response = _http_get(grib_url, headers=headers, stream=True, timeout=(5, 60))
    try:
        if response.status_code not in (200, 206):
            response.raise_for_status()
        with open(out_tmp, "wb") as f:
            for chunk in response.iter_content(STREAM_CHUNK_SIZE):
                if chunk:
                    f.write(chunk)
    finally:
        response.close()

    out_tmp.replace(out_path)


def _download_hrrr_full(grib_url, out_path):
    out_tmp = Path(str(out_path) + ".part")
    out_tmp.unlink(missing_ok=True)
    response = _http_get(grib_url, stream=True, timeout=(5, 90))
    try:
        response.raise_for_status()
        with open(out_tmp, "wb") as f:
            for chunk in response.iter_content(STREAM_CHUNK_SIZE):
                if chunk:
                    f.write(chunk)
    finally:
        response.close()
    out_tmp.replace(out_path)


def _extract_hrrr_grids(grib_path):
    if xr is None:
        raise RuntimeError("xarray is not installed. Install xarray and cfgrib for HRRR support.")

    ds = xr.open_dataset(grib_path, engine="cfgrib", backend_kwargs={"indexpath": ""})
    try:
        data_var_name = next(iter(ds.data_vars.keys()))
        data_var = ds[data_var_name]
        values = np.array(data_var.values, dtype=np.float32)
        while values.ndim > 2:
            values = values[0]

        if "latitude" in ds:
            lat = np.array(ds["latitude"].values, dtype=np.float32)
        elif "lat" in ds:
            lat = np.array(ds["lat"].values, dtype=np.float32)
        else:
            raise RuntimeError("Latitude grid missing in HRRR dataset.")

        if "longitude" in ds:
            lon = np.array(ds["longitude"].values, dtype=np.float32)
        elif "lon" in ds:
            lon = np.array(ds["lon"].values, dtype=np.float32)
        else:
            raise RuntimeError("Longitude grid missing in HRRR dataset.")

        if lat.ndim == 1 and lon.ndim == 1:
            lon, lat = np.meshgrid(lon, lat)

        lon = np.where(lon > 180.0, lon - 360.0, lon).astype(np.float32)
        lat = lat.astype(np.float32)
        units = str(data_var.attrs.get("units") or "")
        return lon, lat, values, data_var_name, units
    finally:
        ds.close()


def _crop_hrrr_to_bounds(lon, lat, values, bounds):
    if not bounds:
        return lon, lat, values

    min_lon, min_lat, max_lon, max_lat = bounds
    mask = (lat >= min_lat) & (lat <= max_lat) & (lon >= min_lon) & (lon <= max_lon)
    if not np.any(mask):
        return lon, lat, values

    row_mask = np.any(mask, axis=1)
    col_mask = np.any(mask, axis=0)
    row_indices = np.where(row_mask)[0]
    col_indices = np.where(col_mask)[0]
    if row_indices.size == 0 or col_indices.size == 0:
        return lon, lat, values

    r0, r1 = int(row_indices[0]), int(row_indices[-1]) + 1
    c0, c1 = int(col_indices[0]), int(col_indices[-1]) + 1
    return lon[r0:r1, c0:c1], lat[r0:r1, c0:c1], values[r0:r1, c0:c1]


def _normalize_hrrr_units(variable, values, units):
    normalized_units = (units or "").strip()
    variable = (variable or "").lower()

    if variable == "tmp2m":
        lowered_units = normalized_units.lower()
        if lowered_units in ("k", "kelvin"):
            return (values - np.float32(273.15)).astype(np.float32), "°C"
        if lowered_units in ("c", "degc", "degree_celsius", "degrees celsius", "°c"):
            return values.astype(np.float32), "°C"

    unit_map = {
        "percent": "%",
        "%": "%",
        "dbz": "dBZ",
        "m s**-1": "m/s",
        "m/s": "m/s",
        "m s-1": "m/s",
    }
    display_units = unit_map.get(normalized_units.lower(), normalized_units)
    return values.astype(np.float32), display_units


def _build_precip_type_grid(crain, cfrzr, cicep, csnow):
    threshold = np.float32(HRRR_PTYPE_FLAG_THRESHOLD)

    precip_type = np.zeros(crain.shape, dtype=np.float32)
    precip_type[crain >= threshold] = np.float32(1.0)
    precip_type[cfrzr >= threshold] = np.float32(2.0)
    precip_type[cicep >= threshold] = np.float32(3.0)
    precip_type[csnow >= threshold] = np.float32(4.0)
    return precip_type


def _encode_reflectivity_by_precip_type(refc, precip_type):
    encoded = np.full(refc.shape, np.nan, dtype=np.float32)
    clipped_refc = np.clip(refc.astype(np.float32), np.float32(0.0), np.float32(95.0))

    rain_mask = precip_type == np.float32(1.0)
    frzr_mask = precip_type == np.float32(2.0)
    icep_mask = precip_type == np.float32(3.0)
    snow_mask = precip_type == np.float32(4.0)

    encoded[rain_mask] = clipped_refc[rain_mask] + HRRR_PTYPE_ENCODE_OFFSETS["rain"]
    encoded[frzr_mask] = clipped_refc[frzr_mask] + HRRR_PTYPE_ENCODE_OFFSETS["frzr"]
    encoded[icep_mask] = clipped_refc[icep_mask] + HRRR_PTYPE_ENCODE_OFFSETS["icep"]
    encoded[snow_mask] = clipped_refc[snow_mask] + HRRR_PTYPE_ENCODE_OFFSETS["snow"]

    encoded[~np.isfinite(refc)] = np.nan
    encoded[refc < np.float32(HRRR_REFC_MIN_DBZ)] = np.nan
    return encoded


def _grid_to_triangle_buffers(lon, lat, values, min_value=None):
    stride = 1

    rows, cols = values.shape
    if rows < 2 or cols < 2:
        return np.array([], dtype=np.float32), np.array([], dtype=np.float32), stride

    v00_lon = lon[:-1, :-1].ravel()
    v00_lat = lat[:-1, :-1].ravel()
    v10_lon = lon[:-1, 1:].ravel()
    v10_lat = lat[:-1, 1:].ravel()
    v11_lon = lon[1:, 1:].ravel()
    v11_lat = lat[1:, 1:].ravel()
    v01_lon = lon[1:, :-1].ravel()
    v01_lat = lat[1:, :-1].ravel()

    c00 = values[:-1, :-1].ravel()
    c10 = values[:-1, 1:].ravel()
    c11 = values[1:, 1:].ravel()
    c01 = values[1:, :-1].ravel()

    corners = np.stack([c00, c10, c11, c01], axis=1)
    cell_value = np.nanmean(corners, axis=1).astype(np.float32)
    valid = np.isfinite(cell_value)
    valid &= np.isfinite(v00_lon) & np.isfinite(v00_lat)
    valid &= np.isfinite(v10_lon) & np.isfinite(v10_lat)
    valid &= np.isfinite(v11_lon) & np.isfinite(v11_lat)
    valid &= np.isfinite(v01_lon) & np.isfinite(v01_lat)

    if min_value is not None:
        min_value = float(min_value)
        valid &= np.all(np.isfinite(corners), axis=1)
        valid &= np.all(corners >= min_value, axis=1)

    if not np.any(valid):
        return np.array([], dtype=np.float32), np.array([], dtype=np.float32), stride

    v00_lon = v00_lon[valid]
    v00_lat = v00_lat[valid]
    v10_lon = v10_lon[valid]
    v10_lat = v10_lat[valid]
    v11_lon = v11_lon[valid]
    v11_lat = v11_lat[valid]
    v01_lon = v01_lon[valid]
    v01_lat = v01_lat[valid]
    cell_value = cell_value[valid]

    n_cells = cell_value.size
    vertices = np.empty(n_cells * 12, dtype=np.float32)

    vertices[0::12] = v00_lon
    vertices[1::12] = v00_lat
    vertices[2::12] = v10_lon
    vertices[3::12] = v10_lat
    vertices[4::12] = v11_lon
    vertices[5::12] = v11_lat
    vertices[6::12] = v00_lon
    vertices[7::12] = v00_lat
    vertices[8::12] = v11_lon
    vertices[9::12] = v11_lat
    vertices[10::12] = v01_lon
    vertices[11::12] = v01_lat

    values_out = np.repeat(cell_value, 6).astype(np.float32)
    return vertices, values_out, stride


def _make_hrrr_cache_key(parts):
    raw = "|".join(str(p) for p in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _build_hrrr_binary_headers(meta):
    return {
        "X-HRRR-Run-Date": str(meta.get("runDate") or ""),
        "X-HRRR-Run-Hour": str(meta.get("runHour") or ""),
        "X-HRRR-Run-ISO": str(meta.get("runTimestampUtc") or ""),
        "X-HRRR-Valid-ISO": str(meta.get("validTimestampUtc") or ""),
        "X-HRRR-Variable": str(meta.get("variable") or ""),
        "X-HRRR-Units": str(meta.get("units") or ""),
        "X-HRRR-Value-Name": str(meta.get("valueName") or ""),
        "X-HRRR-Forecast-Hour": str(meta.get("forecastHour") if meta.get("forecastHour") is not None else ""),
    }


def _get_model_processed(model, variable, forecast_hour, target_date=None, target_run_hour=None, bounds=None):
    model_name = _normalize_model_name(model)

    if xr is None:
        raise RuntimeError("HRRR support requires xarray and cfgrib. Install them in your Python environment.")

    if forecast_hour < 0 or forecast_hour > HRRR_MAX_FORECAST_HOUR:
        raise ValueError(f"forecast_hour must be between 0 and {HRRR_MAX_FORECAST_HOUR}.")

    if model_name == "mrms":
        if variable not in ("refc", "refc_ptype"):
            raise ValueError("MRMS currently supports reflectivity only (variable=refc).")
        if forecast_hour != 0:
            raise ValueError("MRMS supports forecast_hour=0 only.")

        latest_obj = _get_latest_mrms_object()
        key = latest_obj['key']
        key_basename = os.path.basename(key)
        key_no_gz = key_basename[:-3] if key_basename.endswith('.gz') else key_basename

        bounds_key = bounds if bounds else "full"
        cache_key = _make_hrrr_cache_key([
            model_name,
            key,
            variable,
            bounds_key,
        ])

        with hrrr_processed_cache_lock:
            cached = hrrr_processed_cache.get(cache_key)
            if cached is not None:
                hrrr_processed_cache.move_to_end(cache_key)
                vertices, values, meta = cached
                _log_hrrr(
                    f"Processed cache hit model=mrms key={key_basename} variable={variable} vertices={int(meta.get('vertexCount') or len(values))}"
                )
                return vertices, values, meta, True

        mrms_dir = Path("./temp/mrms")
        mrms_dir.mkdir(parents=True, exist_ok=True)
        gz_path = mrms_dir / key_basename
        grib2_path = mrms_dir / key_no_gz

        if not gz_path.exists() or gz_path.stat().st_size == 0:
            _log_hrrr(f"Downloading MRMS gzip file {key_basename}")
            _download_mrms_gz(latest_obj['url'], gz_path)
        else:
            _log_hrrr(f"Reusing cached MRMS gzip {gz_path.name}")

        _ensure_mrms_grib2_from_gz(gz_path, grib2_path)

        lon, lat, values_grid, data_var_name, units = _extract_hrrr_grids(str(grib2_path))
        values_grid, display_units = _normalize_hrrr_units("refc", values_grid, units)
        lon, lat, values_grid = _crop_hrrr_to_bounds(lon, lat, values_grid, bounds)

        vertices, values, effective_stride = _grid_to_triangle_buffers(
            lon,
            lat,
            values_grid,
            min_value=HRRR_REFC_MIN_DBZ,
        )

        run_timestamp = latest_obj['last_modified']
        if run_timestamp.tzinfo is None:
            run_timestamp = run_timestamp.replace(tzinfo=timezone.utc)

        meta = {
            "runDate": run_timestamp.strftime("%Y%m%d"),
            "runHour": int(run_timestamp.hour),
            "forecastHour": 0,
            "variable": "refc" if variable == "refc_ptype" else variable,
            "file": key_basename,
            "indexLine": key,
            "valueName": data_var_name,
            "units": display_units or "dBZ",
            "vertexCount": int(values.size),
            "effectiveStride": int(effective_stride),
            "runTimestampUtc": run_timestamp.isoformat().replace("+00:00", "Z"),
            "validTimestampUtc": run_timestamp.isoformat().replace("+00:00", "Z"),
        }

        with hrrr_processed_cache_lock:
            hrrr_processed_cache[cache_key] = (vertices, values, meta)
            hrrr_processed_cache.move_to_end(cache_key)
            while len(hrrr_processed_cache) > HRRR_PROCESSED_CACHE_MAX_SIZE:
                hrrr_processed_cache.popitem(last=False)

        with model_last_successful_runs_lock:
            model_last_successful_runs[model_name] = {
                "date": meta["runDate"],
                "hour": int(meta["runHour"]),
                "time": time.monotonic(),
            }

        _log_hrrr(
            f"Processed model=mrms key={key_basename} variable={variable} vertices={int(meta.get('vertexCount') or len(values))}"
        )

        return vertices, values, meta, False

    candidates = _build_model_run_candidates(target_date=target_date, target_run_hour=target_run_hour)
    _log_hrrr(
        f"Process request model={model_name} variable={variable} fh={forecast_hour} target_date={target_date or 'latest'} "
        f"target_run_hour={target_run_hour if target_run_hour is not None else 'latest'} bounds={'yes' if bounds else 'no'}"
    )
    with model_last_successful_runs_lock:
        last_run = model_last_successful_runs.get(model_name)
    if last_run:
        fallback_pair = (str(last_run.get("date")), int(last_run.get("hour")))
        if fallback_pair not in candidates:
            candidates.append(fallback_pair)
            _log_hrrr(f"Appended fallback candidate from last successful run: {fallback_pair[0]} {fallback_pair[1]:02d}z")

    _log_hrrr(f"Scanning {len(candidates)} candidate runs")

    selected_run = None
    selected_entry = None
    selected_entries = None
    selected_index_line = None
    selected_urls = None
    selected_inventory_error = None

    for candidate_date, candidate_hour in candidates:
        try:
            grib_url, idx_url, file_name = _build_model_file_urls(model_name, candidate_date, candidate_hour, forecast_hour)
            _log_hrrr(f"Try run {candidate_date} {int(candidate_hour):02d}z -> {file_name}")
            inventory = _get_hrrr_inventory(idx_url)
            if variable in ("ptype", "refc_ptype"):
                include_refc = variable == "refc_ptype"
                entries = _match_model_precip_entries(inventory, include_refc=include_refc)
                if entries is None:
                    _log_hrrr(f"Run {candidate_date} {int(candidate_hour):02d}z missing precip-type components for {variable}")
                    continue
                selected_entries = entries
                selected_entry = entries.get("refc") or entries.get("crain")
                selected_index_line = "; ".join(
                    f"{name}:{entries[name].get('line', '')}"
                    for name in entries.keys()
                )
            else:
                entry = _match_model_inventory_entry(inventory, variable)
                if entry is None:
                    _log_hrrr(f"Run {candidate_date} {int(candidate_hour):02d}z missing variable/level match for {variable}")
                    continue
                selected_entry = entry
                selected_entries = None
                selected_index_line = entry.get("line")

            selected_run = (candidate_date, candidate_hour)
            selected_urls = (grib_url, idx_url, file_name)
            _log_hrrr(
                f"Selected run {candidate_date} {int(candidate_hour):02d}z line='{(selected_index_line or '')[:120]}'"
            )
            break
        except Exception as err:
            selected_inventory_error = str(err)
            _log_hrrr(f"Run {candidate_date} {int(candidate_hour):02d}z scan error: {err}")
            continue

    if not selected_run or not selected_entry or not selected_urls:
        msg = "Unable to find requested HRRR field in recent runs."
        if selected_inventory_error:
            msg = f"{msg} Last error: {selected_inventory_error}"
        _log_hrrr(f"Scan failed for variable={variable} fh={forecast_hour}: {msg}")
        raise FileNotFoundError(msg)

    run_date, run_hour = selected_run
    grib_url, _, file_name = selected_urls

    bounds_key = bounds if bounds else "full"
    cache_key = _make_hrrr_cache_key([
        model_name,
        run_date,
        run_hour,
        forecast_hour,
        variable,
        bounds_key,
    ])

    with hrrr_processed_cache_lock:
        cached = hrrr_processed_cache.get(cache_key)
        if cached is not None:
            hrrr_processed_cache.move_to_end(cache_key)
            vertices, values, meta = cached
            _log_hrrr(
                f"Processed cache hit run={run_date} {int(run_hour):02d}z fh={forecast_hour} variable={variable} "
                f"vertices={int(meta.get('vertexCount') or len(values))}"
            )
            return vertices, values, meta, True

    hrrr_dir = Path(f"./temp/{model_name}")
    hrrr_dir.mkdir(parents=True, exist_ok=True)

    file_stem = f"{run_date}_t{run_hour:02d}_f{forecast_hour:02d}_{variable}_{cache_key[:10]}"
    partial_path = None
    if variable not in ("ptype", "refc_ptype"):
        partial_path = hrrr_dir / f"{file_stem}.grib2"

        if not partial_path.exists() or partial_path.stat().st_size == 0:
            try:
                _log_hrrr(f"Downloading byte-range message for {file_name}")
                _download_hrrr_message(grib_url, selected_entry["start"], selected_entry["end"], partial_path)
            except Exception:
                _log_hrrr(f"Byte-range download failed for {file_name}, falling back to full file download")
                _download_hrrr_full(grib_url, partial_path)
        else:
            _log_hrrr(f"Reusing cached GRIB file {partial_path.name}")

    if variable in ("ptype", "refc_ptype"):
        if not selected_entries:
            raise RuntimeError(f"Missing precip-type entries for variable '{variable}'.")

        loaded = {}
        for name, entry in selected_entries.items():
            component_path = hrrr_dir / f"{file_stem}_{name}.grib2"
            if not component_path.exists() or component_path.stat().st_size == 0:
                try:
                    _log_hrrr(f"Downloading byte-range message for {file_name} component={name}")
                    _download_hrrr_message(grib_url, entry["start"], entry["end"], component_path)
                except Exception:
                    _log_hrrr(f"Byte-range download failed for {file_name} component={name}, falling back to full file download")
                    _download_hrrr_full(grib_url, component_path)
            else:
                _log_hrrr(f"Reusing cached GRIB file {component_path.name}")

            lon_i, lat_i, values_i, var_name_i, units_i = _extract_hrrr_grids(str(component_path))
            loaded[name] = {
                "lon": lon_i,
                "lat": lat_i,
                "values": values_i.astype(np.float32),
                "name": var_name_i,
                "units": units_i,
            }

        base_name = "refc" if "refc" in loaded else "crain"
        lon = loaded[base_name]["lon"]
        lat = loaded[base_name]["lat"]
        base_shape = loaded[base_name]["values"].shape

        for name, payload in loaded.items():
            if payload["values"].shape != base_shape:
                raise RuntimeError(f"HRRR precip component shape mismatch for {name}: {payload['values'].shape} != {base_shape}")

        precip_type_grid = _build_precip_type_grid(
            loaded["crain"]["values"],
            loaded["cfrzr"]["values"],
            loaded["cicep"]["values"],
            loaded["csnow"]["values"],
        )

        if variable == "ptype":
            values_grid = precip_type_grid
            data_var_name = "ptype"
            display_units = "ptype"
            min_value = None
        else:
            refc_values, _ = _normalize_hrrr_units("refc", loaded["refc"]["values"], loaded["refc"]["units"])
            values_grid = _encode_reflectivity_by_precip_type(refc_values, precip_type_grid)
            data_var_name = "refc_ptype"
            display_units = "dBZ"
            min_value = None

        lon, lat, values_grid = _crop_hrrr_to_bounds(lon, lat, values_grid, bounds)
    else:
        if partial_path is None:
            raise RuntimeError("HRRR partial_path missing for non-precip variable processing.")
        lon, lat, values_grid, data_var_name, units = _extract_hrrr_grids(str(partial_path))
        values_grid, display_units = _normalize_hrrr_units(variable, values_grid, units)
        lon, lat, values_grid = _crop_hrrr_to_bounds(lon, lat, values_grid, bounds)
        min_value = HRRR_REFC_MIN_DBZ if variable == "refc" else None

    vertices, values, effective_stride = _grid_to_triangle_buffers(
        lon,
        lat,
        values_grid,
        min_value=min_value,
    )

    run_timestamp = datetime(
        int(run_date[0:4]),
        int(run_date[4:6]),
        int(run_date[6:8]),
        int(run_hour),
        0,
        0,
        tzinfo=timezone.utc,
    )
    valid_timestamp = run_timestamp + timedelta(hours=forecast_hour)

    meta = {
        "runDate": run_date,
        "runHour": run_hour,
        "forecastHour": forecast_hour,
        "variable": variable,
        "file": file_name,
        "indexLine": selected_index_line,
        "valueName": data_var_name,
        "units": display_units,
        "vertexCount": int(values.size),
        "effectiveStride": int(effective_stride),
        "runTimestampUtc": run_timestamp.isoformat().replace("+00:00", "Z"),
        "validTimestampUtc": valid_timestamp.isoformat().replace("+00:00", "Z"),
    }

    with hrrr_processed_cache_lock:
        hrrr_processed_cache[cache_key] = (vertices, values, meta)
        hrrr_processed_cache.move_to_end(cache_key)
        while len(hrrr_processed_cache) > HRRR_PROCESSED_CACHE_MAX_SIZE:
            hrrr_processed_cache.popitem(last=False)

    with model_last_successful_runs_lock:
        model_last_successful_runs[model_name] = {
            "date": run_date,
            "hour": int(run_hour),
            "time": time.monotonic(),
        }

    _log_hrrr(
        f"Processed model={model_name} run={run_date} {int(run_hour):02d}z fh={forecast_hour} variable={variable} "
        f"vertices={int(meta.get('vertexCount') or len(values))} stride={int(meta.get('effectiveStride') or 1)}"
    )

    return vertices, values, meta, False


@app.route('/api/hrrr-webgl', methods=['GET'])
def get_hrrr_data_webgl():
    format_type = request.args.get('format', 'binary').lower()
    try:
        model = _normalize_model_name(request.args.get('model', 'hrrr'))
    except ValueError as err:
        return jsonify({"error": str(err)}), 400
    variable = (request.args.get('variable', 'tmp2m') or 'tmp2m').strip().lower()

    try:
        forecast_hour = int(request.args.get('forecast_hour', '0'))
    except ValueError:
        return jsonify({"error": "forecast_hour must be an integer."}), 400

    if forecast_hour < 0 or forecast_hour > HRRR_MAX_FORECAST_HOUR:
        return jsonify({"error": f"forecast_hour must be between 0 and {HRRR_MAX_FORECAST_HOUR}."}), 400

    date_param = request.args.get('date')
    run_hour_param = request.args.get('run_hour')
    stride_param = request.args.get('stride', '1')

    try:
        target_date = _normalize_hrrr_date(date_param) if date_param else None
        _ = max(1, int(stride_param))
    except ValueError as err:
        return jsonify({"error": str(err)}), 400

    target_run_hour = None
    if run_hour_param not in (None, "", "latest"):
        try:
            target_run_hour = int(run_hour_param)
        except ValueError:
            return jsonify({"error": "run_hour must be 00-23 or 'latest'."}), 400
        if target_run_hour < 0 or target_run_hour > 23:
            return jsonify({"error": "run_hour must be between 0 and 23."}), 400

    bounds = None
    if all(request.args.get(k) is not None for k in ("minLon", "minLat", "maxLon", "maxLat")):
        try:
            min_lon = float(request.args.get("minLon"))
            min_lat = float(request.args.get("minLat"))
            max_lon = float(request.args.get("maxLon"))
            max_lat = float(request.args.get("maxLat"))
            bounds = (min_lon, min_lat, max_lon, max_lat)
        except ValueError:
            return jsonify({"error": "Invalid map bounds values."}), 400

    _log_hrrr(
        f"/api/hrrr-webgl request model={model} variable={variable} fh={forecast_hour} format={format_type} "
        f"date={target_date or 'latest'} run_hour={target_run_hour if target_run_hour is not None else 'latest'} "
        f"bounds={'yes' if bounds else 'no'}"
    )

    try:
        vertices, values, meta, from_cache = _get_model_processed(
            model=model,
            variable=variable,
            forecast_hour=forecast_hour,
            target_date=target_date,
            target_run_hour=target_run_hour,
            bounds=bounds,
        )
        _log_hrrr(
            f"/api/hrrr-webgl success model={model} variable={variable} fh={forecast_hour} "
            f"run={meta.get('runDate')} {int(meta.get('runHour') or 0):02d}z from_cache={from_cache}"
        )
    except ValueError as err:
        _log_hrrr(f"/api/hrrr-webgl validation error: {err}")
        return jsonify({"error": str(err)}), 400
    except FileNotFoundError as err:
        _log_hrrr(f"/api/hrrr-webgl not found: {err}")
        return jsonify({"error": str(err)}), 404
    except Exception as err:
        _log_hrrr(f"/api/hrrr-webgl failure: {err}")
        print(traceback.format_exc())
        return jsonify({"error": f"Failed to process HRRR data: {err}"}), 500

    if format_type == 'binary':
        return create_binary_response(vertices, values, _build_hrrr_binary_headers(meta))

    return jsonify({
        "vertices": vertices.tolist(),
        "values": values.tolist(),
        "meta": meta,
    })


@app.route('/api/hrrr-precache', methods=['POST'])
def precache_hrrr_range():
    payload = request.get_json(silent=True) or {}
    try:
        model = _normalize_model_name(payload.get('model', 'hrrr'))
    except ValueError as err:
        return jsonify({"error": str(err)}), 400
    variable = (payload.get('variable', 'tmp2m') or 'tmp2m').strip().lower()

    try:
        start_hour = int(payload.get('start_hour', 0))
        end_hour = int(payload.get('end_hour', HRRR_MAX_FORECAST_HOUR))
    except (TypeError, ValueError):
        return jsonify({"error": "start_hour and end_hour must be integers."}), 400

    start_hour = max(0, min(HRRR_MAX_FORECAST_HOUR, start_hour))
    end_hour = max(0, min(HRRR_MAX_FORECAST_HOUR, end_hour))
    if end_hour < start_hour:
        start_hour, end_hour = end_hour, start_hour

    date_param = payload.get('date')
    run_hour_param = payload.get('run_hour')

    try:
        target_date = _normalize_hrrr_date(date_param) if date_param else None
    except ValueError as err:
        return jsonify({"error": str(err)}), 400

    target_run_hour = None
    if run_hour_param not in (None, "", "latest"):
        try:
            target_run_hour = int(run_hour_param)
        except ValueError:
            return jsonify({"error": "run_hour must be 00-23 or 'latest'."}), 400
        if target_run_hour < 0 or target_run_hour > 23:
            return jsonify({"error": "run_hour must be between 0 and 23."}), 400

    bounds = None
    bounds_payload = payload.get('bounds')
    if isinstance(bounds_payload, dict):
        try:
            min_lon = float(bounds_payload.get('minLon'))
            min_lat = float(bounds_payload.get('minLat'))
            max_lon = float(bounds_payload.get('maxLon'))
            max_lat = float(bounds_payload.get('maxLat'))
            bounds = (min_lon, min_lat, max_lon, max_lat)
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid bounds payload."}), 400

    started = time.time()
    results = []

    for hour in range(start_hour, end_hour + 1):
        try:
            _, _, meta, from_cache = _get_model_processed(
                model=model,
                variable=variable,
                forecast_hour=hour,
                target_date=target_date,
                target_run_hour=target_run_hour,
                bounds=bounds,
            )
            results.append({
                "forecastHour": hour,
                "status": "cached" if from_cache else "fetched",
                "runDate": meta.get("runDate"),
                "runHour": meta.get("runHour"),
                "validTimestampUtc": meta.get("validTimestampUtc"),
                "vertexCount": meta.get("vertexCount"),
            })
        except Exception as err:
            results.append({
                "forecastHour": hour,
                "status": "error",
                "error": str(err),
            })

    cached_count = len([r for r in results if r.get("status") == "cached"])
    fetched_count = len([r for r in results if r.get("status") == "fetched"])
    error_count = len([r for r in results if r.get("status") == "error"])

    return jsonify({
        "model": model,
        "variable": variable,
        "startHour": start_hour,
        "endHour": end_hour,
        "totalHours": len(results),
        "cachedCount": cached_count,
        "fetchedCount": fetched_count,
        "errorCount": error_count,
        "elapsedMs": int((time.time() - started) * 1000),
        "results": results,
    })


@app.route('/api/hrrr-runs', methods=['GET'])
def get_hrrr_runs():
    try:
        model = _normalize_model_name(request.args.get('model', 'hrrr'))
    except ValueError as err:
        return jsonify({"error": str(err)}), 400
    variable = (request.args.get('variable', 'tmp2m') or 'tmp2m').strip().lower()
    lookback_param = request.args.get('lookback', str(HRRR_DEFAULT_LOOKBACK_HOURS))
    max_runs_param = request.args.get('max_runs', str(HRRR_DEFAULT_RUNS_MAX))

    if model == 'mrms':
        try:
            latest_obj = _get_latest_mrms_object()
            run_ts = latest_obj['last_modified']
            if run_ts.tzinfo is None:
                run_ts = run_ts.replace(tzinfo=timezone.utc)
            run_date = run_ts.strftime('%Y%m%d')
            run_hour = int(run_ts.hour)

            with model_last_successful_runs_lock:
                model_last_successful_runs['mrms'] = {
                    "date": run_date,
                    "hour": run_hour,
                    "time": time.monotonic(),
                }
                last_run = dict(model_last_successful_runs['mrms'])

            return jsonify({
                "model": model,
                "variable": variable,
                "forecastHour": 0,
                "lookbackHours": 0,
                "maxRuns": 1,
                "scannedCandidates": 1,
                "elapsedMs": 0,
                "runs": [{
                    "date": run_date,
                    "hour": run_hour,
                    "runTimestampUtc": run_ts.isoformat().replace('+00:00', 'Z'),
                    "isLastSuccessful": True,
                }],
                "lastSuccessfulRun": last_run,
                "latestFile": {
                    "key": latest_obj.get('key'),
                    "lastModifiedUtc": run_ts.isoformat().replace('+00:00', 'Z'),
                },
            })
        except Exception as err:
            return jsonify({"error": f"Failed to load MRMS latest run: {err}"}), 500

    try:
        forecast_hour = int(request.args.get('forecast_hour', '0'))
    except ValueError:
        return jsonify({"error": "forecast_hour must be an integer."}), 400

    if forecast_hour < 0 or forecast_hour > HRRR_MAX_FORECAST_HOUR:
        return jsonify({"error": f"forecast_hour must be between 0 and {HRRR_MAX_FORECAST_HOUR}."}), 400

    try:
        lookback_hours = max(6, min(96, int(lookback_param)))
    except ValueError:
        return jsonify({"error": "lookback must be an integer between 6 and 96."}), 400

    try:
        max_runs = max(1, min(HRRR_RUNS_MAX_LIMIT, int(max_runs_param)))
    except ValueError:
        return jsonify({"error": f"max_runs must be an integer between 1 and {HRRR_RUNS_MAX_LIMIT}."}), 400

    candidates = _build_model_run_candidates(lookback_hours=lookback_hours)
    runs = []
    scanned = 0
    started = time.time()

    _log_hrrr(
        f"/api/hrrr-runs request model={model} variable={variable} fh={forecast_hour} lookback={lookback_hours} max_runs={max_runs} candidates={len(candidates)}"
    )

    with model_last_successful_runs_lock:
        model_last_run = model_last_successful_runs.get(model)
        last_run = dict(model_last_run) if model_last_run else None

    for candidate_date, candidate_hour in candidates:
        if len(runs) >= max_runs:
            break

        scanned += 1
        try:
            _, idx_url, _ = _build_model_file_urls(model, candidate_date, candidate_hour, forecast_hour)
            inventory = _get_hrrr_inventory(idx_url)
            if variable in ("ptype", "refc_ptype"):
                include_refc = variable == "refc_ptype"
                entries = _match_model_precip_entries(inventory, include_refc=include_refc)
                if entries is None:
                    _log_hrrr(f"/api/hrrr-runs skip {candidate_date} {int(candidate_hour):02d}z (missing precip-type components)")
                    continue
            else:
                entry = _match_model_inventory_entry(inventory, variable)
                if entry is None:
                    _log_hrrr(f"/api/hrrr-runs skip {candidate_date} {int(candidate_hour):02d}z (no variable match)")
                    continue

            run_ts = datetime(
                int(candidate_date[0:4]),
                int(candidate_date[4:6]),
                int(candidate_date[6:8]),
                int(candidate_hour),
                0,
                0,
                tzinfo=timezone.utc,
            )
            runs.append({
                "date": candidate_date,
                "hour": int(candidate_hour),
                "runTimestampUtc": run_ts.isoformat().replace("+00:00", "Z"),
                "isLastSuccessful": bool(
                    last_run and str(last_run.get("date")) == candidate_date and int(last_run.get("hour")) == int(candidate_hour)
                ),
            })
            _log_hrrr(f"/api/hrrr-runs add {candidate_date} {int(candidate_hour):02d}z")
        except Exception:
            _log_hrrr(f"/api/hrrr-runs scan error {candidate_date} {int(candidate_hour):02d}z")
            continue

    elapsed_ms = int((time.time() - started) * 1000)
    _log_hrrr(
        f"/api/hrrr-runs response runs={len(runs)} scanned={scanned} elapsed_ms={elapsed_ms} last_success={last_run}"
    )

    return jsonify({
        "model": model,
        "variable": variable,
        "forecastHour": forecast_hour,
        "lookbackHours": lookback_hours,
        "maxRuns": max_runs,
        "scannedCandidates": scanned,
        "elapsedMs": elapsed_ms,
        "runs": runs,
        "lastSuccessfulRun": last_run,
    })


@app.route('/api/cameras', methods=['GET'])
def get_cameras():
    """
    API endpoint to get all traffic cameras from all state folders.
    Returns a merged GeoJSON FeatureCollection with all cameras.
    
    Expected GeoJSON format in cameras/{state}/*.geojson files:
    {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [longitude, latitude]
                },
                "properties": {
                    "name": "Camera Name (optional)",
                    "image_url": "URL to static image (optional)",
                    "video_url": "URL to video stream (optional)"
                }
            }
        ]
    }
    
    Supported media formats:
    - Images: .jpg, .jpeg, .png, .gif
    - Videos: .mp4, .webm, .ogg, .m3u8, .flv, .mov, .avi
    
    Note: Cameras can have both image_url and video_url for dual-format support.
    """
    try:
        cameras_dir = Path(__file__).parent / "cameras"
        
        if not cameras_dir.exists():
            return jsonify({
                "type": "FeatureCollection",
                "features": []
            })
        
        all_features = []
        states_found = []
        
        # Iterate through all state folders
        for state_dir in cameras_dir.iterdir():
            if not state_dir.is_dir():
                continue
            
            state_name = state_dir.name
            
            # Look for GeoJSON files in each state folder
            for geojson_file in state_dir.glob("*.geojson"):
                try:
                    with open(geojson_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # Handle both FeatureCollection and single Feature
                    if data.get("type") == "FeatureCollection":
                        features = data.get("features", [])
                    elif data.get("type") == "Feature":
                        features = [data]
                    else:
                        continue
                    
                    # Add state name to each feature's properties
                    for feature in features:
                        if "properties" not in feature:
                            feature["properties"] = {}
                        feature["properties"]["state"] = state_name
                        all_features.append(feature)
                    
                    states_found.append(state_name)
                    
                except Exception as e:
                    print(f"⚠️  Error loading camera file {geojson_file}: {e}")
                    continue
        
        print(f"📷 Loaded {len(all_features)} cameras from {len(set(states_found))} state(s)")
        
        return jsonify({
            "type": "FeatureCollection",
            "features": all_features
        })
        
    except Exception as e:
        print(f"❌ Error in /api/cameras: {e}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    print("=" * 60)
    print("🌦️  RadarApp Flask Backend Starting...")
    print("=" * 60)
    print(f"📡 Level 2 Update Mode: On-demand request checks")
    print(f"🌐 Server: http://localhost:5100")
    print("=" * 60)
    app.run(debug=True, port=5100, threaded=True)
    
#conda activate radar_api_env
#python app.py

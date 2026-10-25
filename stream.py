"""
모델 파일을 받으면서 바로 R2에 올린다 (backup.sh 가 부른다). 디스크를 거치지 않는다.

- 파일 하나를 PART 크기 구간으로 나눠 여러 연결로 동시에 받고(HTTP Range), 받은 구간은 바로 R2 나눠 올리기(multipart)의 한 조각으로 올린다.
- sha256은 구간을 순서대로 이어 붙이며 잰다. 크기·sha256이 맞으면 올리기를 끝내고, 틀리면 올린 조각을 버린다.
- 서버가 Range를 받지 않으면 처음부터 끝까지 한 연결로 받으며 조각을 올린다.
- 파일 여러 개를 동시에 (FILES개).
- Civitai 주소에만 grill의 키를 붙인다. 넘겨주는 주소(저장소 서명 주소)로는 키를 보내지 않는다.

입력: 표준 입력으로 [{sha256, size, url, file}, ...] (로비 /backups/work 의 pending)
환경변수: LOBBY_URL, BACKUP_SECRET, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET, R2_ACCOUNT_ID 또는 R2_ENDPOINT, CIVITAI_API_KEY(선택)
출력: 마지막 줄에 "ok <성공 수> failed <실패 수>"
"""

from __future__ import annotations

import concurrent.futures as cf
import hashlib
import http.client
import json
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

import boto3
from botocore.config import Config

PART = int(os.environ.get("BACKUP_PART_BYTES", 64 * 1024 * 1024))
# 파일 하나를 받는 동시 연결 수, 앞서 받아 둘 수 있는 구간 수 (메모리: FILES × AHEAD × PART)
CONNS = int(os.environ.get("BACKUP_CONNS", 8))
AHEAD = int(os.environ.get("BACKUP_AHEAD", 12))
FILES = int(os.environ.get("BACKUP_FILES", 3))
UA = "grill-backup"
# 시험용: http 주소 허용
ALLOW_HTTP = os.environ.get("BACKUP_ALLOW_HTTP") == "1"
CIVITAI = re.compile(r"(^|\.)civitai\.(com|red|green)$")

LOBBY = os.environ["LOBBY_URL"].rstrip("/") + "/api"
SECRET = os.environ["BACKUP_SECRET"]
BUCKET = os.environ["R2_BUCKET"]
ENDPOINT = os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
    region_name="auto",
    # R2는 최신 SDK가 기본으로 붙이는 체크섬 일부를 받지 않는다
    config=Config(request_checksum_calculation="when_required", response_checksum_validation="when_required", max_pool_connections=FILES * CONNS + 4, retries={"max_attempts": 5}),
)
log_lock = threading.Lock()


def log(msg: str) -> None:
    with log_lock:
        print(msg, flush=True)


class NoRange(Exception):
    """처음 확인(bytes=0-0)에는 구간으로 답했는데 실제 구간 요청에는 파일 전체(200)로 답하는 곳: 한 연결로 다시 받는다"""


class Fail(Exception):
    pass


def report(body: dict) -> None:
    req = urllib.request.Request(f"{LOBBY}/backups/report", data=json.dumps(body).encode(), method="POST",
                                 headers={"Authorization": f"Bearer {SECRET}", "Content-Type": "application/json"})
    for i in range(3):
        try:
            urllib.request.urlopen(req, timeout=30).read()
            return
        except (urllib.error.URLError, OSError):
            time.sleep(2 * (i + 1))


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **k):
        return None


no_redirect = urllib.request.build_opener(NoRedirect)


def headers_for(url: str, extra: dict | None = None) -> dict:
    h = {"User-Agent": UA, **(extra or {})}
    host = (urllib.parse.urlparse(url).hostname or "").lower()
    key = os.environ.get("CIVITAI_API_KEY", "").strip()
    if key and CIVITAI.search(host):
        h["Authorization"] = f"Bearer {key}"
    return h


def resolve(url: str) -> tuple[str, bool]:
    """넘겨주는 주소를 따라가 실제로 받을 주소와 Range를 받는지 (키는 Civitai 주소에만)"""
    for _ in range(8):
        req = urllib.request.Request(url, headers=headers_for(url, {"Range": "bytes=0-0"}))
        try:
            with no_redirect.open(req, timeout=60) as r:
                r.read(1)
                return url, r.status == 206
        except urllib.error.HTTPError as e:
            if e.code in (301, 302, 303, 307, 308) and e.headers.get("Location"):
                url = urllib.parse.urljoin(url, e.headers["Location"])
                continue
            if e.code in (401, 403):
                raise Fail(f"로그인이 필요합니다 ({e.code})") from None
            raise Fail(f"받지 못했습니다 ({e.code})") from None
        except (urllib.error.URLError, OSError, http.client.HTTPException) as e:
            raise Fail(f"받지 못했습니다 ({e})") from None
    raise Fail("넘겨주기가 너무 많습니다")


class Source:
    """원래 주소와 넘겨받은 실제 주소. 서명 주소가 만료되면(401·403·410) 다시 넘겨받는다"""

    def __init__(self, url: str):
        self.url = url
        self.lock = threading.Lock()
        self.real, self.ranged = resolve(url)

    def refresh(self, old: str) -> None:
        with self.lock:
            if self.real == old:
                self.real, self.ranged = resolve(self.url)


def fetch_range(src: Source, start: int, end: int) -> bytes:
    want = end - start + 1
    err = ""
    for i in range(6):
        url = src.real
        try:
            req = urllib.request.Request(url, headers=headers_for(url, {"Range": f"bytes={start}-{end}"}))
            with urllib.request.urlopen(req, timeout=120) as r:
                if r.status == 206:
                    data = r.read()
                elif start == 0:
                    # 구간을 무시하고 전체를 주는 곳: 첫 구간이면 앞부분만 쓴다 (파일이 구간 하나면 그대로 끝난다)
                    data = r.read(want)
                else:
                    raise NoRange()
            if len(data) == want:
                return data
            err = f"구간 크기가 다릅니다 ({len(data)}/{want})"
        except urllib.error.HTTPError as e:
            err = f"{e.code}"
            if e.code in (401, 403, 410):
                src.refresh(url)
        except (urllib.error.URLError, OSError, http.client.HTTPException) as e:
            err = str(e)
        time.sleep(min(30, 2 ** i))
    raise Fail(f"받지 못했습니다 ({err})")


def in_r2(sha: str, size: int) -> bool:
    try:
        return s3.head_object(Bucket=BUCKET, Key=f"models/{sha}")["ContentLength"] == size
    except Exception:  # noqa: BLE001 (없으면 404)
        return False


def send(src: Source, key: str, sha: str, size: int, pool: cf.ThreadPoolExecutor, ranged: bool) -> None:
    """받으면서 R2 나눠 올리기로 올린다. 실패하면 올린 조각을 버린다 (NoRange면 호출한 쪽이 한 연결로 다시)"""
    up = s3.create_multipart_upload(Bucket=BUCKET, Key=key)["UploadId"]
    h = hashlib.sha256()
    parts: list[dict] = []
    got = 0
    try:
        def put(n: int, data: bytes) -> dict:
            r = s3.upload_part(Bucket=BUCKET, Key=key, UploadId=up, PartNumber=n, Body=data)
            return {"PartNumber": n, "ETag": r["ETag"]}

        if ranged:
            count = (size + PART - 1) // PART

            def one(i: int) -> tuple[bytes, dict]:
                start = i * PART
                data = fetch_range(src, start, min(size, start + PART) - 1)
                return data, put(i + 1, data)

            futs: dict[int, cf.Future] = {}
            nxt = 0
            try:
                for i in range(count):
                    # 앞서 받는 구간 수를 AHEAD 개로 묶는다 (메모리)
                    while nxt < count and nxt < i + AHEAD:
                        futs[nxt] = pool.submit(one, nxt)
                        nxt += 1
                    data, part = futs.pop(i).result()
                    h.update(data)
                    got += len(data)
                    parts.append(part)
            finally:
                # 실패하면 아직 시작하지 않은 구간은 받지 않는다
                for f in futs.values():
                    f.cancel()
        else:
            # Range를 받지 않는 서버: 한 연결로 받으며 조각을 올린다
            req = urllib.request.Request(src.real, headers=headers_for(src.real))
            with urllib.request.urlopen(req, timeout=120) as r:
                n = 0
                while True:
                    data = r.read(PART)
                    if not data:
                        break
                    got += len(data)
                    if got > size:
                        raise Fail(f"크기가 다릅니다 ({got} > {size})")
                    h.update(data)
                    n += 1
                    parts.append(put(n, data))
        if got != size:
            raise Fail(f"크기가 다릅니다 ({got})")
        if h.hexdigest() != sha:
            raise Fail("sha256이 다릅니다")
        s3.complete_multipart_upload(Bucket=BUCKET, Key=key, UploadId=up, MultipartUpload={"Parts": parts})
    except BaseException:
        try:
            s3.abort_multipart_upload(Bucket=BUCKET, Key=key, UploadId=up)
        except Exception:  # noqa: BLE001
            pass
        raise


def backup(job: dict, pool: cf.ThreadPoolExecutor) -> None:
    sha, size, url = job["sha256"], int(job["size"]), job["url"]
    key = f"models/{sha}"
    if not url.startswith("https://") and not (ALLOW_HTTP and url.startswith("http://")):
        raise Fail("https 주소가 아닙니다")
    if in_r2(sha, size):
        log(f"이미 있음 {sha[:8]}")
        return
    src = Source(url)
    try:
        send(src, key, sha, size, pool, src.ranged and size > 0)
    except NoRange:
        log(f"구간 받기를 받지 않는 곳이라 한 연결로 다시 받습니다: {sha[:8]}")
        send(src, key, sha, size, pool, False)
    log(f"백업 {sha[:8]} ({size} bytes)")


def main() -> int:
    jobs = [j for j in json.load(sys.stdin) if re.fullmatch(r"[0-9a-f]{64}", str(j.get("sha256", ""))) and str(j.get("size", "")).isdigit()]
    ok = failed = 0
    # 구간 받기·올리기는 모든 파일이 함께 쓰는 연결 묶음으로
    with cf.ThreadPoolExecutor(FILES * CONNS) as pool, cf.ThreadPoolExecutor(FILES) as files:
        def run(job: dict) -> bool:
            t0 = time.time()
            try:
                backup(job, pool)
            except Fail as e:
                log(f"실패 {job['sha256'][:8]}")  # 이유(주소가 들어갈 수 있다)는 로비에만 보낸다
                report({"sha256": job["sha256"], "state": "failed", "size": int(job["size"]), "error": str(e)[:400]})
                return False
            except Exception as e:  # noqa: BLE001 (R2 등)
                log(f"실패 {job['sha256'][:8]} (R2)")
                report({"sha256": job["sha256"], "state": "failed", "size": int(job["size"]), "error": f"R2에 올리지 못했습니다: {str(e)[:300]}"})
                return False
            # 알리기에 실패해도 다음 작업이 저장소에서 찾아 다시 알린다
            report({"sha256": job["sha256"], "state": "done", "size": int(job["size"])})
            secs = max(0.001, time.time() - t0)
            log(f"  {job['sha256'][:8]}: {int(job['size']) / secs / 1e6:.0f} MB/s")
            return True

        for res in files.map(run, jobs):
            ok, failed = ok + res, failed + (not res)
    print(f"ok {ok} failed {failed}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())

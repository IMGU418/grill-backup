"""받으면서 바로 올리기 (stream.py): 가짜 원본 서버와 가짜 R2로"""

import concurrent.futures as cf
import hashlib
import os
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

os.environ.update({
    "LOBBY_URL": "http://127.0.0.1:9", "BACKUP_SECRET": "s", "R2_ACCESS_KEY_ID": "k", "R2_SECRET_ACCESS_KEY": "k",
    "R2_BUCKET": "b", "R2_ENDPOINT": "http://127.0.0.1:9", "BACKUP_PART_BYTES": "1000", "BACKUP_AHEAD": "3",
    "BACKUP_ALLOW_HTTP": "1", "CIVITAI_API_KEY": "key",
})
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import stream  # noqa: E402

DATA = bytes(range(256)) * 41  # 10,496 bytes → 11 구간
SHA = hashlib.sha256(DATA).hexdigest()


class FakeS3:
    def __init__(self):
        self.parts: dict[int, bytes] = {}
        self.done: dict[str, bytes] = {}
        self.aborted = 0

    def head_object(self, Bucket, Key):
        if Key in self.done:
            return {"ContentLength": len(self.done[Key])}
        raise KeyError(Key)

    def create_multipart_upload(self, Bucket, Key):
        self.parts = {}
        return {"UploadId": "u1"}

    def upload_part(self, Bucket, Key, UploadId, PartNumber, Body):
        self.parts[PartNumber] = Body
        return {"ETag": f"e{PartNumber}"}

    def complete_multipart_upload(self, Bucket, Key, UploadId, MultipartUpload):
        nums = [p["PartNumber"] for p in MultipartUpload["Parts"]]
        assert nums == sorted(nums) == list(range(1, len(nums) + 1)), nums
        self.done[Key] = b"".join(self.parts[n] for n in nums)

    def abort_multipart_upload(self, Bucket, Key, UploadId):
        self.aborted += 1


class Handler(BaseHTTPRequestHandler):
    seen_auth_on_blob = []
    ranges = []

    def log_message(self, *a):
        pass

    def do_GET(self):
        port = self.server.server_address[1]
        if self.path == "/dl":
            # Civitai 같은 곳: 키가 있어야 넘겨준다 (다른 호스트의 서명 주소로)
            if self.headers.get("Authorization") != "Bearer key":
                self.send_response(401); self.end_headers(); return
            self.send_response(302); self.send_header("Location", f"http://localhost:{port}/blob"); self.end_headers(); return
        if self.path in ("/blob", "/norange", "/bad", "/probeonly"):
            Handler.seen_auth_on_blob.append(self.headers.get("Authorization"))
            body = DATA if self.path != "/bad" else DATA[:-1] + b"x"
            rng = self.headers.get("Range")
            # /probeonly: 처음 확인(bytes=0-0)에만 구간으로 답하고 실제 구간 요청에는 전체(200)로 답하는 곳
            if rng and self.path != "/norange" and not (self.path == "/probeonly" and rng != "bytes=0-0"):
                a, b = rng.split("=")[1].split("-")
                a, b = int(a), min(int(b), len(body) - 1)
                Handler.ranges.append((a, b))
                self.send_response(206); self.send_header("Content-Length", str(b - a + 1)); self.end_headers()
                self.wfile.write(body[a:b + 1]); return
            self.send_response(200); self.send_header("Content-Length", str(len(body))); self.end_headers()
            self.wfile.write(body); return
        self.send_response(404); self.end_headers()


class StreamTest(unittest.TestCase):
    def setUp(self):
        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.base = f"http://127.0.0.1:{self.srv.server_address[1]}"
        self.s3 = stream.s3 = FakeS3()
        stream.CIVITAI = __import__("re").compile(r"^127\.0\.0\.1$")
        Handler.seen_auth_on_blob.clear()
        Handler.ranges.clear()

    def tearDown(self):
        self.srv.shutdown()

    def run_job(self, path, sha=SHA, size=len(DATA)):
        with cf.ThreadPoolExecutor(4) as pool:
            stream.backup({"sha256": sha, "size": size, "url": self.base + path, "file": "m.safetensors"}, pool)

    def test_ranges_streamed_to_multipart_without_forwarding_key(self):
        self.run_job("/dl")
        self.assertEqual(self.s3.done[f"models/{SHA}"], DATA)
        self.assertEqual(len(self.s3.parts), 11, "구간마다 조각 하나")
        self.assertGreaterEqual(len(Handler.ranges), 11)
        self.assertTrue(all(a is None for a in Handler.seen_auth_on_blob), "넘겨받은 주소로는 키를 보내지 않는다")

    def test_server_without_range(self):
        self.run_job("/norange")
        self.assertEqual(self.s3.done[f"models/{SHA}"], DATA)

    def test_ranges_answered_with_whole_file(self):
        """구간 요청에 전체(200)로 답하면 한 연결로 다시 받는다 ("구간 받기 실패 (200)"이 아니라)"""
        self.run_job("/probeonly")
        self.assertEqual(self.s3.done[f"models/{SHA}"], DATA)
        self.assertGreaterEqual(self.s3.aborted, 1, "구간으로 올리던 것은 버린다")

    def test_single_part_answered_with_whole_file(self):
        """파일이 구간 하나보다 작으면 전체(200) 응답을 그대로 쓴다"""
        small = DATA[:700]
        sha = hashlib.sha256(small).hexdigest()
        orig = DATA
        try:
            globals()["DATA"] = small
            self.run_job("/probeonly", sha=sha, size=len(small))
        finally:
            globals()["DATA"] = orig
        self.assertEqual(self.s3.done[f"models/{sha}"], small)
        self.assertEqual(self.s3.aborted, 0)

    def test_hash_mismatch_aborts(self):
        with self.assertRaises(stream.Fail):
            self.run_job("/bad")
        self.assertEqual(self.s3.done, {})
        self.assertEqual(self.s3.aborted, 1)

    def test_size_mismatch_aborts(self):
        with self.assertRaises(stream.Fail):
            self.run_job("/norange", size=len(DATA) - 5)
        self.assertEqual(self.s3.aborted, 1)

    def test_already_in_r2(self):
        self.s3.done[f"models/{SHA}"] = DATA
        self.run_job("/blob")
        self.assertEqual(Handler.ranges, [], "받지 않는다")

    def test_login_needed(self):
        os.environ["CIVITAI_API_KEY"] = ""
        try:
            with self.assertRaises(stream.Fail) as e:
                self.run_job("/dl")
            self.assertIn("로그인", str(e.exception))
        finally:
            os.environ["CIVITAI_API_KEY"] = "key"


if __name__ == "__main__":
    unittest.main()

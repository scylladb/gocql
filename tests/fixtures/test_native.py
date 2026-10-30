"""Focused harness checks; database behavior is proved by native.py receipts."""
import contextlib
import fcntl
import importlib.util
import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import unittest

spec = importlib.util.spec_from_file_location("native", Path(__file__).with_name("native.py"))
native = importlib.util.module_from_spec(spec)
spec.loader.exec_module(native)


class HarnessTest(unittest.TestCase):
    def test_protocol_health_reads_split_header_and_body(self):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            errors = []
            def server():
                try:
                    with listener.accept()[0] as conn:
                        self.assertEqual(native.exact(conn, 9), bytes.fromhex("040000000500000000"))
                        for byte in bytes.fromhex("8400000006000000020000"):
                            conn.sendall(bytes([byte]))
                except BaseException as exc:
                    errors.append(exc)
            worker = threading.Thread(target=server)
            worker.start()
            result = native.health("127.0.0.1", listener.getsockname()[1])
            worker.join()
            self.assertFalse(errors)
            self.assertEqual(result["header"], "840000000600000002")

    def test_hash_mismatch_precedes_provisioning(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "binary").write_bytes(b"wrong build")
            runner = native.Runner({}, root / "state", root / "receipt.json", root)
            with self.assertRaisesRegex(RuntimeError, "hash mismatch"):
                runner.install({"install": str(root), "binary": "binary", "sha256": "incorrect"})
            self.assertFalse((runner.fixture / "install").exists())

    def test_subprocess_status_and_logs_retained(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runner = native.Runner({}, root / "state", root / "receipt.json", root)
            with self.assertRaisesRegex(RuntimeError, "intentional failed"):
                runner.command("intentional", [sys.executable, "-c", "print('retained fault'); raise SystemExit(7)"])
            receipt = json.loads((root / "receipt.json").read_text())
            record = receipt["commands"][0]
            self.assertEqual(record["exit_code"], 7)
            self.assertEqual(Path(record["log"]).read_text(), "retained fault\n")
            self.assertEqual(record["sha256"], native.sha(Path(record["log"])))

    def test_same_address_lock_conflicts_across_processes(self):
        with tempfile.TemporaryDirectory() as directory:
            lock = Path(directory) / "127.0.20.1.lock"
            with lock.open("w") as held:
                fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
                code = "import fcntl,sys; f=open(sys.argv[1],'a+'); fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)"
                result = subprocess.run([sys.executable,"-c",code,str(lock)], capture_output=True)
                self.assertNotEqual(result.returncode, 0)
            result = subprocess.run([sys.executable,"-c",code,str(lock)], capture_output=True)
            self.assertEqual(result.returncode, 0)

class ProvisionTest(unittest.TestCase):
    def test_pinned_archive_is_new_and_confined(self):
        import hashlib
        import io
        import tarfile
        provision_spec = importlib.util.spec_from_file_location("provision", Path(__file__).with_name("provision.py"))
        module = importlib.util.module_from_spec(provision_spec)
        provision_spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root / "server.tar"
            with tarfile.open(archive, "w") as bundle:
                info = tarfile.TarInfo("server/version")
                info.size = 5
                bundle.addfile(info, io.BytesIO(b"5.0.9"))
            digest = hashlib.sha256(archive.read_bytes()).hexdigest()
            with self.assertRaisesRegex(ValueError, "SHA-256"):
                module.provision(archive, root / "install", "wrong")
            self.assertFalse((root / "install").exists())
            module.provision(archive, root / "install", digest)
            self.assertEqual((root / "install/server/version").read_text(), "5.0.9")
            with self.assertRaisesRegex(ValueError, "already exists"):
                module.provision(archive, root / "install", digest)
            with tarfile.open(archive, "w") as bundle:
                info = tarfile.TarInfo("../../outside")
                info.size = 1
                bundle.addfile(info, io.BytesIO(b"x"))
            digest = hashlib.sha256(archive.read_bytes()).hexdigest()
            with self.assertRaises(tarfile.FilterError):
                module.provision(archive, root / "malicious", digest)
            self.assertFalse((root / "malicious").exists())
            self.assertFalse((root.parent / "outside").exists())


if __name__ == "__main__":
    unittest.main()

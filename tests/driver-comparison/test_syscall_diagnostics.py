import importlib.util
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('syscall_diagnostics', Path(__file__).with_name('syscall_diagnostics.py'))
d = importlib.util.module_from_spec(spec)
spec.loader.exec_module(d)


class SyscallDiagnosticsTest(unittest.TestCase):
    def test_network_completions_include_drain_and_reassembled_calls(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'trace'
            path.write_text('''10 9.000000 read(6<TCP:[a->b]>, <unfinished ...>
11 9.900000 writev(7<TCP:[a->b]>, [], 1) = 128 <0.200000>
10 10.200000 <... read resumed>""..., 1024) = 1024 <1.200000>
10 10.300000 read(6<TCP:[a->b]>, "", 1024) = -1 EAGAIN <0.000001>
11 10.400000 write(8</proc/self/status>, "", 4) = 4 <0.000001>
11 10.500000 writev(7<TCP:[a->b]>, <unfinished ...>
11 11.500000 <... writev resumed>[], 1) = 512 <1.000000>
''')
            result = d.network_syscalls(path, dict(start='1970-01-01T00:00:10Z', drain_end='1970-01-01T00:00:11Z', completed=2))
            self.assertEqual(result['syscall_counts'], dict(writev=1, read=2))
            self.assertEqual(result['transferred_bytes'], dict(writev=128, read=1024))
            self.assertEqual(result['write_batch_bytes'], {128: 1})
            self.assertEqual(result['syscalls_per_op']['read'], 1)
            self.assertEqual(result['bytes_per_syscall']['read'], 512)

    def test_malformed_network_records_and_missing_activity_are_explicit(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'trace'
            window = dict(start='1970-01-01T00:00:10Z', drain_end='1970-01-01T00:00:11Z', completed=2)
            for text in ('', '10 10.500000 write(7<TCP:[a->b]>, []) = ???\n'):
                path.write_text(text)
                with self.assertRaises(ValueError):
                    d.network_syscalls(path, window)


if __name__ == '__main__':
    unittest.main()

"""Confined RPC to the runner's already locked CCM cluster, never CCM CURRENT."""
import contextlib
import json
import os
from pathlib import Path
import socketserver
import tempfile
import threading
import time


@contextlib.contextmanager
def node_control(cluster, record, health):
    nodes = {node.name: node for node in cluster.nodelist()}
    events = record.setdefault("node_control", [])

    class Handler(socketserver.StreamRequestHandler):
        def handle(self):
            self.connection.settimeout(120)
            event = {"started": time.time()}
            try:
                request = json.loads(self.rfile.readline(4096))
                command = request["command"]
                event["command"] = command
                if command == ["status", "-v"]:
                    output = "Cluster: " + cluster.name + "\n----------------\n"
                    for node in nodes.values():
                        output += node.name + (": UP\n" if node.is_running() else ": DOWN\n")
                        output += " binary=('" + node.network_interfaces['binary'][0] + "', 9042)\n\n"
                elif len(command) >= 2 and command[0] in nodes and command[1:] in (["stop"], ["start", "--wait-for-binary-proto"]):
                    node = nodes[command[0]]
                    event.update(node_path=node.get_path(), address=node.network_interfaces['binary'][0], pid_before=getattr(node, 'pid', None))
                    if command[1] == "stop":
                        node.stop(gently=True, wait=True)
                        if node.is_running():
                            raise RuntimeError("owned node stop did not join")
                        event["joined"] = True
                    else:
                        node.start(wait_for_binary_proto=True, wait_other_notice=True)
                        event["health"] = health(node.network_interfaces['binary'][0])
                    event["pid_after"] = getattr(node, 'pid', None)
                    output = ""
                else:
                    raise ValueError("unsupported owned-node command")
                response = {"output": output}
            except BaseException as exc:
                event["error"] = repr(exc)
                response = {"error": repr(exc)}
            finally:
                event["finished"] = time.time()
                events.append(event)
            self.wfile.write((json.dumps(response) + "\n").encode())

    with tempfile.TemporaryDirectory(prefix="gocql-fixture-") as directory:
        path = str(Path(directory) / "control.sock")
        with socketserver.UnixStreamServer(path, Handler) as server:
            os.chmod(path, 0o600)
            worker = threading.Thread(target=server.serve_forever, name="owned-node-control")
            worker.start()
            try:
                yield path
            finally:
                server.shutdown()
                worker.join()

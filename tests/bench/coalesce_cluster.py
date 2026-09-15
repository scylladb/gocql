#!/usr/bin/env python3
"""Create/destroy the scylla-ccm podman cluster used by the write-coalescing benchmark.

Topology (docs/coalescing-redesign.md E4): three tiers at increasing RTT from
the client container, which always sits on dc1/rack1's network.

    dc1/rack1  same-rack            no delay
    dc1/rack2  same-DC-other-rack   --inter-rack-delay
    dc2/rack1  cross-DC             --inter-dc-delay

The benchmark pins every request to one tier by giving scylla-bench a
rack-aware policy; that only pins reliably if the target node is a replica for
every token. run_coalesce_bench.sh therefore uses RF=2, which under
NetworkTopologyStrategy means "2 replicas per DC" -- so each DC needs at least
two token-owning nodes, and every node ends up a replica of every token.

Usage:
    coalesce_cluster.py create     [--image IMG] [--name NAME]
    coalesce_cluster.py info       [--name NAME]
    coalesce_cluster.py enable-tls [--name NAME]
    coalesce_cluster.py disable-tls [--name NAME]
    coalesce_cluster.py remove     [--name NAME]

Implementation note (read before changing the netem/client-container bits):
upstream scylla-ccm (ccmlib.scylla_docker_cluster.ScyllaDockerCluster) has no
concept of inter-rack/inter-dc latency injection, no client-container helper,
and no CPU pinning -- an earlier version of this script imported a
`ScyllaPodmanCluster` with all three built in, but that class does not exist
anywhere in scylladb/scylla-ccm or any fork. This version builds on the real
ScyllaDockerCluster and implements the two things this benchmark actually
needs directly here, via plain `podman exec`/`podman run`:
  * netem delay: `tc qdisc ... netem delay` inside each node's own container,
    keyed by which tier (TOPOLOGY) it's in.
  * client container: a plain container on the same podman network as the
    scylla nodes, running `sleep infinity` so it has nothing else to do.
    run_coalesce_bench.sh only nsenters into its *network namespace*
    (`nsenter --net`) and runs scylla-bench (a host binary) inside that netns
    -- see in_netns() there -- so the container never needs scylla-bench,
    gocql, or any CQL tooling inside it; a plain network stack is enough.
CPU pinning (`pinning=False` in the old code) is dropped entirely:
ScyllaDockerCluster has no cpuset/pinning support to disable in the first
place, so the kwarg was never meaningful.
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.expanduser("~/github/scylla-ccm"))

from ccmlib.scylla_docker_cluster import ScyllaDockerCluster  # noqa: E402
from ccmlib.cluster_factory import ClusterFactory  # noqa: E402

CCM_ROOT = os.path.expanduser("~/.ccm")
DEFAULT_NAME = "coalesce-bench"
DEFAULT_IMAGE = "docker.io/scylladb/scylla:2026.3"

# Matches the plan's E4: netem is per-direction egress, so RTT is 2x these.
INTER_RACK_DELAY_MS = 0.5
INTER_DC_DELAY_MS = 20

# NetworkTopologyStrategy with RF=2 puts one replica per rack where a DC has
# two racks (dc1), and two replicas on distinct nodes where it has one (dc2).
# Either way every token has a replica in every tier, so the rack-aware policy
# pins all traffic to the tier under test.
TOPOLOGY = {"dc1": {"rack1": 1, "rack2": 1}, "dc2": {"rack1": 2}}

# Container network interface scylla-ccm's containers come up on; single
# -network containers from run_container() get this name from podman/docker.
NODE_IFACE = "eth0"


def _client_name(name):
    return f"{name}-client"


def _delay_ms_for(dc, rack):
    """netem delay for a node's tier, keyed off TOPOLOGY's dc1/rack1 as home."""
    if dc == "dc1" and rack == "rack1":
        return 0
    if dc == "dc1":
        return INTER_RACK_DELAY_MS
    return INTER_DC_DELAY_MS


def _apply_netem(cluster):
    client = cluster.get_container_client()
    for node in cluster.nodelist():
        delay = _delay_ms_for(node.data_center, node.rack)
        if delay <= 0:
            continue
        rc, out, err = client.exec_command(
            node.pid, ["tc", "qdisc", "add", "dev", NODE_IFACE, "root", "netem", "delay", f"{delay}ms"]
        )
        if rc != 0:
            # Most likely `tc` (iproute2) isn't installed in the scylla image.
            sys.exit(
                f"failed to add netem delay on {node.name} ({node.data_center}/{node.rack}): {err}\n"
                "does the scylla image have iproute2 (tc) installed?"
            )


def _start_client_container(cluster):
    client = cluster.get_container_client()
    client.run_container(
        image=cluster.docker_image,
        name=_client_name(cluster.name),
        network=cluster.cluster_network,
        entrypoint="sleep",
        command=["infinity"],
        detach=True,
    )


def create(name, image):
    path = os.path.join(CCM_ROOT, name)
    if os.path.exists(path):
        sys.exit(f"cluster {name} already exists at {path}; run 'remove' first")

    cluster = ScyllaDockerCluster(CCM_ROOT, name, docker_image=image, container_runtime="podman")
    cluster.populate(TOPOLOGY)
    cluster.start(wait_for_binary_proto=True)
    _apply_netem(cluster)
    _start_client_container(cluster)
    info(name, cluster)


def _load(name):
    return ClusterFactory.load(CCM_ROOT, name)


def info(name, cluster=None):
    cluster = cluster or _load(name)
    print(f"cluster:\t{cluster.name}")
    print(f"image:\t\t{cluster.docker_image}")
    print(f"client:\t\t{_client_name(cluster.name)}")
    for node in cluster.nodelist():
        dc, rack = node.data_center, node.rack
        ip = node.network_interfaces["binary"][0]
        print(f"node:\t\t{node.name}\t{dc}/{rack}\t{ip}")


def enable_tls(name):
    """Turn on client_encryption_options in place and restart, for E5.

    Self-signed is enough: scylla-bench's -tls-host-verification defaults off,
    so no CA is distributed and nothing here needs a JDK/keytool.
    """
    import subprocess

    cluster = _load(name)
    ssl_dir = os.path.join(cluster.get_path(), "ssl")
    os.makedirs(ssl_dir, exist_ok=True)
    subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "365",
         "-subj", "/CN=ccm-node", "-keyout", os.path.join(ssl_dir, "ccm_node.key"),
         "-out", os.path.join(ssl_dir, "ccm_node.pem")],
        check=True, capture_output=True,
    )
    cluster.enable_ssl(ssl_dir, require_client_auth=False)
    # enable_ssl only rewrites cluster.conf; this pushes it into each node yaml.
    cluster.set_configuration_options({})
    cluster.stop()
    cluster.start(wait_for_binary_proto=True)
    # No native_transport_port_ssl, so 9042 itself becomes TLS-only.
    print(f"TLS enabled on {name}; port 9042 is now encrypted")


def disable_tls(name):
    """Undo enable-tls, so plaintext archetypes can run on the same cluster."""
    cluster = _load(name)
    cluster.set_configuration_options({"client_encryption_options": {"enabled": False}})
    cluster.stop()
    cluster.start(wait_for_binary_proto=True)
    print(f"TLS disabled on {name}; port 9042 is plaintext again")


def remove(name):
    cluster = _load(name)
    cluster.get_container_client().remove_container(_client_name(name), force=True)
    cluster.remove()
    print(f"removed {name}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=["create", "info", "enable-tls", "disable-tls", "remove"])
    ap.add_argument("--name", default=DEFAULT_NAME)
    ap.add_argument("--image", default=DEFAULT_IMAGE)
    args = ap.parse_args()

    if args.action == "create":
        create(args.name, args.image)
    elif args.action == "info":
        info(args.name)
    elif args.action == "enable-tls":
        enable_tls(args.name)
    elif args.action == "disable-tls":
        disable_tls(args.name)
    else:
        remove(args.name)


if __name__ == "__main__":
    main()

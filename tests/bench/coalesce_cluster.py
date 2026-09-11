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
"""

import argparse
import os
import subprocess
import sys

sys.path.insert(0, os.path.expanduser("~/github/scylla-ccm"))

try:
    # ScyllaPodmanCluster (netem-delay tiers, start_client_container) is not
    # in upstream scylla-ccm; needs a checkout at/after commit eb97a6e0.
    from ccmlib.scylla_podman_cluster import ScyllaPodmanCluster  # noqa: E402
except ImportError as e:
    sys.exit(
        "scylla-ccm at ~/github/scylla-ccm lacks "
        "ccmlib.scylla_podman_cluster.ScyllaPodmanCluster "
        f"(needs commit eb97a6e0 or later): {e}"
    )
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


def create(name, image):
    path = os.path.join(CCM_ROOT, name)
    if os.path.exists(path):
        sys.exit(f"cluster {name} already exists at {path}; run 'remove' first")

    cluster = ScyllaPodmanCluster(
        CCM_ROOT,
        name,
        podman_image=image,
        inter_rack_delay_ms=INTER_RACK_DELAY_MS,
        inter_dc_delay_ms=INTER_DC_DELAY_MS,
        # Rootless podman here has no delegated cpuset controller, so CPU
        # pinning cannot be used; run benchmarks on an otherwise idle machine.
        pinning=False,
    )
    cluster.populate(TOPOLOGY)
    cluster.start(wait_for_binary_proto=True)
    cluster.start_client_container()
    info(name, cluster)


def _load(name):
    return ClusterFactory.load(CCM_ROOT, name)


def info(name, cluster=None):
    cluster = cluster or _load(name)
    print(f"cluster:\t{cluster.name}")
    print(f"image:\t\t{getattr(cluster, 'podman_image', '?')}")
    print(f"client:\t\t{cluster._client_container_name()}")
    for node in cluster.nodelist():
        dc, rack = node.data_center, node.rack
        ip = node.network_interfaces["binary"][0]
        print(f"node:\t\t{node.name}\t{dc}/{rack}\t{ip}")


def enable_tls(name):
    """Turn on client_encryption_options in place and restart, for E5.

    Self-signed is enough: scylla-bench's -tls-host-verification defaults off,
    so no CA is distributed and nothing here needs a JDK/keytool.
    """
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
    cluster.stop_client_container()
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

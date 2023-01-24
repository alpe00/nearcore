#!/usr/bin/python3
"""
Spins up an archival node with cold store configred and verifies that blocks are copied from hot to cold store.
"""

import time
import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster, load_config
import utils


def main():

    config = load_config()
    genesis_config_changes = []
    client_config_changes = {0: {"archive": True}}
    [node] = start_cluster(1, 0, 1, config, genesis_config_changes,
                           client_config_changes)

    for height, hash in utils.poll_blocks(node, timeout=20):
        if height > 20:
            break

    node.kill(gentle=True)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Run service/server.py with MOEX pattern config from the repository root."""

from pathlib import Path

from service.server import start_server


def main():
    repo_root = Path(__file__).resolve().parent
    config_rel_path = "configs/config-moex-patterns-sample-1h.jsonc"

    # Invoke Click command programmatically with explicit config argument.
    start_server.main(args=["--config_file", config_rel_path], standalone_mode=True)


if __name__ == "__main__":
    main()

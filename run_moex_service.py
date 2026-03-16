#!/usr/bin/env python3
"""Run moex/moex_service.py with the correct working directory."""

import os
import runpy
import sys
from pathlib import Path


def main():
    repo_root = Path(__file__).resolve().parent
    moex_dir = repo_root / 'moex'
    service_path = moex_dir / 'moex_service.py'

    original_cwd = Path.cwd()
    original_argv0 = sys.argv[0]

    os.chdir(moex_dir)
    sys.path.insert(0, str(moex_dir))
    sys.argv[0] = str(service_path)

    try:
        runpy.run_path(str(service_path), run_name='__main__')
    finally:
        sys.argv[0] = original_argv0
        os.chdir(original_cwd)


if __name__ == '__main__':
    main()
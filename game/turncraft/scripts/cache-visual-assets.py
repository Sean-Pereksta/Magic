#!/usr/bin/env python3
"""Download and verify Cloudflare artwork into a temporary cache outside GitHub."""
import argparse
import json
from pathlib import Path
from visual_assets import download_assets

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path)
    parser.add_argument("--refresh", action="store_true", help="Check every current public URL again")
    args = parser.parse_args()
    print(json.dumps(download_assets(args.directory, refresh=args.refresh)))

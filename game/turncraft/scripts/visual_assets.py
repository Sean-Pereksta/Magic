"""Resolve and cache the exact production artwork hosted on Cloudflare."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

GAME = Path(__file__).resolve().parents[1]
ASSETS = GAME / "assets"
INDEX = json.loads((ASSETS / "visual-asset-index.json").read_text())
BASE_URL = INDEX["baseUrl"]
ORIGIN = urlsplit(BASE_URL)


def cache_directory():
    override = os.environ.get("TURNCRAFT_ASSET_CACHE")
    return Path(override).resolve() if override else Path(tempfile.gettempdir()) / "turncraft-artwork" / ORIGIN.netloc


def asset_key(url):
    parsed = urlsplit(url)
    key = parsed.path.lstrip("/")
    if (parsed.scheme != "https" or parsed.netloc != ORIGIN.netloc or parsed.query or parsed.fragment
            or key not in INDEX["images"] or ".." in parsed.path.split("/")):
        raise ValueError(f"Not a registered Cloudflare artwork URL: {url}")
    return key


def asset_path(url, directory=None):
    return Path(directory or cache_directory()) / asset_key(url)


def production_urls():
    animation = json.loads((ASSETS / "animation-manifest.json").read_text())
    terrain = json.loads((ASSETS / "terrain-manifest.json").read_text())
    effects = json.loads((ASSETS / "effects-manifest.json").read_text())
    urls = {state["sheet"] for asset in animation["assets"].values() for state in asset["states"].values()}
    urls.update(record["file"] for map_assets in terrain["maps"].values() for record in map_assets.values())
    urls.add(effects["file"])
    expected = {BASE_URL + key for key in INDEX["images"]}
    if urls != expected:
        raise ValueError(f"Artwork index mismatch; missing={sorted(expected-urls)}, unexpected={sorted(urls-expected)}")
    for url in urls:
        asset_key(url)
    return sorted(urls)


def download_assets(directory=None, *, refresh=False, workers=8):
    directory = Path(directory or cache_directory())
    urls = production_urls()

    def download(url):
        record = INDEX["images"][asset_key(url)]
        destination = asset_path(url, directory)
        if not refresh and destination.is_file():
            existing = destination.read_bytes()
            if len(existing) == record["bytes"] and hashlib.sha256(existing).hexdigest() == record["sha256"]:
                return False
        request = Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "image/webp"})
        for attempt in range(3):
            try:
                with urlopen(request, timeout=30) as response:
                    if response.status != 200:
                        raise ValueError(f"Artwork request returned {response.status}: {url}")
                    data = response.read(record["bytes"] + 1)
                break
            except (HTTPError, URLError, TimeoutError, ConnectionError) as error:
                if isinstance(error, HTTPError) and error.code not in {408, 429, 500, 502, 503, 504}:
                    raise RuntimeError(f"Cannot load Cloudflare artwork: {url}") from error
                if attempt == 2:
                    raise RuntimeError(f"Cannot load Cloudflare artwork after retries: {url}") from error
                time.sleep(2 ** attempt)
        if (len(data) != record["bytes"] or data[:4] != b"RIFF" or data[8:12] != b"WEBP"
                or hashlib.sha256(data).hexdigest() != record["sha256"]):
            raise ValueError(f"Uploaded artwork differs from the production image: {url}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as output:
                temporary = Path(output.name)
                output.write(data)
            temporary.replace(destination)
        finally:
            if temporary and temporary.exists():
                temporary.unlink()
        return True

    with ThreadPoolExecutor(max_workers=workers) as pool:
        downloaded = sum(pool.map(download, urls))
    return {"directory": str(directory), "images": len(urls), "downloaded": downloaded,
            "bytes": sum(record["bytes"] for record in INDEX["images"].values())}

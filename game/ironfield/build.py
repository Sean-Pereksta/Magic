#!/usr/bin/env python3
"""Bundle IRONFIELD into one offline HTML file. Standard Python only.

Run `python build.py` from any directory. This creates IRONFIELD.html and
dist/index.html. No network access, package manager, or build server is used.
"""
from pathlib import Path
import re


def build():
    root = Path(__file__).resolve().parent
    html = (root / "index.html").read_text(encoding="utf-8")
    css = (root / "src" / "style.css").read_text(encoding="utf-8")
    html = html.replace('<link rel="stylesheet" href="src/style.css">',
                        '<style>\n' + css + '\n</style>')
    for filename in ("data.js", "tactics.js", "impacts.js", "siege.js", "engine.js", "graphics.js", "app.js"):
        script = (root / "src" / filename).read_text(encoding="utf-8")
        script = re.sub(r"</script", r"<\\/script", script, flags=re.IGNORECASE)
        html = html.replace(f'<script src="src/{filename}"></script>',
                            '<script>\n' + script + '\n</script>')
    if re.search(r'<(?:script|link)\b[^>]*(?:src|href)="src/', html):
        raise RuntimeError("An external source reference remains in the bundle.")
    html = html.replace('<!doctype html>', '<!doctype html>\n<!-- IRONFIELD 2.5.1 | Standalone offline game | All source included -->', 1)
    out = root / "IRONFIELD.html"
    out.write_text(html, encoding="utf-8")
    (root / "dist").mkdir(exist_ok=True)
    (root / "dist" / "index.html").write_text(html, encoding="utf-8")
    print(f"Built {out.name}: {out.stat().st_size:,} bytes; no external dependencies.")


if __name__ == "__main__":
    build()

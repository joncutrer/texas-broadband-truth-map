"""
serve.py — Build _site/ directory mirroring GitHub Pages deploy, then serve it.

Mirrors what .github/workflows/deploy.yml does:
  mkdir -p _site/data/processed
  cp frontend/* → _site/
  cp data/processed/* → _site/data/processed/

Then serves _site/ on http://localhost:8000

Usage:
    uv run python serve.py
    uv run python serve.py --port 8080
"""

import argparse
import http.server
import os
import shutil
import socketserver
from pathlib import Path

ROOT          = Path(__file__).parent
SITE_DIR      = ROOT / "_site"
PROCESSED_DIR = ROOT / "data" / "processed"
FRONTEND_DIR  = ROOT / "frontend"


def build_site() -> None:
    """Recreate _site/ to mirror the GitHub Pages deploy step."""
    print("[serve] Building _site/ …")

    # Clean and recreate directory structure
    if SITE_DIR.exists():
        shutil.rmtree(SITE_DIR)
    (SITE_DIR / "data" / "processed").mkdir(parents=True, exist_ok=True)

    # Copy frontend files (index.html, map.js, style.css)
    for src in FRONTEND_DIR.iterdir():
        if src.is_file():
            shutil.copy2(src, SITE_DIR / src.name)
            print(f"[serve]   {src.name}")

    # Copy processed data files
    if not PROCESSED_DIR.exists() or not any(PROCESSED_DIR.iterdir()):
        print(
            "[serve] WARNING: data/processed/ is empty or missing. "
            "Run the pipeline first:\n"
            "  uv run python pipeline/fetch_fcc.py\n"
            "  uv run python pipeline/build_geojson.py"
        )
    else:
        for src in PROCESSED_DIR.iterdir():
            if src.is_file():
                shutil.copy2(src, SITE_DIR / "data" / "processed" / src.name)
                print(f"[serve]   data/processed/{src.name}")

    # Matches deploy.yml
    (SITE_DIR / ".nojekyll").touch()
    print("[serve] _site/ ready.")


def serve(port: int) -> None:
    os.chdir(SITE_DIR)

    class Handler(http.server.SimpleHTTPRequestHandler):
        def log_message(self, fmt, *args):
            # Only log non-200/304 responses to reduce noise
            if args[1] not in ("200", "304"):
                super().log_message(fmt, *args)

    with socketserver.TCPServer(("", port), Handler) as httpd:
        print(f"[serve] http://localhost:{port}/")
        httpd.serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build _site/ and serve locally.")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    build_site()
    serve(args.port)

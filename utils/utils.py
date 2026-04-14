#!/usr/bin/env python3
"""
Fetch a URL and print the extracted text content to stdout.
Uses a browser User-Agent so news sites don't block the request.

Usage:
    python3 utils.py <url> [--max-chars N]
"""

import argparse
import re
import sys
import urllib.request
import urllib.error

parser = argparse.ArgumentParser()
parser.add_argument("url")
parser.add_argument("--max-chars", type=int, default=8000)
args = parser.parse_args()

req = urllib.request.Request(
    args.url,
    headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
             "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
)

try:
    with urllib.request.urlopen(req, timeout=15) as r:
        html = r.read().decode("utf-8", errors="ignore")
except urllib.error.HTTPError as e:
    print(f"Error: HTTP {e.code} — {args.url}", file=sys.stderr)
    sys.exit(1)
except Exception as e:
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)

text = re.sub(r"(?s)<script[^>]*>.*?</script>", " ", html)
text = re.sub(r"(?s)<style[^>]*>.*?</style>", " ", text)
text = re.sub(r"<[^>]+>", " ", text)
text = re.sub(r"&[a-z#0-9]+;", " ", text)
text = re.sub(r"\s+", " ", text).strip()
print(text[:args.max_chars])

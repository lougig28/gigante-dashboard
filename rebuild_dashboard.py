#!/usr/bin/env python3
"""
Gigante Dashboard Rebuilder
Takes dashboard_data.json and injects it into index.html
by replacing the const D={...}; block with fresh data.
"""
import json
import re
import sys
import os
from datetime import datetime, timezone

def rebuild():
    data_path = os.path.join(os.path.dirname(__file__) or '.', 'dashboard_data.json')
    html_path = os.path.join(os.path.dirname(__file__) or '.', 'index.html')

    if not os.path.exists(data_path):
        print(f"ERROR: {data_path} not found")
        sys.exit(1)

    with open(data_path, 'r') as f:
        data = json.load(f)
    print(f"Loaded data: {len(json.dumps(data))} bytes")

    if not os.path.exists(html_path):
        print(f"ERROR: {html_path} not found")
        sys.exit(1)

    with open(html_path, 'r') as f:
        html = f.read()
    print(f"Loaded HTML: {len(html)} bytes")

    pattern = r'const D=\{.*?\};'
    match = re.search(pattern, html, re.DOTALL)

    if not match:
        print("ERROR: Could not find 'const D={...};' in index.html")
        sys.exit(1)

    print(f"Found data blob: {len(match.group())} chars at position {match.start()}")

    new_data = 'const D=' + json.dumps(data, separators=(',', ':')) + ';'
    print(f"New data blob: {len(new_data)} chars")

    new_html = html[:match.start()] + new_data + html[match.end():]

    now = datetime.now(timezone.utc).strftime('%b %d, %Y %H:%M UTC')
    new_html = re.sub(
        r'Updated [A-Z][a-z]+ \d+, \d{4}[^&]*',
        f'Updated {now}',
        new_html,
        count=1
    )

    with open(html_path, 'w') as f:
        f.write(new_html)

    print(f"Dashboard rebuilt: {len(new_html)} bytes")
    print(f"Timestamp: {now}")

if __name__ == '__main__':
    rebuild()

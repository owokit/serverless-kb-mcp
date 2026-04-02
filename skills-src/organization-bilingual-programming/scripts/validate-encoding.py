from __future__ import annotations

import sys
from pathlib import Path


def main() -> int:
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path.cwd()
    failures: list[str] = []
    for path in root.rglob('*'):
        if not path.is_file():
            continue
        if path.suffix.lower() not in {'.md', '.yaml', '.yml', '.py', '.txt'}:
            continue
        data = path.read_bytes()
        if data.startswith(b'ï»¿'):
            failures.append(f'BOM: {path}')
            continue
        try:
            data.decode('utf-8')
        except UnicodeDecodeError:
            failures.append(f'Invalid UTF-8: {path}')
    if failures:
        for line in failures:
            print(line)
        return 1
    print(f'Encoding OK: {root}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

"""Content fingerprints. A file's identity is its SHA-256, not its name or folder, so a file
delivered twice (renamed, re-sent, copied) is recognised as the same content."""
from __future__ import annotations

import hashlib
from pathlib import Path

EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()


def sha256_file(path: Path, chunk_size: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()

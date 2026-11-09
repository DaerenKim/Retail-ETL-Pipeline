"""Find files in the landing area and work out which store / business date / version each one is."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from .config import FilePattern
from .fingerprint import sha256_file


@dataclass
class LandingFile:
    path: Path
    file_name: str
    landing_folder: str
    store_code: str | None          # None when the filename matches no pattern
    business_date: date | None
    version: int
    pattern: str | None
    bytes: int
    sha256: str = ""

    @property
    def partition(self) -> tuple[str, str]:
        return (self.store_code or "", self.business_date.isoformat() if self.business_date else "")


def parse_filename(name: str, patterns: list[FilePattern]) -> tuple[str, date, int, str] | None:
    """Return (store_code, business_date, version, pattern_name) or None if no pattern matches.

    >>> from etl.config import load_settings
    >>> s = load_settings()
    >>> parse_filename("sales_GGN014_29-09-2026_v2.csv", s.file_patterns)[:3]
    ('GGN014', datetime.date(2026, 9, 29), 2)
    """
    for fp in patterns:
        m = fp.regex.match(name)
        if not m:
            continue
        try:
            d = datetime.strptime(m.group("date"), fp.date_format).date()
        except ValueError:
            return None
        version = int(m.group("version")) if "version" in fp.regex.groupindex and m.group("version") else 1
        return m.group("store").upper(), d, version, fp.name
    return None


def discover_files(landing_dir: Path, patterns: list[FilePattern], until: date | None = None) -> list[LandingFile]:
    """List every .csv under landing_dir/<arrival-folder>/ in arrival order (folder, then filename).

    `until` limits discovery to arrival folders <= that date, to simulate daily runs.
    """
    if not landing_dir.exists():
        return []
    files: list[LandingFile] = []
    for folder in sorted(p for p in landing_dir.iterdir() if p.is_dir()):
        if until is not None:
            try:
                if date.fromisoformat(folder.name) > until:
                    continue
            except ValueError:
                pass  # non-date folder names are always included
        for path in sorted(folder.iterdir()):
            if not path.is_file() or path.suffix.lower() != ".csv":
                continue
            parsed = parse_filename(path.name, patterns)
            store, bdate, version, pat = parsed if parsed else (None, None, 1, None)
            files.append(LandingFile(path=path, file_name=path.name, landing_folder=folder.name,
                                     store_code=store, business_date=bdate, version=version,
                                     pattern=pat, bytes=path.stat().st_size))
    return files


def fingerprint(files: list[LandingFile]) -> None:
    for f in files:
        f.sha256 = sha256_file(f.path)

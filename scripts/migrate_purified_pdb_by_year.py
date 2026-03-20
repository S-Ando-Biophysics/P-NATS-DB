#!/usr/bin/env python3
from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any, Dict, List

# Project root directory
ROOT = Path(__file__).resolve().parents[1]

# Directory settings
JSON_DIR = ROOT / "data" / "json"
PURIFIED_DIR = ROOT / "data" / "purified_pdb"

# JSON file paths
ENTRIES_PROCESSED_JSON = JSON_DIR / "entries_processed.json"


def get_release_year(entry: Dict[str, Any]) -> str | None:
    """Extract release year from release_date."""
    release_date = str(entry.get("release_date", "")).strip()
    if len(release_date) >= 4 and release_date[:4].isdigit():
        return release_date[:4]
    return None


def load_entries() -> List[Dict[str, Any]]:
    """Load entries_processed.json."""
    if not ENTRIES_PROCESSED_JSON.exists():
        raise FileNotFoundError(f"{ENTRIES_PROCESSED_JSON} not found")
    return json.loads(ENTRIES_PROCESSED_JSON.read_text(encoding="utf-8"))


def build_pdb_year_map(entries: List[Dict[str, Any]]) -> Dict[str, str]:
    """Build mapping: pdb_id -> release_year."""
    mapping: Dict[str, str] = {}
    for entry in entries:
        pdb_id = str(entry.get("pdb_id", "")).upper()
        year = get_release_year(entry)
        if pdb_id and year:
            mapping[pdb_id] = year
    return mapping


def glob_flat_pdb_files() -> List[Path]:
    """Find only top-level purified pdb files, excluding already migrated files."""
    return sorted(PURIFIED_DIR.glob("*.pdb"))


def extract_pdb_id_from_filename(path: Path) -> str | None:
    """
    Extract PDB ID from filename.
    Example:
      1TUP-1-Purified.pdb -> 1TUP
      101D-2-Purified.pdb -> 101D
    """
    parts = path.name.split("-")
    if not parts:
        return None
    pdb_id = parts[0].strip().upper()
    return pdb_id or None


def move_file_to_year_dir(src: Path, year: str) -> Path:
    """Move one file into data/purified_pdb/<year>/"""
    dest_dir = PURIFIED_DIR / year
    dest_dir.mkdir(parents=True, exist_ok=True)

    dst = dest_dir / src.name
    if dst.exists():
        dst.unlink()

    shutil.move(str(src), str(dst))
    return dst


def main() -> None:
    entries = load_entries()
    pdb_year_map = build_pdb_year_map(entries)
    flat_files = glob_flat_pdb_files()

    moved = 0
    skipped = 0

    print(f"Found {len(flat_files)} top-level .pdb files in {PURIFIED_DIR}")

    for src in flat_files:
        pdb_id = extract_pdb_id_from_filename(src)
        if not pdb_id:
            print(f"[skip] Could not parse PDB ID from filename: {src.name}")
            skipped += 1
            continue

        year = pdb_year_map.get(pdb_id)
        if not year:
            print(f"[skip] No valid release year found for {pdb_id}: {src.name}")
            skipped += 1
            continue

        dst = move_file_to_year_dir(src, year)
        print(f"[move] {src.relative_to(ROOT)} -> {dst.relative_to(ROOT)}")
        moved += 1

    print()
    print("Migration completed.")
    print(f"Moved:   {moved}")
    print(f"Skipped: {skipped}")


if __name__ == "__main__":
    main()
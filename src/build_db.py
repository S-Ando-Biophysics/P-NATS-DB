#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List

# Project root directory
ROOT = Path(__file__).resolve().parents[1]

# Directory settings
JSON_DIR = ROOT / "data" / "json"
DB_DIR = ROOT / "data" / "db"

# Data source and database paths
ENTRIES_FINAL_JSON = JSON_DIR / "entries_final.json"
SQLITE_PATH = DB_DIR / "pnats-db.sqlite"


def ensure_dirs() -> None:
    """Create necessary directories for the database."""
    DB_DIR.mkdir(parents=True, exist_ok=True)


def load_final_entries() -> List[Dict[str, Any]]:
    """Load the final processed entries from JSON."""
    if not ENTRIES_FINAL_JSON.exists():
        print(f"Error: {ENTRIES_FINAL_JSON} not found.")
        return []
    data = json.loads(ENTRIES_FINAL_JSON.read_text(encoding="utf-8"))
    return data


def create_schema(conn: sqlite3.Connection) -> None:
    """Initialize the database schema. Constraints are relaxed to allow entries with no assemblies."""
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS entries")
    cur.execute("DROP TABLE IF EXISTS assemblies")
    cur.execute("DROP TABLE IF EXISTS chains")

    # Primary entry table
    cur.execute(
        """
        CREATE TABLE entries (
            pdb_id TEXT PRIMARY KEY,
            method TEXT,
            resolution REAL,
            release_date TEXT,
            rcsb_url TEXT NOT NULL,
            nakb_url TEXT NOT NULL,
            view_structure_url TEXT NOT NULL,
            representative_assembly_id INTEGER,
            na_info TEXT,
            purified_structure TEXT,
            json_path TEXT
        )
        """
    )

    # Biological assembly table
    cur.execute(
        """
        CREATE TABLE assemblies (
            pdb_id TEXT NOT NULL,
            assembly_id INTEGER NOT NULL,
            na_info TEXT,
            purified_structure TEXT,
            PRIMARY KEY (pdb_id, assembly_id)
        )
        """
    )

    # Chain details table
    cur.execute(
        """
        CREATE TABLE chains (
            pdb_id TEXT NOT NULL,
            assembly_id INTEGER NOT NULL,
            chain_id TEXT NOT NULL,
            na_type TEXT,
            sequence TEXT,
            length INTEGER,
            PRIMARY KEY (pdb_id, assembly_id, chain_id)
        )
        """
    )

    conn.commit()


def insert_data(conn: sqlite3.Connection, entries: List[Dict[str, Any]]) -> None:
    """Insert processed metadata and structural data into the SQLite tables."""
    cur = conn.cursor()

    for entry in entries:
        # Insert into entries table
        cur.execute(
            """
            INSERT INTO entries (
                pdb_id,
                method,
                resolution,
                release_date,
                rcsb_url,
                nakb_url,
                view_structure_url,
                representative_assembly_id,
                na_info,
                purified_structure,
                json_path
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entry["pdb_id"],
                entry.get("method", "Unknown"),
                entry.get("resolution"),
                entry.get("release_date", ""),
                entry["rcsb_url"],
                entry["nakb_url"],
                entry["view_structure_url"],
                entry.get("representative_assembly_id"), # Supports None
                entry.get("na_info", ""),
                entry.get("purified_structure", ""),
                entry.get("json_path", ""),
            ),
        )

        # Insert into assemblies and chains tables
        for assembly in entry.get("assemblies", []):
            cur.execute(
                """
                INSERT INTO assemblies (
                    pdb_id,
                    assembly_id,
                    na_info,
                    purified_structure
                )
                VALUES (?, ?, ?, ?)
                """,
                (
                    entry["pdb_id"],
                    assembly["assembly_id"],
                    assembly.get("na_info", ""),
                    assembly.get("purified_structure", ""),
                ),
            )

            for chain in assembly.get("chains", []):
                cur.execute(
                    """
                    INSERT INTO chains (
                        pdb_id,
                        assembly_id,
                        chain_id,
                        na_type,
                        sequence,
                        length
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        entry["pdb_id"],
                        assembly["assembly_id"],
                        chain["chain_id"],
                        chain.get("na_type", ""),
                        chain.get("sequence", ""),
                        chain.get("length", 0),
                    ),
                )

    conn.commit()


def main() -> None:
    """Main execution logic to build the SQLite database from final JSON."""
    ensure_dirs()
    entries = load_final_entries()

    if not entries:
        print("No data to import. Exiting.")
        return

    print(f"Connecting to database at {SQLITE_PATH}...")
    conn = sqlite3.connect(SQLITE_PATH)
    try:
        print("Creating schema...")
        create_schema(conn)
        print(f"Inserting {len(entries)} entries with metadata...")
        insert_data(conn, entries)
    finally:
        conn.close()

    print(f"Successfully built SQLite database: {SQLITE_PATH}")


if __name__ == "__main__":
    main()
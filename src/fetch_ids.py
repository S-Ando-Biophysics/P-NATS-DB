#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import requests


# Project root directory
ROOT = Path(__file__).resolve().parents[1]
JSON_DIR = ROOT / "data" / "json"
CANDIDATE_DATA_JSON = JSON_DIR / "candidate_data.json"
EXCLUDED_PDB_FILE = ROOT / "src" / "excluded_pdb_ids.txt"


def ensure_dirs() -> None:
    """Create necessary directories if they do not exist."""
    JSON_DIR.mkdir(parents=True, exist_ok=True)


def load_excluded_pdb_ids() -> set[str]:
    """Load excluded PDB IDs from text file."""
    if not EXCLUDED_PDB_FILE.exists():
        print(f"Excluded PDB file not found: {EXCLUDED_PDB_FILE}")
        return set()

    excluded_ids = {
        line.strip().upper()
        for line in EXCLUDED_PDB_FILE.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    }

    print(f"Loaded {len(excluded_ids)} excluded PDB IDs.")
    return excluded_ids


def normalize_broader_method(raw_val: Any) -> str:
    """
    Normalize RCSB broader experimental method categories into 5 classes:
    X-ray, NMR, EM, Neutron, Other.

    RCSB broader categories:
      - X-ray
      - NMR
      - EM
      - Neutron
      - Integrative
      - Multiple methods
      - Other

    Here, Integrative / Multiple methods / Other are merged into "Other".
    """
    if raw_val is None:
        return "Other"

    method = str(raw_val).strip()

    if method in {"X-ray", "NMR", "EM", "Neutron"}:
        return method

    if method in {"Integrative", "Multiple methods", "Other"}:
        return "Other"

    # Fallback for unexpected values
    print(f"[warn] Unexpected broader experimental method: {method!r}")
    return "Other"


def search_pdb_ids(excluded_ids: set[str]) -> List[str]:
    """Search for PDB IDs that contain nucleic acids but no proteins."""
    print("Searching for nucleic-acid-only structures...")
    url = "https://search.rcsb.org/rcsbsearch/v2/query"

    query = {
        "query": {
            "type": "group",
            "logical_operator": "and",
            "nodes": [
                {
                    "type": "terminal",
                    "service": "text",
                    "parameters": {
                        "attribute": "rcsb_entry_info.polymer_entity_count_protein",
                        "operator": "equals",
                        "value": 0,
                    },
                },
                {
                    "type": "terminal",
                    "service": "text",
                    "parameters": {
                        "attribute": "rcsb_entry_info.polymer_entity_count_nucleic_acid",
                        "operator": "greater",
                        "value": 0,
                    },
                },
            ],
        },
        "return_type": "entry",
        "request_options": {
            "return_all_hits": True,
        },
    }

    response = requests.post(url, json=query, timeout=60)
    response.raise_for_status()
    result = response.json()

    pdb_ids = [hit["identifier"] for hit in result.get("result_set", [])]
    filtered_pdb_ids = [pdb_id for pdb_id in pdb_ids if pdb_id.upper() not in excluded_ids]

    print(f"Excluded {len(pdb_ids) - len(filtered_pdb_ids)} entries by excluded_pdb_ids.txt")
    return filtered_pdb_ids


def fetch_metadata(pdb_ids: List[str]) -> List[Dict[str, Any]]:
    """
    Fetch metadata (broader experimental method, resolution, release date)
    for given PDB IDs via GraphQL.
    """
    print(f"Fetching metadata for {len(pdb_ids)} entries...")
    url = "https://data.rcsb.org/graphql"

    batch_size = 100
    metadata_list: List[Dict[str, Any]] = []

    for i in range(0, len(pdb_ids), batch_size):
        batch = pdb_ids[i:i + batch_size]
        ids_str = ",".join(f'"{pdb_id}"' for pdb_id in batch)

        query = f"""
        {{
          entries(entry_ids: [{ids_str}]) {{
            rcsb_id
            rcsb_entry_info {{
              experimental_method
              resolution_combined
            }}
            rcsb_accession_info {{
              initial_release_date
            }}
          }}
        }}
        """

        response = requests.post(url, json={"query": query}, timeout=60)
        response.raise_for_status()
        data = response.json()

        entries = data.get("data", {}).get("entries", [])
        if not entries:
            continue

        for entry in entries:
            if not entry:
                continue

            raw_date = entry.get("rcsb_accession_info", {}).get("initial_release_date", "")
            release_date = raw_date.split("T")[0] if raw_date else ""

            res_list = entry.get("rcsb_entry_info", {}).get("resolution_combined") or []
            resolution = res_list[0] if res_list else None

            raw_method = entry.get("rcsb_entry_info", {}).get("experimental_method")
            method = normalize_broader_method(raw_method)

            metadata_list.append(
                {
                    "pdb_id": entry.get("rcsb_id"),
                    "method": method,
                    "resolution": resolution,
                    "release_date": release_date,
                }
            )

        print(f"  Progress: {min(i + batch_size, len(pdb_ids))}/{len(pdb_ids)}")

    return metadata_list


def main() -> None:
    """Main pipeline to create candidate_data.json."""
    ensure_dirs()

    excluded_ids = load_excluded_pdb_ids()
    pdb_ids = search_pdb_ids(excluded_ids)

    # Only when testing (limitation of number) --> Comment out
    # pdb_ids = pdb_ids[:1000]

    print(f"Found {len(pdb_ids)} entries for processing.")

    metadata = fetch_metadata(pdb_ids)

    CANDIDATE_DATA_JSON.write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"Saved candidate data to {CANDIDATE_DATA_JSON}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, List

# Project root directory
ROOT = Path(__file__).resolve().parents[1]

# Directory settings
JSON_DIR = ROOT / "data" / "json"
PURIFIED_DIR = ROOT / "data" / "purified_pdb"

# JSON file paths
CANDIDATE_DATA_JSON = JSON_DIR / "candidate_data.json"
ENTRIES_PROCESSED_JSON = JSON_DIR / "entries_processed.json"
FAILURES_JSON = JSON_DIR / "process_failures.json"


def ensure_dirs() -> None:
    """Create necessary directories if they do not exist."""
    JSON_DIR.mkdir(parents=True, exist_ok=True)
    PURIFIED_DIR.mkdir(parents=True, exist_ok=True)


def load_candidates() -> List[Dict[str, Any]]:
    """Load candidate metadata from candidate_data.json."""
    if not CANDIDATE_DATA_JSON.exists():
        print(f"Error: {CANDIDATE_DATA_JSON} not found.")
        return []
    return json.loads(CANDIDATE_DATA_JSON.read_text(encoding="utf-8"))


def run_command_safe(cmd: List[str]) -> bool:
    """
    Execute an external command.
    Returns True if success (exit 0), False otherwise.
    Does not raise Exception on non-zero exit status.
    """
    print("  Running:", " ".join(cmd))
    try:
        result = subprocess.run(cmd, check=False, cwd=ROOT, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"    [P-NATS Warning] Command returned non-zero status {result.returncode}")
            if result.stderr:
                print(f"    [stderr] {result.stderr.strip()}")
            return False
        return True
    except Exception as e:
        print(f"    [error] Failed to execute command: {e}")
        return False


def get_release_year(candidate: Dict[str, Any]) -> str:
    """Extract release year from release_date."""
    release_date = str(candidate.get("release_date", "")).strip()
    if len(release_date) >= 4 and release_date[:4].isdigit():
        return release_date[:4]
    return "unknown"


def glob_purified_root(pdb_id: str) -> List[Path]:
    """Search for Purified PDB files generated in the root directory."""
    return sorted(ROOT.glob(f"{pdb_id.upper()}-*-Purified.pdb"))


def glob_purified_data(pdb_id: str, release_year: str) -> List[Path]:
    """Search for Purified PDB files in the year-based data/purified_pdb directory."""
    year_dir = PURIFIED_DIR / release_year
    if not year_dir.exists():
        return []
    return sorted(year_dir.glob(f"{pdb_id.upper()}-*-Purified.pdb"))


def collect_outputs_from_root(pdb_id: str, release_year: str) -> List[str]:
    """Move generated files from the root directory to the year-based data directory."""
    src_files = glob_purified_root(pdb_id)
    dest_dir = PURIFIED_DIR / release_year
    dest_dir.mkdir(parents=True, exist_ok=True)

    collected: List[str] = []
    for src in src_files:
        dst = dest_dir / src.name
        if dst.exists():
            dst.unlink()
        shutil.move(str(src), str(dst))
        collected.append(dst.relative_to(ROOT).as_posix())

    return collected


def list_existing_outputs(pdb_id: str, release_year: str) -> List[str]:
    """Return a list of already existing output files in the year-based directory."""
    files = glob_purified_data(pdb_id, release_year)
    return [f.relative_to(ROOT).as_posix() for f in files]


def parse_assembly_id(path_str: str) -> int:
    """Extract Assembly ID from the filename (e.g., 1TUP-1-Purified.pdb -> 1)."""
    name = Path(path_str).name
    parts = name.split("-")
    if len(parts) < 2:
        return 1
    try:
        return int(parts[1])
    except ValueError:
        return 1


def make_view_structure_url(pdb_id: str) -> str:
    """Generate a URL for viewing the structure in PDBj Molmil."""
    return (
        "https://pdbj.org/molmil2/#fetch%20"
        f"{pdb_id};%20hide%20cartoon,%20all;%20show%20sticks,%20all;%20hide%20sticks,%20hydro;"
        "%20orient%20all;%20color%20orange,%20resn%20A;%20color%20cyan,%20resn%20G;"
        "%20color%20green,%20resn%20C;%20color%20purple,%20resn%20U;%20color%20orange,%20resn%20DA;"
        "%20color%20cyan,%20resn%20DG;%20color%20green,%20resn%20DC;%20color%20purple,%20resn%20DT;"
        "%20color%20red,%20symbol%20O;%20color%20blue,%20symbol%20N;%20show%20cartoon,%20resn%20ALA;"
        "%20show%20cartoon,%20resn%20ARG;%20show%20cartoon,%20resn%20ASN;%20show%20cartoon,%20resn%20ASP;"
        "%20show%20cartoon,%20resn%20CYS;%20show%20cartoon,%20resn%20GLN;%20show%20cartoon,%20resn%20GLU;"
        "%20show%20cartoon,%20resn%20GLY;%20show%20cartoon,%20resn%20HIS;%20show%20cartoon,%20resn%20ILE;"
        "%20show%20cartoon,%20resn%20LEU;%20show%20cartoon,%20resn%20LYS;%20show%20cartoon,%20resn%20MET;"
        "%20show%20cartoon,%20resn%20PHE;%20show%20cartoon,%20resn%20PRO;%20show%20cartoon,%20resn%20SER;"
        "%20show%20cartoon,%20resn%20THR;%20show%20cartoon,%20resn%20TRP;%20show%20cartoon,%20resn%20TYR;"
        "%20show%20cartoon,%20resn%20VAL;"
    )


def ensure_outputs_with_pnats(pdb_id: str, release_year: str) -> List[str]:
    """Execute P-NATS and collect the generated files even if status is non-zero."""
    existing_purified = list_existing_outputs(pdb_id, release_year)
    if existing_purified:
        print(f"  Skip existing outputs for {pdb_id}")
        return existing_purified

    run_command_safe(["P-NATS", pdb_id.upper()])

    purified_paths = collect_outputs_from_root(pdb_id, release_year)

    if not purified_paths:
        purified_paths = list_existing_outputs(pdb_id, release_year)

    return purified_paths


def process_one(candidate: Dict[str, Any]) -> Dict[str, Any]:
    """Process a single PDB entry. Generates entry data even if some files are missing."""
    pdb_id = candidate["pdb_id"]
    release_year = get_release_year(candidate)

    print(f"Processing {pdb_id}...")

    purified_paths = ensure_outputs_with_pnats(pdb_id, release_year)

    if not purified_paths:
        print(f"  [warning] No purified files found for {pdb_id}")

    assembly_ids = sorted({parse_assembly_id(p) for p in purified_paths})

    return {
        "pdb_id": pdb_id,
        "method": candidate.get("method", "Unknown"),
        "resolution": candidate.get("resolution"),
        "release_date": candidate.get("release_date", ""),
        "rcsb_url": f"https://www.rcsb.org/structure/{pdb_id}",
        "nakb_url": f"https://nakb.org/atlas={pdb_id}",
        "view_structure_url": make_view_structure_url(pdb_id),
        "assembly_ids": assembly_ids,
        "purified_structures": purified_paths,
    }


def main() -> None:
    ensure_dirs()
    candidates = load_candidates()

    processed: List[Dict[str, Any]] = []
    failures: List[Dict[str, str]] = []

    total = len(candidates)
    for i, candidate in enumerate(candidates, start=1):
        pdb_id = candidate.get("pdb_id", "Unknown")
        try:
            processed.append(process_one(candidate))
        except Exception as e:
            failures.append({"pdb_id": pdb_id, "error": str(e)})
            print(f"  [error] {pdb_id}: {e}")

        if i % 50 == 0 or i == total:
            print(f"Progress: {i}/{total}")

    ENTRIES_PROCESSED_JSON.write_text(
        json.dumps(processed, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    FAILURES_JSON.write_text(
        json.dumps(failures, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"Wrote {len(processed)} entries to {ENTRIES_PROCESSED_JSON}")
    if failures:
        print(f"Wrote {len(failures)} critical failures to {FAILURES_JSON}")


if __name__ == "__main__":
    main()

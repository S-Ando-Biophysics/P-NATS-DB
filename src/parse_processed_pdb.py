#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

# Project root directory
ROOT = Path(__file__).resolve().parents[1]

# Directory settings
JSON_DIR = ROOT / "data" / "json"
PURIFIED_DIR = ROOT / "data" / "purified_pdb"

# JSON file paths
ENTRIES_PROCESSED_JSON = JSON_DIR / "entries_processed.json"
ENTRIES_FINAL_JSON = JSON_DIR / "entries_final.json"

DNA_BASES = {"DA", "DG", "DC", "DT", "DU"}
RNA_BASES = {"A", "G", "C", "U", "T"}

RESIDUE_MAP = {
    "1": "a",
    "3": "c",
    "7": "g",
    "20": "t",
    "21": "u",
    "LIG": "n"
}

def convert_sequence(res_names: List[str]) -> str:
    """
    Convert residue names to single characters.
    Standard bases remain UPPERCASE (e.g., DA -> A, U -> U).
    Extension residues map to lowercase (e.g., 1 -> a).
    """
    converted = []
    for r in res_names:
        # DNA system (DA, DG, DC, DT, DU) -> Take last char., Uppercase
        if r in DNA_BASES:
            converted.append(r[-1].upper())
        # RNA system (A, G, C, U, T) -> Uppercase
        elif r in RNA_BASES:
            converted.append(r.upper())
        # Extension residues -> lowercase
        elif r in RESIDUE_MAP:
            converted.append(RESIDUE_MAP[r])
        else:
            # Fallback for others
            converted.append(r)
    return "".join(converted)

def classify_chain_type(res_names: List[str]) -> str:
    """
    Classify a single chain based on updated rules:
    - DNA: Contains DNA_BASES, NO RNA_BASES. (Extensions don't affect)
    - RNA: Contains RNA_BASES, NO DNA_BASES. (Extensions don't affect)
    - Chimera: Contains BOTH DNA_BASES and RNA_BASES.
    - Other: Contains NEITHER DNA_BASES nor RNA_BASES.
    """
    res_set = set(res_names)
    if not res_set:
        return "Unknown"

    has_dna_sys = any(r in DNA_BASES for r in res_set)
    has_rna_sys = any(r in RNA_BASES for r in res_set)

    if has_dna_sys and has_rna_sys:
        return "Chimera"
    
    if has_dna_sys:
        return "DNA"
    
    if has_rna_sys:
        return "RNA"

    return "Other"

# Priority for determining assembly-level NA type
PRIORITY = {"DNA": 0, "RNA": 1, "Chimera": 2, "Other": 3}

def format_na_info(chains: List[Dict[str, Any]]) -> str:
    """Determine ss/ds prefix and summarize assembly-level NA type."""
    num_chains = len(chains)
    if num_chains == 0:
        return "N/A"
    
    types = [c["na_type"] for c in chains]

    if num_chains == 1:
        return f"ss {types[0]}"
    
    if num_chains == 2:
        sorted_types = sorted(types, key=lambda x: PRIORITY.get(x, 99))
        return f"ds {sorted_types[0]}/{sorted_types[1]}"
    
    return "more than two strands"

def index_paths_by_assembly(paths: List[str]) -> Dict[int, str]:
    """Group file paths by Assembly ID."""
    mapping = {}
    for p in paths:
        name = Path(p).name
        parts = name.split("-")
        if len(parts) >= 2:
            try:
                aid = int(parts[1])
                mapping[aid] = p
            except ValueError:
                continue
    return mapping

def make_chain_records(pdb_path: Path) -> Tuple[List[Dict[str, Any]], str]:
    """Parse PDB and generate records for each chain."""
    if not pdb_path.exists():
        return [], "Unknown"

    chains_data: Dict[str, Dict[Tuple[int, str], str]] = {}

    with pdb_path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.startswith(("ATOM", "HETATM")):
                res_name = line[17:20].strip()
                chain_id = line[21].strip() or "A"
                
                try:
                    res_seq = int(line[22:26].strip())
                except ValueError:
                    continue
                
                i_code = line[26].strip()
                res_key = (res_seq, i_code)
                
                if chain_id not in chains_data:
                    chains_data[chain_id] = {}
                
                if res_key not in chains_data[chain_id]:
                    chains_data[chain_id][res_key] = res_name

    records = []
    for cid in sorted(chains_data.keys()):
        sorted_res_keys = sorted(chains_data[cid].keys())
        res_list = [chains_data[cid][k] for k in sorted_res_keys]
        
        ctype = classify_chain_type(res_list)
        records.append({
            "chain_id": cid,
            "na_type": ctype,
            "sequence": convert_sequence(res_list),
            "length": len(res_list)
        })

    na_info = format_na_info(records)
    return records, na_info

def build_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    """Assemble final entry data for all available assemblies."""
    purified_map = index_paths_by_assembly(entry.get("purified_structures", []))
    available_assembly_ids = sorted([aid for aid, p in purified_map.items() if (ROOT / p).exists()])

    assemblies = []
    for aid in available_assembly_ids:
        path = purified_map[aid]
        chains, na_info = make_chain_records(ROOT / path)
        if chains:
            assemblies.append({
                "assembly_id": aid,
                "na_info": na_info,
                "chains": chains,
                "purified_structure": path,
            })

    if not assemblies:
        return {
            "pdb_id": entry["pdb_id"],
            "method": entry.get("method", "Unknown"),
            "resolution": entry.get("resolution"),
            "release_date": entry.get("release_date", ""),
            "rcsb_url": entry["rcsb_url"],
            "nakb_url": entry["nakb_url"],
            "view_structure_url": entry["view_structure_url"],
            "assemblies": [],
            "representative_assembly_id": None,
            "na_info": "N/A",
            "chains": [],
            "purified_structure": "",
            "json_path": f"data/json/entries/{entry['pdb_id']}.json"
        }

    rep = assemblies[0]
    
    return {
        "pdb_id": entry["pdb_id"],
        "method": entry.get("method", "Unknown"),
        "resolution": entry.get("resolution"),
        "release_date": entry.get("release_date", ""),
        "rcsb_url": entry["rcsb_url"],
        "nakb_url": entry["nakb_url"],
        "view_structure_url": entry["view_structure_url"],
        "assemblies": assemblies,
        "representative_assembly_id": rep["assembly_id"],
        "na_info": rep["na_info"],
        "chains": rep["chains"],
        "purified_structure": rep["purified_structure"],
        "json_path": f"data/json/entries/{entry['pdb_id']}.json"
    }

def main() -> None:
    if not ENTRIES_PROCESSED_JSON.exists():
        print(f"Error: {ENTRIES_PROCESSED_JSON} not found.")
        return

    raw_entries = json.loads(ENTRIES_PROCESSED_JSON.read_text(encoding="utf-8"))
    final_entries = []

    print(f"Parsing {len(raw_entries)} entries...")
    for entry in raw_entries:
        try:
            processed_entry = build_entry(entry)
            final_entries.append(processed_entry)
        except Exception as e:
            print(f"  [error] {entry.get('pdb_id')}: {e}")

    # Sort entries by PDB ID ascending
    final_entries.sort(key=lambda x: x["pdb_id"])

    entry_dir = JSON_DIR / "entries"
    entry_dir.mkdir(parents=True, exist_ok=True)
    for entry in final_entries:
        (entry_dir / f"{entry['pdb_id']}.json").write_text(
            json.dumps(entry, indent=2, ensure_ascii=False),
            encoding="utf-8"
        )

    ENTRIES_FINAL_JSON.write_text(
        json.dumps(final_entries, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    print(f"Successfully created {len(final_entries)} entry JSONs.")

if __name__ == "__main__":
    main()
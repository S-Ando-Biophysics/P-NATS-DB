#!/bin/bash
# scripts/run_all.sh

set -e

echo "Step 1: Fetching IDs and Metadata..."
python3 src/fetch_ids.py

echo "Step 2: Processing structures with P-NATS..."
python3 src/process_structure.py

echo "Step 3: Parsing processed PDB files..."
python3 src/parse_processed_pdb.py

echo "Step 4: Building SQLite database..."
python3 src/build_db.py

echo "Step 5: Exporting index for web..."
python3 -c "import json; from pathlib import Path; f=Path('data/json/entries_final.json'); Path('data/json/index.json').write_text(f.read_text())"

echo "Pipeline completed successfully!"

#!/usr/bin/env bash
set -euo pipefail

#on active le venv
[ -d .venv ] && source .venv/bin/activate

#lance le pipeline de construction
spark-submit --master "local[*]" --py-files src/transformations.py src/build_patients.py
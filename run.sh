#!/usr/bin/env bash
set -euo pipefail

#on active le venv
[ -d .venv ] && source .venv/bin/activate

#lance le pipeline de construction
spark-submit --master "local[*]" src/build_patients.py
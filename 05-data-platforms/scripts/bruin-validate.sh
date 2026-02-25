#!/usr/bin/env bash
# Run Bruin validate against 05-data-platforms using its config (no root .bruin.yml).
# Usage: from repo root: ./05-data-platforms/scripts/bruin-validate.sh
#        or from 05-data-platforms: ./scripts/bruin-validate.sh
set -e
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"
exec bruin validate 05-data-platforms --config-file 05-data-platforms/.bruin.yml "$@"

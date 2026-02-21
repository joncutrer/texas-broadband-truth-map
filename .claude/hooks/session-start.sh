#!/bin/bash
set -euo pipefail

# Only run in remote (Claude Code on the web) environments
if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

REPO_DIR="${CLAUDE_PROJECT_DIR:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel)}"
VENV_DIR="$REPO_DIR/venv"

# Create venv if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
  python3 -m venv "$VENV_DIR"
fi

# Install/update pipeline dependencies
"$VENV_DIR/bin/pip" install --quiet -r "$REPO_DIR/pipeline/requirements.txt"

# Make venv available for the session
echo "export PATH=\"$VENV_DIR/bin:\$PATH\"" >> "${CLAUDE_ENV_FILE:-/dev/null}"
echo "export VIRTUAL_ENV=\"$VENV_DIR\"" >> "${CLAUDE_ENV_FILE:-/dev/null}"

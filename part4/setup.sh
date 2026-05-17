#!/usr/bin/env bash
# Part 4 – One-shot setup: initialise Flutter Web project + install API deps
#
# Run once from the part4/ directory:
#   bash setup.sh
#
# Then start everything:
#   bash api/run.sh                                             ← terminal 1
#   cd dashboard && flutter run -d web-server --web-port 3000  ← terminal 2
#   open http://localhost:3000

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── 1. Python API deps ────────────────────────────────────────────────────────
echo ">>> Installing Python API dependencies..."
VENV_PIP="$SCRIPT_DIR/../.venv/bin/pip"
"$VENV_PIP" install -r "$SCRIPT_DIR/api/requirements.txt" -q
echo "    Done."

# ── 2. Flutter project scaffold ───────────────────────────────────────────────
DASH="$SCRIPT_DIR/dashboard"
echo ">>> Initialising Flutter project in dashboard/..."
cd "$DASH"

if [ ! -d "web" ]; then
  flutter create --platforms=web --project-name=binance_dashboard . --quiet
  echo "    flutter create done."
else
  echo "    Web runner already present — skipping flutter create."
fi

# ── 4. flutter pub get ────────────────────────────────────────────────────────
echo ">>> Running flutter pub get..."
flutter pub get
echo "    Done."

echo ""
echo "=================================================="
echo "  Setup complete!"
echo ""
echo "  Start the stack:"
echo "    1. Kafka + HBase  (already running from part1/part3)"
echo "    2. Terminal A:  bash $SCRIPT_DIR/api/run.sh"
echo "    3. Terminal B:  cd $DASH && flutter run -d web-server --web-port 3000"
echo ""
echo "  Then open: http://localhost:3000"
echo "=================================================="

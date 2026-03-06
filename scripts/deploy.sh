#!/usr/bin/env bash
set -euo pipefail

# Meridian Insights — Full Deploy Script
#
# Automates the three-step deploy process:
#   1. databricks bundle deploy
#   2. workspace import-dir for frontend/dist (not synced by bundle)
#   3. databricks apps deploy to take a snapshot and start the app
#
# Usage:
#   ./scripts/deploy.sh                    # deploy with defaults
#   ./scripts/deploy.sh --target demo      # deploy to a specific target
#   ./scripts/deploy.sh --skip-build       # skip frontend build
#   ./scripts/deploy.sh --skip-permissions # skip SP permission grants
#   ./scripts/deploy.sh --setup-genie     # create/update Genie spaces after deploy

PROFILE="${PROFILE:-k2zkdm}"
TARGET="${TARGET:-dev}"
APP_NAME="meridian-portal"
SKIP_BUILD=false
SKIP_PERMISSIONS=false
SETUP_GENIE=false
WORKSPACE_USER=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --target) TARGET="$2"; shift 2 ;;
        --profile) PROFILE="$2"; shift 2 ;;
        --skip-build) SKIP_BUILD=true; shift ;;
        --skip-permissions) SKIP_PERMISSIONS=true; shift ;;
        --setup-genie) SETUP_GENIE=true; shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

WORKSPACE_USER=$(databricks current-user me --profile "$PROFILE" 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)['userName'])" 2>/dev/null || echo "")
if [[ -z "$WORKSPACE_USER" ]]; then
    echo "WARNING: Could not determine workspace user. Using default path."
    WORKSPACE_USER="unknown"
fi

WORKSPACE_PATH="/Workspace/Users/${WORKSPACE_USER}/meridian/files/src/app"
FRONTEND_DIR="src/app/frontend"

echo "=========================================="
echo "  Meridian Insights — Deploy"
echo "=========================================="
echo "  Profile:  $PROFILE"
echo "  Target:   $TARGET"
echo "  User:     $WORKSPACE_USER"
echo "  App:      $APP_NAME"
echo "=========================================="
echo ""

# Step 0: Build frontend (unless skipped)
if [[ "$SKIP_BUILD" == false ]]; then
    echo "[0/4] Building frontend..."
    (cd "$FRONTEND_DIR" && npm run build)
    echo "      Frontend built successfully."
    echo ""
else
    echo "[0/4] Skipping frontend build (--skip-build)"
    echo ""
fi

# Step 1: Bundle deploy
echo "[1/4] Deploying bundle..."
databricks bundle deploy -t "$TARGET"
echo "      Bundle deployed."
echo ""

# Step 2: Upload frontend/dist (not synced by bundle)
echo "[2/4] Uploading frontend/dist to workspace..."
databricks workspace import-dir "${FRONTEND_DIR}/dist" \
    "${WORKSPACE_PATH}/frontend/dist" \
    --profile "$PROFILE" --overwrite
echo "      Frontend dist uploaded."
echo ""

# Step 3: Deploy the app (takes snapshot, starts container)
echo "[3/4] Deploying app..."
databricks apps deploy "$APP_NAME" \
    --source-code-path "${WORKSPACE_PATH}" \
    --profile "$PROFILE"
echo "      App deployment initiated."
echo ""

# Step 4: Grant SP permissions (unless skipped)
if [[ "$SKIP_PERMISSIONS" == false ]]; then
    echo "[4/4] Checking SP permissions..."
    CATALOG=$(databricks bundle validate -t "$TARGET" 2>/dev/null | python3 -c "
import sys, json
data = json.load(sys.stdin)
print(data.get('variables', {}).get('catalog_name', {}).get('value', 'serverless_stable_k2zkdm_catalog'))
" 2>/dev/null || echo "serverless_stable_k2zkdm_catalog")

    echo "    Catalog: $CATALOG"
    echo "    Granting USE CATALOG, USE SCHEMA, SELECT to app SP..."

    for SCHEMA in meridian_research meridian_internal meridian_regulatory meridian_system meridian_staging; do
        databricks api post /api/2.0/sql/statements --profile "$PROFILE" --json "{
            \"warehouse_id\": \"e8eadc734c07e7f5\",
            \"statement\": \"GRANT USE SCHEMA, SELECT ON SCHEMA ${CATALOG}.${SCHEMA} TO \\\`${APP_NAME}\\\`\",
            \"wait_timeout\": \"30s\"
        }" > /dev/null 2>&1 && echo "    Granted on ${SCHEMA}" || echo "    WARNING: Could not grant on ${SCHEMA}"
    done

    databricks api post /api/2.0/sql/statements --profile "$PROFILE" --json "{
        \"warehouse_id\": \"e8eadc734c07e7f5\",
        \"statement\": \"GRANT USE CATALOG ON CATALOG ${CATALOG} TO \\\`${APP_NAME}\\\`\",
        \"wait_timeout\": \"30s\"
    }" > /dev/null 2>&1 && echo "    Granted USE CATALOG" || echo "    WARNING: Could not grant USE CATALOG"
    echo ""
fi

# Step 5: Genie space setup (if requested)
if [[ "$SETUP_GENIE" == true ]]; then
    echo "[+] Setting up Genie spaces..."
    echo "    Running genie_setup_job (creates spaces, binds resources, applies enrichment)..."
    databricks bundle run genie_setup_job -t "$TARGET"
    echo "    Genie spaces configured."
    echo ""
    echo "    Re-deploying app to pick up valueFrom resource bindings..."
    databricks apps deploy "$APP_NAME" \
        --source-code-path "${WORKSPACE_PATH}" \
        --profile "$PROFILE"
    echo "    App re-deployed with updated Genie bindings."
    echo ""
fi

echo "=========================================="
echo "  Deploy complete!"
echo "  App URL will be available shortly."
echo "=========================================="

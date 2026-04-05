#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# push_to_vm.sh  — Transfer WIRP files to Oracle VM and run setup
#
# Usage (from your Windows machine in Git Bash / WSL):
#   bash push_to_vm.sh
#
# Or with a custom key path:
#   KEY=/path/to/key.key bash push_to_vm.sh
# ─────────────────────────────────────────────────────────────────────────────
set -e

KEY="${KEY:-$HOME/Downloads/ssh-key-2026-02-11.key}"
VM="ubuntu@143.47.255.169"
REMOTE_DIR="~/wirp"
LOCAL_DIR="$(cd "$(dirname "$0")" && pwd)"

SSH_OPTS=(-i "$KEY" -o StrictHostKeyChecking=no)

echo "=== Pushing WIRP to Oracle VM ==="
echo "Key  : $KEY"
echo "VM   : $VM"
echo "Local: $LOCAL_DIR"
echo ""

# Create remote directory
ssh "${SSH_OPTS[@]}" "$VM" "mkdir -p $REMOTE_DIR"

# Transfer files
echo "Transferring files..."
scp "${SSH_OPTS[@]}" \
    "$LOCAL_DIR/wirp.html" \
    "$LOCAL_DIR/update_data.py" \
    "$LOCAL_DIR/deploy_vm.sh" \
    "$VM:$REMOTE_DIR/"

echo "Running deployment script on VM..."
ssh "${SSH_OPTS[@]}" "$VM" "chmod +x $REMOTE_DIR/deploy_vm.sh && bash $REMOTE_DIR/deploy_vm.sh"

echo ""
echo "Done. Visit: http://143.47.255.169"

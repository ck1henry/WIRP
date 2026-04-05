#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# WIRP — Oracle VM Deployment Script
# Run this ON the VM after transferring files:
#   ssh ubuntu@143.47.255.169 "bash ~/wirp/deploy_vm.sh"
# ─────────────────────────────────────────────────────────────────────────────
set -e

WIRP_DIR="$HOME/wirp"
WEB_ROOT="/var/www/wirp"
CRON_TIME="0 * * * *"   # every hour

echo "=== WIRP VM Setup ==="

# ── 1. System packages ────────────────────────────────────────────────────────
echo "[1/6] Installing system packages..."
sudo apt-get update -q
sudo apt-get install -y -q nginx python3 python3-pip python3-venv

# ── 2. Python virtual environment ─────────────────────────────────────────────
echo "[2/6] Setting up Python virtualenv..."
python3 -m venv "$WIRP_DIR/venv"
"$WIRP_DIR/venv/bin/pip" install --quiet --upgrade pip
"$WIRP_DIR/venv/bin/pip" install --quiet requests beautifulsoup4 python-docx openpyxl yfinance websocket-client

# ── 3. Web root ───────────────────────────────────────────────────────────────
echo "[3/6] Setting up web root..."
sudo mkdir -p "$WEB_ROOT"
sudo cp "$WIRP_DIR/wirp.html" "$WEB_ROOT/index.html"
sudo chown -R www-data:www-data "$WEB_ROOT"

# ── 4. Nginx config ───────────────────────────────────────────────────────────
echo "[4/6] Configuring nginx..."
sudo tee /etc/nginx/sites-available/wirp > /dev/null << 'NGINX'
server {
    listen 80 default_server;
    listen [::]:80 default_server;
    server_name _;

    root /var/www/wirp;
    index index.html;

    location / {
        try_files $uri $uri/ =404;
        add_header Cache-Control "no-cache, must-revalidate";
    }

    # Serve wirp_data.json for debugging
    location /data {
        alias /home/ubuntu/wirp/wirp_data.json;
        add_header Content-Type application/json;
    }

    # Health check
    location /health {
        return 200 "ok\n";
        add_header Content-Type text/plain;
    }
}
NGINX

sudo ln -sf /etc/nginx/sites-available/wirp /etc/nginx/sites-enabled/wirp
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t
sudo systemctl enable nginx
sudo systemctl restart nginx

# ── 5. Update wrapper script ──────────────────────────────────────────────────
echo "[5/6] Creating update wrapper..."
tee "$WIRP_DIR/run_update.sh" > /dev/null << SCRIPT
#!/usr/bin/env bash
# Runs update_data.py then copies the updated HTML to the web root
set -e
cd "$WIRP_DIR"
"$WIRP_DIR/venv/bin/python" update_data.py
sudo cp "$WIRP_DIR/wirp.html" "$WEB_ROOT/index.html"
sudo chown www-data:www-data "$WEB_ROOT/index.html"
echo "WIRP updated at \$(date -u)" >> "$WIRP_DIR/wirp_update.log"
SCRIPT
chmod +x "$WIRP_DIR/run_update.sh"

# Allow ubuntu to copy to web root without password prompt
echo "ubuntu ALL=(ALL) NOPASSWD: /bin/cp $WIRP_DIR/wirp.html $WEB_ROOT/index.html, /bin/chown www-data\\:www-data $WEB_ROOT/index.html" | sudo tee /etc/sudoers.d/wirp-update
sudo chmod 440 /etc/sudoers.d/wirp-update

# ── 6. Cron job ───────────────────────────────────────────────────────────────
echo "[6/6] Installing cron job ($CRON_TIME UTC)..."
(crontab -l 2>/dev/null | grep -v "run_update.sh"; \
 echo "$CRON_TIME $WIRP_DIR/run_update.sh >> $WIRP_DIR/wirp_cron.log 2>&1") | crontab -

# Run once immediately to populate fresh data
echo "Running initial data update..."
"$WIRP_DIR/run_update.sh" || echo "Initial update had errors — check wirp_update.log"

echo ""
echo "=== Setup complete ==="
echo "Dashboard: http://143.47.255.169"
echo "Health:    http://143.47.255.169/health"
echo "Data JSON: http://143.47.255.169/data"
echo "Cron:      $CRON_TIME UTC daily"
echo "Logs:      $WIRP_DIR/wirp_update.log"

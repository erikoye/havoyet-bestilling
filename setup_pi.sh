#!/bin/bash
# ════════════════════════════════════════════════════════
#  Havøyet — Raspberry Pi oppsett
#  Kjøres automatisk fra Mac via "Sett opp Pi.command"
#  Kan også kjøres manuelt: sudo bash setup_pi.sh
# ════════════════════════════════════════════════════════
set -e

APPDIR="/home/pi/havoyet"

echo ""
echo "╔══════════════════════════════════════════╗"
echo "║   Havøyet Pi-oppsett starter...          ║"
echo "╚══════════════════════════════════════════╝"
echo ""

# ── 1. System ────────────────────────────────────────
echo "▸ Oppdaterer system..."
apt-get update -qq
apt-get install -y -qq python3 python3-pip cups cups-client avahi-daemon ufw curl

# ── 2. Python-pakker ─────────────────────────────────
echo "▸ Installerer Python-pakker..."
pip3 install flask flask-cors requests --quiet --break-system-packages 2>/dev/null \
  || pip3 install flask flask-cors requests --quiet

# ── 3. CUPS + AirPrint ───────────────────────────────
echo "▸ Setter opp CUPS og AirPrint..."
usermod -aG lpadmin pi
cupsctl --remote-admin --remote-any --share-printers
systemctl enable cups avahi-daemon
systemctl restart cups avahi-daemon

# Auto-legg til Brother QL-1110NWB hvis den er tilkoblet via USB
echo "▸ Leter etter Brother-skriver på USB..."
sleep 2
BROTHER_URI=$(lpinfo -v 2>/dev/null | grep -i "brother\|QL-1110\|QL1110" | awk '{print $2}' | head -1)
if [ -n "$BROTHER_URI" ]; then
    echo "  Fant skriver: $BROTHER_URI"
    lpadmin -p Havøyet-etiketter -E \
            -v "$BROTHER_URI" \
            -m everywhere \
            -D "Havøyet etikettskriver" \
            -o printer-is-shared=true 2>/dev/null || true
    echo "  ✔ Skriver lagt til og delt (AirPrint)"
else
    echo "  ⚠ Fant ikke skriveren på USB ennå."
    echo "    Sørg for at Brother QL-1110NWB er koblet til og slått på."
    echo "    Legg til manuelt på: http://$(hostname -I | awk '{print $1}'):631"
fi

# ── 4. App-mappe og rettigheter ──────────────────────
echo "▸ Setter opp appmappen..."
mkdir -p "$APPDIR"
chown -R pi:pi "$APPDIR"

# ── 5. systemd-tjeneste ──────────────────────────────
echo "▸ Installerer systemd-tjeneste..."
cat > /etc/systemd/system/havoyet.service << 'SERVICE'
[Unit]
Description=Havøyet Bestillingsside (Flask)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=pi
WorkingDirectory=/home/pi/havoyet
ExecStart=/usr/bin/python3 /home/pi/havoyet/app.py
Restart=always
RestartSec=5
EnvironmentFile=/home/pi/havoyet/.env
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
SERVICE

systemctl daemon-reload
systemctl enable havoyet
systemctl restart havoyet 2>/dev/null || true
echo "  ✔ Flask starter automatisk ved oppstart"

# ── 5b. Print-worker (henter jobber fra Render og skriver lokalt) ──
echo "▸ Installerer print-worker..."
cat > /etc/systemd/system/havoyet-printer.service << 'PWSERVICE'
[Unit]
Description=Havøyet etikett-print worker (poller bestilling.havoyet.no)
After=network-online.target cups.service
Wants=network-online.target

[Service]
Type=simple
User=pi
WorkingDirectory=/home/pi/havoyet
ExecStart=/usr/bin/python3 /home/pi/havoyet/print_worker.py
Restart=always
RestartSec=5
EnvironmentFile=-/home/pi/havoyet/.env.printer
Environment=PRINT_API_BASE=https://bestilling.havoyet.no
Environment=PRINTER_NAME=brother-ql1110
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
PWSERVICE

# Lag tom env-fil hvis den ikke finnes — brukeren legger inn PRINT_WORKER_TOKEN her
if [ ! -f /home/pi/havoyet/.env.printer ]; then
    cat > /home/pi/havoyet/.env.printer << 'ENVFILE'
# Sett PRINT_WORKER_TOKEN til samme verdi som er konfigurert på Render.
# Hvis tom: auth deaktivert (kun OK i utvikling).
PRINT_WORKER_TOKEN=
ENVFILE
    chown pi:pi /home/pi/havoyet/.env.printer
    chmod 600 /home/pi/havoyet/.env.printer
fi

systemctl enable havoyet-printer
systemctl restart havoyet-printer 2>/dev/null || true
echo "  ✔ Print-worker starter automatisk (poller bestilling.havoyet.no)"

# ── 6. Tailscale ─────────────────────────────────────
echo "▸ Installerer Tailscale..."
if ! command -v tailscale &>/dev/null; then
    curl -fsSL https://tailscale.com/install.sh | sh -s -- --accept-tos 2>/dev/null || true
fi

# Start Tailscale (ikke-blokkerende — bruker logger inn selv etterpå)
tailscale up --accept-routes 2>/dev/null || true
echo "  ✔ Tailscale installert"

# ── 7. Brannmur ──────────────────────────────────────
echo "▸ Konfigurerer brannmur..."
ufw allow ssh     comment "SSH"    2>/dev/null || true
ufw allow 5001    comment "Havøyet Flask" 2>/dev/null || true
ufw allow 631     comment "CUPS"   2>/dev/null || true
ufw --force enable 2>/dev/null || true

# ── Ferdig ───────────────────────────────────────────
PI_IP=$(hostname -I | awk '{print $1}')
echo ""
echo "╔════════════════════════════════════════════════════╗"
echo "║   ✅ Oppsett fullført!                              ║"
echo "╠════════════════════════════════════════════════════╣"
echo "║                                                    ║"
printf "║  iPad / lokalt:  http://%-27s ║\n" "$PI_IP:5001"
echo "║  Mac utenfra:    Kjør «sudo tailscale up» på Pi    ║"
echo "║                  logg inn — bruk Tailscale-IP      ║"
printf "║  Skriveradmin:   http://%-27s ║\n" "$PI_IP:631"
echo "║                                                    ║"
echo "╚════════════════════════════════════════════════════╝"
echo ""

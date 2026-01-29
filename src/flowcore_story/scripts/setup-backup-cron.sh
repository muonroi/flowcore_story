#!/bin/bash
#
# Setup automated backup schedule
# Run this once to configure automatic backups
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKUP_SCRIPT="${SCRIPT_DIR}/backup.sh"
LOG_DIR="/var/log/storyflow"
BACKUP_ROOT="/home/storyflow-backups"

GREEN='\033[0;32m'
NC='\033[0m'

echo -e "${GREEN}Setting up StoryFlow automatic backups...${NC}"

# Create directories
sudo mkdir -p "${LOG_DIR}"
sudo mkdir -p "${BACKUP_ROOT}"
sudo chown $(whoami):$(whoami) "${BACKUP_ROOT}"

# Create systemd service for backup
sudo tee /etc/systemd/system/storyflow-backup.service > /dev/null << EOF
[Unit]
Description=StoryFlow Backup Service
After=network.target mariadb.service postgresql.service

[Service]
Type=oneshot
User=root
Environment="BACKUP_ROOT=${BACKUP_ROOT}"
ExecStart=${BACKUP_SCRIPT}
StandardOutput=append:${LOG_DIR}/backup.log
StandardError=append:${LOG_DIR}/backup.log

[Install]
WantedBy=multi-user.target
EOF

# Create systemd timer (runs every 6 hours)
sudo tee /etc/systemd/system/storyflow-backup.timer > /dev/null << EOF
[Unit]
Description=Run StoryFlow backup every 6 hours

[Timer]
# Run at 00:00, 06:00, 12:00, 18:00 daily
OnCalendar=*-*-* 00,06,12,18:00:00
# Also run 5 minutes after boot
OnBootSec=5min
Persistent=true
RandomizedDelaySec=300

[Install]
WantedBy=timers.target
EOF

# Create logrotate config
sudo tee /etc/logrotate.d/storyflow-backup > /dev/null << EOF
${LOG_DIR}/backup.log {
    daily
    rotate 14
    compress
    delaycompress
    missingok
    notifempty
    create 644 root root
}
EOF

# Reload systemd and enable timer
sudo systemctl daemon-reload
sudo systemctl enable storyflow-backup.timer
sudo systemctl start storyflow-backup.timer

echo ""
echo -e "${GREEN}✓ Backup automation configured!${NC}"
echo ""
echo "Schedule: Every 6 hours (00:00, 06:00, 12:00, 18:00)"
echo "Backup location: ${BACKUP_ROOT}"
echo "Log file: ${LOG_DIR}/backup.log"
echo ""
echo "Commands:"
echo "  - Check timer status:  systemctl status storyflow-backup.timer"
echo "  - Run backup now:      systemctl start storyflow-backup.service"
echo "  - View logs:           journalctl -u storyflow-backup.service"
echo "  - Disable timer:       systemctl disable storyflow-backup.timer"
echo ""

# Show timer status
systemctl status storyflow-backup.timer --no-pager

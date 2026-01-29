#!/bin/bash

echo "========================================================"
echo "       STORYFLOW DEEP HEALTH CHECK (THỰC CHIẾN)"
echo "========================================================"

echo ""
echo "🔍 [1/3] KIỂM TRA LOGS CÁC CONTAINER QUAN TRỌNG (Last 10 lines)"
echo "--------------------------------------------------------"

echo "👉 database-sync-worker:"
docker logs --tail 10 database-sync-worker 2>&1 || echo "Container not found"

echo ""
echo "👉 crawler-consumer:"
docker logs --tail 10 crawler-consumer 2>&1 || echo "Container not found"

echo ""
echo "👉 challenge-harvester:"
docker logs --tail 10 challenge-harvester 2>&1 || echo "Container not found"

echo ""
echo "🔍 [2/3] KIỂM TRA DỮ LIỆU DATABASE THỰC TẾ"
echo "--------------------------------------------------------"
# Đảm bảo script check db có trong container
if [ -f "/home/storyflow-core/check_db_state_v2.py" ]; then
    docker cp /home/storyflow-core/check_db_state_v2.py database-sync-worker:/app/check_db_state_v2.py >/dev/null 2>&1
    docker exec database-sync-worker python /app/check_db_state_v2.py 2>&1 || echo "Failed to exec db check script"
else
    echo "⚠️ Không tìm thấy file check_db_state_v2.py trên host."
fi

echo ""
echo "🔍 [3/3] KIỂM TRA TÀI NGUYÊN HỆ THỐNG"
echo "--------------------------------------------------------"
DISK=$(df -h / | tail -1 | awk '{print $5}')
MEM=$(free -m | awk 'NR==2{printf "%.2f%%", $3*100/$2 }')
echo "Disk Usage: $DISK"
echo "Memory Usage: $MEM"

echo ""
echo "========================================================"
echo "✅ KIỂM TRA HOÀN TẤT"

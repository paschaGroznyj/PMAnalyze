#!/usr/bin/env bash
set -euo pipefail

# Recreate pmanalyze-18021 with explicit Kafka env for catchpm_net
# Usage:
#   bash deploy/recreate_pmanalyze_18021.sh
#   IMAGE_TAG=pmanalyze:v24-kafkafix bash deploy/recreate_pmanalyze_18021.sh
#   KAFKA_IP=172.28.0.10:9092 bash deploy/recreate_pmanalyze_18021.sh

NAME="${NAME:-pmanalyze-18021}"
IMAGE_TAG="${IMAGE_TAG:-pmanalyze:v24-kafkafix}"
NETWORK="${NETWORK:-catchpm_net}"
HOST_PORT="${HOST_PORT:-18021}"
APP_PORT="${APP_PORT:-8000}"
KAFKA_IP="${KAFKA_IP:-172.28.0.10:9092}"
ENV_TMP="/tmp/${NAME}.env"

echo "[1/6] Build image ${IMAGE_TAG} from current workspace"
docker build -t "${IMAGE_TAG}" .

echo "[2/6] Prepare env file"
if docker ps -a --format '{{.Names}}' | grep -q "^${NAME}$"; then
  docker inspect "${NAME}" --format '{{range .Config.Env}}{{println .}}{{end}}' > "${ENV_TMP}"
else
  : > "${ENV_TMP}"
fi

python3 - <<PY
from pathlib import Path
p=Path('${ENV_TMP}')
lines=[x.strip() for x in p.read_text().splitlines() if x.strip() and '=' in x]
kv={}
for ln in lines:
    k,v=ln.split('=',1)
    kv[k]=v
kv['KAFKA_BROKER']='${KAFKA_IP}'
kv['KAFKA_BROKER_DIND']='${KAFKA_IP}'
kv['CATCHPM_KAFKA_BOOTSTRAP']='${KAFKA_IP}'
kv.setdefault('CATCHPM_BRIDGE_ENABLED','1')
kv.setdefault('CATCHPM_CHAT_TOPIC','chat-messages')
kv.setdefault('CATCHPM_TASK_EVENTS_TOPIC','task-events')
out='\n'.join(f'{k}={v}' for k,v in sorted(kv.items()))+'\n'
p.write_text(out)
print('env entries:', len(kv))
PY

echo "[3/6] Remove old container (if exists)"
if docker ps -a --format '{{.Names}}' | grep -q "^${NAME}$"; then
  docker rm -f "${NAME}" >/dev/null
fi

echo "[4/6] Run container"
docker run -d \
  --name "${NAME}" \
  --restart unless-stopped \
  --network "${NETWORK}" \
  --env-file "${ENV_TMP}" \
  -p "${HOST_PORT}:${APP_PORT}" \
  "${IMAGE_TAG}" \
  uvicorn main:app --host 0.0.0.0 --port "${APP_PORT}" >/dev/null

echo "[5/6] Wait for readiness"
python3 - <<'PY'
import time, urllib.request
url='http://127.0.0.1:18021/health'
for i in range(30):
    try:
        with urllib.request.urlopen(url, timeout=2) as r:
            print('health', r.status)
            break
    except Exception:
        time.sleep(1)
else:
    print('health check failed on host; may still be reachable inside catchpm_net')
PY

echo "[6/6] Report"
docker ps --format '{{.Names}} | {{.Status}} | {{.Image}} | {{.Ports}}' | grep "^${NAME}"
docker inspect "${NAME}" --format '{{.Created}}'
docker exec "${NAME}" env | grep -Ei 'kafka|catchpm' | sort || true

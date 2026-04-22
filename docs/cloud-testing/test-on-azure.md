# Testing polyqueue on Azure

End-to-end playbook for provisioning a throwaway polyqueue test stack on Azure,
running the worker in Azure Container Instances (ACI) against Postgres
Flexible Server + Service Bus, and tearing everything down again.

## Resources you will create

| Resource | SKU | Est. cost (24/7) |
|---|---|---|
| PostgreSQL Flexible Server | `Standard_B1ms` (1 vCore, 2 GB) + 32 GB Premium_LRS, no HA | ~$15/mo |
| Service Bus | Basic namespace, 1 queue | ~$0 at test volume |
| Azure Container Registry (ACR) | Basic ($0.167/day) | ~$5/mo |
| Azure Container Instances (ACI) | 0.25 vCPU / 0.5 GB, 24/7 | ~$1/mo |
| Log Analytics | default | ~$0 (under free tier at test volume) |
| Networking | public with firewall | $0 |

## Prerequisites

- `az` CLI, logged in (`az account show`).
- `podman` (or `docker`) with a running VM.
- Python 3.12 + `uv` for `enqueue.py` from your laptop.

Azure CLI 2.85+ is required — 2.84 shipped with broken Python deps
(`jmespath` / `_error_format` module missing). If you hit that, run
`brew reinstall azure-cli` (or your platform's equivalent).

## 1. Pre-flight: env, resource group, password

Pick a region (`koreacentral` if you're in Korea; `eastus` / `westeurope`
have similar pricing).

```bash
mkdir -p .az-test
cat > .az-test/env.sh <<'EOF'
export LOCATION=koreacentral
export RG=pq-test-rg
export PREFIX=pqtest             # no hyphens — ACR/Service Bus forbid them
export MY_IP=$(curl -s https://checkip.amazonaws.com)
export DB_PASS=$(openssl rand -base64 24 | tr -d '+/=' | head -c 24)
export DB_USER=polyqueue
export DB_NAME=polyqueue
export PG_SERVER=${PREFIX}-pg
export SB_NS=${PREFIX}-bus       # suffix '-sb' is Azure-reserved
export SB_QUEUE=jobs
export ACR_NAME=${PREFIX}acr
export ACI_NAME=${PREFIX}-worker
EOF
chmod 600 .az-test/env.sh
source .az-test/env.sh

az group create --name $RG --location $LOCATION
```

## 2. Create ACR, Service Bus, Postgres (in parallel)

ACR and Service Bus return in seconds. Postgres takes ~5 min. Start all
three concurrently.

```bash
# ACR (Basic, admin enabled so podman can log in with username/password)
az acr create --name $ACR_NAME --resource-group $RG --location $LOCATION \
  --sku Basic --admin-enabled true &

# Service Bus Basic + queue
(
  az servicebus namespace create --name $SB_NS --resource-group $RG \
    --location $LOCATION --sku Basic
  az servicebus queue create --name $SB_QUEUE --namespace-name $SB_NS \
    --resource-group $RG
) &

# Postgres Flexible Server
az postgres flexible-server create \
  --name $PG_SERVER --resource-group $RG --location $LOCATION \
  --sku-name Standard_B1ms --tier Burstable \
  --version 17 \
  --storage-size 32 --storage-type Premium_LRS \
  --zonal-resiliency Disabled \
  --backup-retention 7 \
  --admin-user $DB_USER --admin-password "$DB_PASS" \
  --public-access 0.0.0.0 \
  --yes &

wait
```

If Service Bus fails with `Namespace with suffix '-sb' is reserved`, the env
file already uses `-bus`. If Postgres complains that `--database-name` is
invalid, that flag was removed in newer `az` versions — create the database
separately below.

## 3. Create the polyqueue database and fetch endpoints

```bash
az postgres flexible-server db create --server-name $PG_SERVER \
  --resource-group $RG --database-name $DB_NAME

PG_HOST=$(az postgres flexible-server show --name $PG_SERVER \
  --resource-group $RG --query 'fullyQualifiedDomainName' --output tsv)

SB_CONN=$(az servicebus namespace authorization-rule keys list \
  --namespace-name $SB_NS --resource-group $RG \
  --name RootManageSharedAccessKey \
  --query primaryConnectionString --output tsv)

ACR_LOGIN=$(az acr credential show --name $ACR_NAME --query username --output tsv)
ACR_PW=$(az acr credential show --name $ACR_NAME --query 'passwords[0].value' --output tsv)
ACR_SERVER=$(az acr show --name $ACR_NAME --query loginServer --output tsv)

cat >> .az-test/env.sh <<EOF
export PG_HOST=$PG_HOST
export SB_CONN='$SB_CONN'
export ACR_LOGIN=$ACR_LOGIN
export ACR_PW='$ACR_PW'
export ACR_SERVER=$ACR_SERVER
EOF
source .az-test/env.sh
```

## 4. Postgres firewall — allow your laptop

Flexible Server's `--public-access 0.0.0.0` on create only allows Azure
services. Add your laptop's IP explicitly:

```bash
az postgres flexible-server firewall-rule create \
  --name $PG_SERVER --resource-group $RG \
  --rule-name my-laptop \
  --start-ip-address $MY_IP --end-ip-address $MY_IP
```

## 5. Build + push the image

The `Dockerfile` at the repo root installs polyqueue with `[all-queues]`
so the same image covers Azure Service Bus / SQS / Redis / PGMQ.

```bash
echo "$ACR_PW" | podman login $ACR_SERVER --username $ACR_LOGIN --password-stdin
podman build --platform linux/amd64 -t $ACR_SERVER/pq-test:latest .
podman push $ACR_SERVER/pq-test:latest
```

## 6. Create the ACI worker

Secure env vars (`--secure-environment-variables`) aren't visible in
`az container show`; plain env vars are.

```bash
az container create \
  --name $ACI_NAME --resource-group $RG --location $LOCATION \
  --image $ACR_SERVER/pq-test:latest \
  --cpu 0.25 --memory 0.5 \
  --os-type Linux --restart-policy OnFailure \
  --registry-login-server $ACR_SERVER \
  --registry-username $ACR_LOGIN --registry-password "$ACR_PW" \
  --environment-variables \
      POLYQUEUE_BACKEND=azure_service_bus \
      POLYQUEUE_TABLE_SCHEMA=polyqueue \
      POLYQUEUE_TABLE_PREFIX=polyqueue \
      POLYQUEUE_AZURE_SB_QUEUE_NAME=$SB_QUEUE \
      POLYQUEUE_HEARTBEAT_INTERVAL=30 \
      PYTHONUNBUFFERED=1 \
  --secure-environment-variables \
      POLYQUEUE_DB_URL="postgresql+asyncpg://$DB_USER:$DB_PASS@$PG_HOST:5432/$DB_NAME?ssl=require" \
      POLYQUEUE_AZURE_SB_CONNECTION_STRING="$SB_CONN"
```

## 7. Smoke test — watch the worker come up

```bash
# State (Running as soon as the process starts; exit=1 if it crashed)
az container show --name $ACI_NAME --resource-group $RG \
  --query 'instanceView.state' --output tsv

# Logs
az container logs --name $ACI_NAME --resource-group $RG | tail -30
```

Expect to see `Jobs/workers tables ready`, `queue: using AzureServiceBusJobQueue`,
and `worker: starting up`. The azure-servicebus SDK emits a lot of `LinkState`
log lines — that's normal.

## 8. Enqueue from your laptop

```bash
source .az-test/env.sh
cd tools/demo

export POLYQUEUE_BACKEND=azure_service_bus
export POLYQUEUE_TABLE_SCHEMA=polyqueue POLYQUEUE_TABLE_PREFIX=polyqueue
export POLYQUEUE_AZURE_SB_QUEUE_NAME=$SB_QUEUE
export POLYQUEUE_AZURE_SB_CONNECTION_STRING="$SB_CONN"
export POLYQUEUE_DB_URL="postgresql+asyncpg://$DB_USER:$DB_PASS@$PG_HOST:5432/$DB_NAME?ssl=require"

uv run enqueue.py
```

Expected: one tracked add job transitions to `succeeded attempt=1/3` with a
populated `worker=SandboxHost-...` field.

## 9. Dashboard (optional)

```bash
cd ts/tools/dashboard
bun install && bun run build
# Bun SQL needs sslmode=require on the URL.
export POLYQUEUE_DB_URL="postgres://$DB_USER:$DB_PASS@$PG_HOST:5432/$DB_NAME?sslmode=require"
export POLYQUEUE_TABLE_SCHEMA=polyqueue POLYQUEUE_TABLE_PREFIX=polyqueue
bun run start
```

The dashboard reads the same env vars as the Python code for table name
derivation, so both `POLYQUEUE_TABLE_SCHEMA` and `POLYQUEUE_TABLE_PREFIX`
must match the worker's settings.

## 10. Useful admin commands

```bash
# Live-tail logs (Azure's version of CloudWatch tail)
az container logs --name $ACI_NAME --resource-group $RG --follow

# Restart the worker (force re-pull after a new image push)
az container delete --name $ACI_NAME --resource-group $RG --yes
az container create ...   # same create command as above

# Multiple workers
# ACI has no built-in scaling. Create additional container groups:
az container create --name ${PREFIX}-worker-2 ... (same env vars)
# Or move to Azure Container Apps if you want real scale-to-N.

# Queue depth
az servicebus queue show --name $SB_QUEUE --namespace-name $SB_NS \
  --resource-group $RG --query 'messageCount'

# Refresh Postgres firewall on IP change
NEW_IP=$(curl -s https://checkip.amazonaws.com)
az postgres flexible-server firewall-rule delete --name $PG_SERVER \
  --resource-group $RG --rule-name my-laptop --yes
az postgres flexible-server firewall-rule create --name $PG_SERVER \
  --resource-group $RG --rule-name my-laptop \
  --start-ip-address $NEW_IP --end-ip-address $NEW_IP
```

## 11. Teardown

One command destroys every resource (Azure's resource-group model makes
this drastically simpler than AWS):

```bash
az group delete --name pq-test-rg --yes --no-wait

# Poll until gone (runs in background on Azure's side, ~2-5 min)
for i in $(seq 1 20); do
  EXISTS=$(az group exists --name pq-test-rg)
  [ "$EXISTS" = "false" ] && break
  sleep 15
done

rm -rf .az-test
```

### Verify clean

```bash
az group list --query "[?name=='pq-test-rg'].name" --output tsv   # empty
az resource list --query "[?contains(name,'pqtest') || contains(name,'pq-test')].name" --output tsv   # empty
```

## Known gotchas

- **Service Bus namespace suffix `-sb` is reserved.** Use `-bus` or anything else.
- **`az postgres flexible-server create --database-name`** was deprecated in
  CLI 2.85 — create the DB with a separate `db create` call afterwards.
- **ACI container logs are empty when the container dies fast.** If
  `az container show` shows `ExitCode 1` with no log output, reproduce
  locally with `podman run` and the same env vars to get a real traceback.
  Also set `PYTHONUNBUFFERED=1` so stdout isn't buffered past the crash.
- **ACR admin-enabled** is the simplest auth path. For production, use a
  managed identity on the ACI group instead of plaintext registry creds.
- **Postgres Flexible Server `--public-access 0.0.0.0`** only allows Azure
  services, not the public internet. Your laptop needs an explicit firewall
  rule (`firewall-rule create`), just like AWS SG inbound rules.

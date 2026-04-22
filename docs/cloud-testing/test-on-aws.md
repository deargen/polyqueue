# Testing polyqueue on AWS

End-to-end playbook for provisioning a throwaway polyqueue test stack on AWS,
running the worker in ECS Fargate against RDS Postgres + SQS, and tearing
everything down again.

## Resources you will create

| Resource | SKU | Est. cost (24/7) |
|---|---|---|
| RDS Postgres | `db.t4g.micro`, 20 GB gp3, single-AZ, no backups, public | ~$15/mo |
| SQS | standard queue, visibility 660 s | ~$0 at test volume |
| ECR | 1 private repo | ~$0 at test volume |
| ECS Fargate | 1 task, 0.25 vCPU / 0.5 GB, 24/7 | ~$8.60/mo |
| CloudWatch Logs | 3-day retention | ~$0 |
| IAM / SG / networking | default VPC, no NAT | $0 |

## Prerequisites

- `aws` CLI logged in (`aws sts get-caller-identity`).
- `podman` (or `docker`) with a running VM.
- Default VPC present in your chosen region.
- Python 3.12 + `uv` for running `enqueue.py` from your laptop.

## 1. Pre-flight: env, VPC, IP, password

Pick a region (the Korea user who wrote this used `ap-northeast-2`; `us-east-1`
is ~30 % cheaper if latency doesn't matter).

```bash
mkdir -p .aws-test
cat > .aws-test/env.sh <<'EOF'
export REGION=ap-northeast-2
export ACCT=$(aws sts get-caller-identity --query Account --output text)
export VPC_ID=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true \
  --region $REGION --query 'Vpcs[0].VpcId' --output text)
export SUBNETS=$(aws ec2 describe-subnets \
  --filters Name=vpc-id,Values=$VPC_ID Name=default-for-az,Values=true \
  --region $REGION --query 'Subnets[].SubnetId' --output text | tr '\t' ',')
export MY_IP=$(curl -s https://checkip.amazonaws.com)
export DB_PASS=$(openssl rand -base64 24 | tr -d '+/=' | head -c 24)
export DB_USER=polyqueue
export DB_NAME=polyqueue
export PREFIX=pq-test
EOF
chmod 600 .aws-test/env.sh
source .aws-test/env.sh
```

## 2. Security groups

```bash
DB_SG=$(aws ec2 create-security-group --group-name ${PREFIX}-db-sg \
  --description "RDS for polyqueue test" --vpc-id $VPC_ID --region $REGION \
  --query 'GroupId' --output text)
TASK_SG=$(aws ec2 create-security-group --group-name ${PREFIX}-task-sg \
  --description "ECS task for polyqueue test" --vpc-id $VPC_ID --region $REGION \
  --query 'GroupId' --output text)

# DB ingress: from your laptop + from the ECS task SG
aws ec2 authorize-security-group-ingress --group-id $DB_SG --region $REGION \
  --protocol tcp --port 5432 --cidr ${MY_IP}/32
aws ec2 authorize-security-group-ingress --group-id $DB_SG --region $REGION \
  --protocol tcp --port 5432 --source-group $TASK_SG

echo "export DB_SG=$DB_SG"   >> .aws-test/env.sh
echo "export TASK_SG=$TASK_SG" >> .aws-test/env.sh
```

## 3. ECR, SQS, CloudWatch log group

```bash
ECR_URI=$(aws ecr create-repository --repository-name ${PREFIX} --region $REGION \
  --image-scanning-configuration scanOnPush=false \
  --query 'repository.repositoryUri' --output text)

QUEUE_URL=$(aws sqs create-queue --queue-name ${PREFIX}-jobs \
  --attributes VisibilityTimeout=660 \
  --region $REGION --query 'QueueUrl' --output text)
QUEUE_ARN=$(aws sqs get-queue-attributes --queue-url $QUEUE_URL \
  --attribute-names QueueArn --region $REGION \
  --query 'Attributes.QueueArn' --output text)

aws logs create-log-group --log-group-name /ecs/${PREFIX} --region $REGION
aws logs put-retention-policy --log-group-name /ecs/${PREFIX} \
  --retention-in-days 3 --region $REGION

cat >> .aws-test/env.sh <<EOF
export ECR_URI=$ECR_URI
export QUEUE_URL=$QUEUE_URL
export QUEUE_ARN=$QUEUE_ARN
EOF
```

## 4. IAM roles

Two roles: one for ECS to pull from ECR / write logs, one for the task itself
to talk to SQS.

```bash
TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ecs-tasks.amazonaws.com"},"Action":"sts:AssumeRole"}]}'

aws iam create-role --role-name ${PREFIX}-exec-role \
  --assume-role-policy-document "$TRUST"
aws iam attach-role-policy --role-name ${PREFIX}-exec-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy

aws iam create-role --role-name ${PREFIX}-task-role \
  --assume-role-policy-document "$TRUST"
SQS_POLICY=$(cat <<EOF
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["sqs:SendMessage","sqs:ReceiveMessage","sqs:DeleteMessage","sqs:ChangeMessageVisibility","sqs:GetQueueAttributes","sqs:GetQueueUrl"],"Resource":"$QUEUE_ARN"}]}
EOF
)
aws iam put-role-policy --role-name ${PREFIX}-task-role \
  --policy-name sqs-access --policy-document "$SQS_POLICY"

EXEC_ROLE_ARN=$(aws iam get-role --role-name ${PREFIX}-exec-role --query 'Role.Arn' --output text)
TASK_ROLE_ARN=$(aws iam get-role --role-name ${PREFIX}-task-role --query 'Role.Arn' --output text)
echo "export EXEC_ROLE_ARN=$EXEC_ROLE_ARN" >> .aws-test/env.sh
echo "export TASK_ROLE_ARN=$TASK_ROLE_ARN" >> .aws-test/env.sh
```

## 5. RDS (kick off async — takes ~10 min)

```bash
aws rds create-db-instance \
  --db-instance-identifier ${PREFIX}-db \
  --db-instance-class db.t4g.micro \
  --engine postgres --engine-version 17.9 \
  --master-username $DB_USER --master-user-password "$DB_PASS" \
  --db-name $DB_NAME \
  --allocated-storage 20 --storage-type gp3 \
  --vpc-security-group-ids $DB_SG \
  --backup-retention-period 0 --no-multi-az --publicly-accessible \
  --no-auto-minor-version-upgrade --no-deletion-protection \
  --no-enable-performance-insights --no-copy-tags-to-snapshot \
  --region $REGION
```

## 6. Build and push the image (can run in parallel with RDS create)

The `Dockerfile` at the repo root installs polyqueue with the
`[all-queues]` extra, so the same image works for Redis / SQS / Azure
Service Bus / PGMQ backends.

```bash
aws ecr get-login-password --region $REGION | \
  podman login --username AWS --password-stdin ${ACCT}.dkr.ecr.${REGION}.amazonaws.com

podman build --platform linux/amd64 -t ${ECR_URI}:latest .
podman push ${ECR_URI}:latest
```

## 7. Wait for RDS, create ECS cluster + task def + service

```bash
# Poll until RDS is available
for i in $(seq 1 90); do
  read -r STATUS ENDPOINT <<< "$(aws rds describe-db-instances \
    --db-instance-identifier ${PREFIX}-db --region $REGION \
    --query 'DBInstances[0].[DBInstanceStatus,Endpoint.Address]' --output text)"
  [ "$STATUS" = "available" ] && break
  echo "[$i] $STATUS — waiting"
  sleep 20
done
echo "export RDS_ENDPOINT=$ENDPOINT" >> .aws-test/env.sh
echo "export RDS_PORT=5432" >> .aws-test/env.sh
source .aws-test/env.sh

# ECS cluster
aws ecs create-cluster --cluster-name ${PREFIX} --region $REGION

# Task definition
cat > .aws-test/taskdef.json <<EOF
{
  "family": "${PREFIX}-worker",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "256", "memory": "512",
  "runtimePlatform": {"operatingSystemFamily": "LINUX", "cpuArchitecture": "X86_64"},
  "executionRoleArn": "${EXEC_ROLE_ARN}",
  "taskRoleArn": "${TASK_ROLE_ARN}",
  "containerDefinitions": [{
    "name": "worker",
    "image": "${ECR_URI}:latest",
    "essential": true,
    "environment": [
      {"name": "POLYQUEUE_BACKEND", "value": "sqs"},
      {"name": "POLYQUEUE_TABLE_SCHEMA", "value": "polyqueue"},
      {"name": "POLYQUEUE_TABLE_PREFIX", "value": "polyqueue"},
      {"name": "POLYQUEUE_DB_URL", "value": "postgresql+asyncpg://${DB_USER}:${DB_PASS}@${RDS_ENDPOINT}:5432/${DB_NAME}"},
      {"name": "POLYQUEUE_SQS_QUEUE_URL", "value": "${QUEUE_URL}"},
      {"name": "POLYQUEUE_SQS_REGION", "value": "${REGION}"},
      {"name": "POLYQUEUE_SQS_VISIBILITY_TIMEOUT", "value": "660"},
      {"name": "POLYQUEUE_HEARTBEAT_INTERVAL", "value": "30"}
    ],
    "logConfiguration": {
      "logDriver": "awslogs",
      "options": {
        "awslogs-group": "/ecs/${PREFIX}",
        "awslogs-region": "${REGION}",
        "awslogs-stream-prefix": "worker"
      }
    }
  }]
}
EOF
TD_ARN=$(aws ecs register-task-definition \
  --cli-input-json file://.aws-test/taskdef.json \
  --region $REGION --query 'taskDefinition.taskDefinitionArn' --output text)

# Service
aws ecs create-service \
  --cluster ${PREFIX} \
  --service-name ${PREFIX}-worker \
  --task-definition $TD_ARN \
  --desired-count 1 \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[${SUBNETS}],securityGroups=[${TASK_SG}],assignPublicIp=ENABLED}" \
  --region $REGION
```

## 8. Smoke test — watch the worker come up

```bash
# Wait for 1 running task + 1 deployment
for i in $(seq 1 30); do
  RUN=$(aws ecs describe-services --cluster ${PREFIX} --services ${PREFIX}-worker \
    --region $REGION --query 'services[0].runningCount' --output text)
  [ "$RUN" = "1" ] && break
  sleep 10
done

# Tail the worker logs
aws logs tail /ecs/${PREFIX} --region $REGION --since 2m | head -30
```

You should see `polyqueue.polyqueue_jobs / polyqueue.polyqueue_workers`,
`queue: using SqsJobQueue`, and `worker: starting up`.

## 9. Enqueue from your laptop

```bash
cd tools/demo
export POLYQUEUE_BACKEND=sqs
export POLYQUEUE_TABLE_SCHEMA=polyqueue POLYQUEUE_TABLE_PREFIX=polyqueue
export POLYQUEUE_DB_URL="postgresql+asyncpg://${DB_USER}:${DB_PASS}@${RDS_ENDPOINT}:5432/${DB_NAME}?ssl=require"
export POLYQUEUE_SQS_QUEUE_URL=$QUEUE_URL
export POLYQUEUE_SQS_REGION=$REGION

uv run enqueue.py
```

Expected: one tracked add job transitions to `succeeded attempt=1/3`.

## 10. Dashboard (optional)

```bash
cd ts/tools/dashboard
bun install && bun run build
export POLYQUEUE_DB_URL="postgres://${DB_USER}:${DB_PASS}@${RDS_ENDPOINT}:5432/${DB_NAME}?sslmode=require"
bun run start
```

Bun SQL needs `sslmode=require` because RDS's default parameter group sets
`rds.force_ssl=1`. Open the URL that Bun prints.

## 11. Useful admin commands

```bash
# Scale worker
aws ecs update-service --cluster ${PREFIX} --service ${PREFIX}-worker \
  --desired-count 3 --region $REGION

# Live-tail worker logs
aws logs tail /ecs/${PREFIX} --follow --region $REGION

# Queue depth
aws sqs get-queue-attributes --queue-url $QUEUE_URL \
  --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible \
  --region $REGION

# Refresh SG if your IP changes (e.g. new WiFi)
NEW_IP=$(curl -s https://checkip.amazonaws.com)
aws ec2 authorize-security-group-ingress --group-id $DB_SG --region $REGION \
  --protocol tcp --port 5432 --cidr ${NEW_IP}/32
aws ec2 revoke-security-group-ingress --group-id $DB_SG --region $REGION \
  --protocol tcp --port 5432 --cidr ${MY_IP}/32
```

## 12. Teardown

AWS has no single-call equivalent to Azure's `group delete`. Order matters:
scale the service to 0, delete it, delete the cluster, then RDS (slowest),
then fast resources, then SGs (after RDS releases), then IAM.

```bash
source .aws-test/env.sh

# 1. Drain + delete ECS
aws ecs update-service --cluster ${PREFIX} --service ${PREFIX}-worker \
  --desired-count 0 --region $REGION
for i in $(seq 1 20); do
  RUN=$(aws ecs describe-services --cluster ${PREFIX} --services ${PREFIX}-worker \
    --region $REGION --query 'services[0].runningCount' --output text)
  [ "$RUN" = "0" ] && break
  sleep 10
done
aws ecs delete-service --cluster ${PREFIX} --service ${PREFIX}-worker --region $REGION
aws ecs delete-cluster --cluster ${PREFIX} --region $REGION
TD_ARN=$(aws ecs list-task-definitions --family-prefix ${PREFIX}-worker \
  --region $REGION --query 'taskDefinitionArns[0]' --output text)
aws ecs deregister-task-definition --task-definition "$TD_ARN" --region $REGION

# 2. Start RDS deletion (async, ~5-8 min)
aws rds delete-db-instance --db-instance-identifier ${PREFIX}-db \
  --skip-final-snapshot --region $REGION

# 3. Fast deletes
aws sqs delete-queue --queue-url $QUEUE_URL --region $REGION
aws ecr delete-repository --repository-name ${PREFIX} --force --region $REGION
aws logs delete-log-group --log-group-name /ecs/${PREFIX} --region $REGION

# 4. Wait for RDS to go away (blocks SG delete)
for i in $(seq 1 40); do
  STATUS=$(aws rds describe-db-instances --db-instance-identifier ${PREFIX}-db \
    --region $REGION 2>&1 | grep -oE 'DBInstanceNotFound')
  [ -n "$STATUS" ] && break
  sleep 20
done

# 5. SGs (revoke cross-SG rule first, then delete)
aws ec2 revoke-security-group-ingress --group-id $DB_SG --region $REGION \
  --protocol tcp --port 5432 --source-group $TASK_SG
aws ec2 delete-security-group --group-id $DB_SG --region $REGION
aws ec2 delete-security-group --group-id $TASK_SG --region $REGION

# 6. IAM
aws iam detach-role-policy --role-name ${PREFIX}-exec-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy
aws iam delete-role --role-name ${PREFIX}-exec-role
aws iam delete-role-policy --role-name ${PREFIX}-task-role --policy-name sqs-access
aws iam delete-role --role-name ${PREFIX}-task-role

rm -rf .aws-test
```

### Verify clean

```bash
aws ecs describe-clusters --clusters ${PREFIX} --region $REGION --query 'clusters[0].status'
aws rds describe-db-instances --db-instance-identifier ${PREFIX}-db --region $REGION 2>&1 | grep -oE 'not found'
aws sqs get-queue-url --queue-name ${PREFIX}-jobs --region $REGION 2>&1 | grep -oE 'does not exist'
aws iam list-roles --query "Roles[?contains(RoleName,'${PREFIX}')].RoleName"
```

Every line should show the resource is gone.

## Known gotchas

- **Korean ISPs routing to specific AWS Seoul IP blocks**: we hit this during
  development — RDS's public IP landed in a block KT/LGU+ couldn't route to.
  The ECS worker (inside the VPC) was fine. If `nc -zv <endpoint> 5432`
  times out from your laptop but an AWS-internal probe works, delete +
  recreate the RDS with the same identifier to get a new public IP.
- **RDS reboot keeps the IP**; only `delete-db-instance` + `create-db-instance`
  gives you a new one.
- **Bun SQL** needs `sslmode=require`; asyncpg / SQLAlchemy negotiate TLS
  opportunistically.

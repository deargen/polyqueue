# polyqueue cloud-testing playbooks

End-to-end playbooks for standing up a throwaway polyqueue test stack on a cloud
provider, exercising the worker, and tearing everything down again.

Both playbooks use the cheapest commercially available SKUs, skip HA/backups,
and fit a few dollars per month if left running. Actual cost for a build +
smoke-test + teardown cycle is pennies (~$0.03–$0.25 depending on how long
Postgres is running).

| Playbook | Broker | Compute | Cost (24/7) | Cost (1-hour test) |
|---|---|---|---|---|
| [test-on-aws.md](./test-on-aws.md) | SQS | ECS Fargate | ~$24/mo | ~$0.05 |
| [test-on-azure.md](./test-on-azure.md) | Service Bus | Container Instances | ~$21/mo | ~$0.03 |

Each playbook is self-contained: prerequisites, resource creation, smoke test,
and teardown. Naming conventions and resource layout match — a user familiar
with one should be able to read the other quickly.

The **cheapest** path for a one-off test is Azure, mostly because Azure
Container Instances bills per-second and SQS + Postgres together cost a bit
less than AWS's RDS `db.t4g.micro`. If you're going to leave the stack up for
weeks, AWS is slightly cheaper because ECR doesn't carry ACR Basic's flat
$0.167/day fee.

## What you're testing

Both playbooks spin up the same polyqueue demo worker from `tools/demo/`,
which registers three handlers (`add`, `greet`, `sleep`) and enters the
worker loop. You enqueue a small batch from your laptop with `enqueue.py`
and watch:

- Jobs transitioning through `queued → processing → succeeded/failed`
- The attempts audit log (`polyqueue_jobs_attempts`) populating with one
  `claimed` event per claim and one `succeeded` / `failed` / `abandoned`
  event per terminal transition, including worker identity, duration,
  and `finalized_by`.
- The dashboard at `ts/tools/dashboard/` showing workers, jobs, and per-job
  attempt history.

## A word of caution

Both playbooks create **publicly accessible Postgres** locked to your laptop's
IP via firewall rules. This is a deliberate choice for a throwaway test
stack — it keeps cost low and avoids VPN/bastion complexity. Do not copy this
pattern for anything that handles real data.

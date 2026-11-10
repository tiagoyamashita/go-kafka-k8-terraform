# AWS cost estimate

This repo applies a local kind cluster. The figures below are what a matching deployment costs on AWS if the same processes run there. They are on-demand list prices for **us-east-1** in November 2026, at 730 hours in a month. Tax, Savings Plans, and data transfer above the free allowance are extra. Recheck the [AWS Pricing Calculator](https://calculator.aws/) before buying anything. Another region is usually a few percent higher.

The Go processes are small. Receiver, transmitter, and console each request 20m CPU and 32Mi of memory. Kafka requests 100m and 384Mi, with a 1Gi volume. Postgres requests 50m and 128Mi, with a 1Gi volume. Prometheus keeps 6 hours of metrics. On AWS those processes ride on one or two worker nodes. The bill is the EKS control plane, Amazon MSK, RDS, and the NAT gateway.

Local Kafka runs one broker with replication factor 1. The estimates below use two brokers for a small cluster and three for a modest production cluster, which is the usual MSK minimum for a replicated topic.

## Small always-on cluster

Enough for the current traffic shape: one worker, a two-broker dev MSK cluster, and a single-AZ database. `kafka.t3.small` is a development broker.

| Line | Quantity | Rate | Month |
| --- | --- | --- | --- |
| EKS control plane, standard support | 1 cluster | $0.10 / hour | $73.00 |
| EC2 worker, t3.medium | 1 | $0.0416 / hour | $30.37 |
| EBS gp3 root volume | 30 GB | $0.08 / GB-month | $2.40 |
| NAT gateway | 1 | $0.045 / hour | $32.85 |
| Public IPv4 on the NAT gateway | 1 | $0.005 / hour | $3.65 |
| Application Load Balancer for the console | 1 | $0.0225 / hour | $16.43 |
| Public IPv4 on the load balancer | 1 | $0.005 / hour | $3.65 |
| MSK `kafka.t3.small` | 2 brokers | $0.0456 / hour | $66.58 |
| MSK broker storage | 100 GB | $0.10 / GB-month | $10.00 |
| RDS PostgreSQL `db.t4g.micro`, single-AZ | 1 | $0.016 / hour | $11.68 |
| RDS gp3 storage | 20 GB | $0.115 / GB-month | $2.30 |
| **Total** | | | **about $253** |

The worker runs the receiver, transmitter, console, OpenTelemetry Collector, Prometheus, Tempo, and Grafana. Those containers have no separate AWS charge.

## Modest production cluster

Same applications, with a Multi-AZ database, three production brokers, and two workers so one node can drain.

| Line | Quantity | Rate | Month |
| --- | --- | --- | --- |
| EKS control plane, standard support | 1 cluster | $0.10 / hour | $73.00 |
| EC2 worker, t3.medium | 2 | $0.0416 / hour | $60.74 |
| EBS gp3 root volumes | 60 GB | $0.08 / GB-month | $4.80 |
| NAT gateway | 1 | $0.045 / hour | $32.85 |
| Public IPv4 on the NAT gateway | 1 | $0.005 / hour | $3.65 |
| Application Load Balancer for the console | 1 | $0.0225 / hour | $16.43 |
| Public IPv4 on the load balancer | 1 | $0.005 / hour | $3.65 |
| MSK `kafka.m7g.large` | 3 brokers | $0.204 / hour | $446.76 |
| MSK broker storage | 300 GB | $0.10 / GB-month | $30.00 |
| RDS PostgreSQL `db.t4g.small`, Multi-AZ | 1 | $0.064 / hour | $46.72 |
| RDS gp3 storage | 20 GB | $0.115 / GB-month | $2.30 |
| **Total** | | | **about $721** |

MSK is about 60% of that total. A larger broker or a fourth broker moves the bill faster than adding transmitter replicas.

## What else shows up on the bill

These stay small at the message rate this service is built for, and they grow with bytes rather than with pod count.

- **Data transfer out to the internet.** The first 100 GB each month is free, then about $0.09 / GB.
- **Cross-AZ traffic.** About $0.01 / GB in each direction. MSK replication and a Multi-AZ RDS standby create this traffic even when clients are quiet.
- **Load balancer capacity units.** The hourly ALB charge above is the base fee. A quiet console adds little on top.
- **NAT data processing.** $0.045 / GB through the gateway, on top of the hourly fee.
- **Container image storage.** One image in ECR is well under a dollar a month.
- **Secrets.** One Secrets Manager secret is $0.40 / month. A customer-managed KMS key is $1 / month.
- **Logs.** CloudWatch Logs ingestion is about $0.50 / GB. A few hundred megabytes a month is about a dollar.

EKS extended support is $0.60 / cluster-hour, about $438 / month, once the Kubernetes version leaves the 14-month standard window. Staying on a supported version keeps the control plane at $73.

## Confluent Cloud instead of MSK

The receiver and transmitter already take `KAFKA_SECURITY_PROTOCOL=SASL_SSL` and a SASL username and password. Pointing them at Confluent Cloud removes the MSK rows and replaces them with that cluster's price. RDS, EKS, the worker, the NAT gateway, and the load balancer stay. Use the Confluent price for the cluster size you pick. A Basic cluster is usage-based. A Standard cluster adds an hourly charge.

## What does not reduce the bill

Stopping the worker saves its EC2 hours, about $30 for one t3.medium. The EKS control plane, MSK brokers, RDS instance, and NAT gateway keep charging until those resources are deleted. RDS storage and MSK storage keep charging after the instances are stopped.

The local 1Gi volumes are not a storage budget. The 20 GB RDS volume and the 50–100 GB per broker are the smallest practical AWS sizes, and they are still a few dollars next to the instance hours.

# AWS Bench

This project implements a distributed load generation tool that enables programs to execute concurrently within containers across an ECS (Elastic Container Service) cluster. Any program available in the container can be configured to execute.

This project is currently configured to execute benchmarks and other test programs against relational databases like Aurora and RDS (Relational Database Service) databases including PostgreSQL, MySQL and Oracle.

The BenchMan program is responsible for implementing these capabilities. This program runs in two modes. In the *controller* mode a single instance of BenchMan runs on an EC2 instance. The controller is responsible for provisioning AWS resources, sending commands to ECS containers and gathering results. The ECS containers run the BenchMan program in *worker* mode. In worker mode the program is responsible for processing commands sent to it by the controller and returning results from those commands to the controller.

This framework uses the SNS, SQS, ECR and ECS (Fargate) AWS services. The architecture looks like this.

```         
╔══════════════════════════════════════════════════════════════════════════════╗
║                           AWS (us-east-1)                                    ║
║                                                                              ║
║  ┌─────────────────────────────────────────────────────────────────────┐     ║
║  │  ECR: 200214900248.dkr.ecr.us-east-1.amazonaws.com/aws/awsbench     │     ║
║  └─────────────────────────────────┬───────────────────────────────────┘     ║
║                                    │ pulled on task start                    ║
║  ╔═════════════════════╗           │                                         ║
║  ║  EC2 Instance       ║   RunTask │                                         ║
║  ║  ┌───────────────┐  ║───────────┼──►┌─────────────────────────────────┐   ║
║  ║  │  BenchMan     │  ║           │   │  ECS Fargate: benchman-cluster  │   ║
║  ║  │  controller   │  ║           │   │                                 │   ║
║  ║  │               │  ║           └──►│  tptbm-task   tptbm-task  ...   │   ║
║  ║  │  BenchMssg    │  ║               │  ┌─────────┐  ┌─────────┐       │   ║
║  ║  │  BenchContainer│ ║               │  │ BenchMan│  │ BenchMan│       │   ║
║  ║  └──────┬────────┘  ║               │  │ worker  │  │ worker  │       │   ║
║  ╚═════════╪═══════════╝               └──┴────┬────┴──┴────┬────┴───────┘   ║
║            │                                   │            │                ║
║            │ publish START                     │ create     │ create         ║
║            │                                   │ + subscribe│ + subscribe    ║
║            ▼                                   ▼            ▼                ║
║  ┌───────────────────────────────────────────────────────────────────┐       ║
║  │                  SNS Topic: benchman-commands                     │       ║
║  └──────────────────────────┬────────────────────────────────────────┘       ║
║                             │ fan-out on every publish                       ║
║              ┌──────────────┼──────────────┐                                 ║
║              ▼              ▼              ▼                                 ║
║  ┌────────────────┐ ┌───────────────┐   ...                                  ║
║  │ SQS            │ │ SQS           │                                        ║
║  │ benchman-      │ │ benchman-     │                                        ║
║  │ worker-<uuid1> │ │ worker-<uuid2>│                                        ║
║  └───────┬────────┘ └───────┬───────┘                                        ║
║          │ worker polls     │ worker polls                                   ║
║          │                  │                                                ║
║          │  READY / DONE sent directly (not via SNS)                         ║
║          └──────────────────┴──────────────────────────────────────┐         ║
║                                                                    ▼         ║
║  ┌─────────────────────────────────────────────────────────────────────┐     ║
║  │                   SQS Queue: benchman-results                       │     ║
║  └─────────────────────────────────┬───────────────────────────────────┘     ║
║                                    │ controller polls                        ║
║                                    ▼                                         ║
║                              BenchMan controller                             ║
║                                                                              ║
║  ┌───────────────────────────┐     ┌─────────────────────┐                   ║
║  │ IAM                       │     │ CloudWatch Logs     │                   ║
║  │ ecsTaskExecutionRole      │     │ /ecs/tptbm-task     │                   ║
║  │  • ECR pull               │     │  (worker stdout)    │                   ║
║  │  • CloudWatch Logs write  │     └─────────────────────┘                   ║
║  │ benchman-task-role        │                                               ║
║  │  • SNS CreateTopic        │                                               ║
║  │  • SNS Subscribe/Receive  │                                               ║
║  │  • SQS Create/Send/Recv   │                                               ║
║  └───────────────────────────┘                                               ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

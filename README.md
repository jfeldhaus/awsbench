# AWS Bench

This project implements a distributed load generation tool that enables programs to execute concurrently within containers across an ECS (Elastic Container Service) cluster. Any program available in the container can be configured to execute.

This project is currently configured to execute benchmarks and other test programs against relational databases like Aurora and RDS (Relational Database Service) databases including PostgreSQL, MySQL and Oracle.

The BenchMan program is responsible for implementing these capabilities. This program runs in two modes. In the *controller* mode a single instance of BenchMan runs on an EC2 instance. The controller is responsible for provisioning AWS resources, sending commands to ECS containers and gathering results. The ECS containers run the BenchMan program in *worker* mode. In worker mode the program is responsible for processing commands sent to it by the controller and returning results from those commands to the controller.

This framework uses the SNS, SQS, ECR and ECS (Fargate) AWS services. The architecture looks like this.

```         
┌──────────────────────────────────────────────────────────────────────────────┐
│  AWS (us-east-1)                                                             │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │  ECR: 200214900248.dkr.ecr.us-east-1.amazonaws.com/aws/awsbench:latest │  │
│  └───────────────────────────────────┬────────────────────────────────────┘  │
│                                      │ image pulled on task start            │
│  ┌───────────────────────────┐       │  ┌─────────────────────────────────┐  │
│  │  EC2 Instance             │       │  │  ECS Fargate: benchman-cluster  │  │
│  │  BenchMan controller      ├─RunTask─►│  benchman-task × N              │  │
│  │   BenchMessaging (SNS/SQS)│       └─►│  BenchMan worker                │  │
│  │   BenchContainer (ECS)    │          │   BenchWorker (SNS/SQS)         │  │
│  │   BenchDoc + BenchCmd     │          │   sh -c <cmd> via ProcessBuilder│  │
│  └─────────────┬─────────────┘          └─────────────────┬───────────────┘  │
│                │                                          │ create queue     │
│                │ publish commands                         │ + subscribe      │
│                │ (READY_REQUEST, EXEC, STOP)              │                  │
│                ▼                                          ▼                  │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │  SNS Topic: benchman-commands                                          │  │
│  └──────────────────────────┬──────────────────┬───────────────────────── ┘  │
│                             │ fan-out          │                             │
│                             ▼                  ▼                             │
│  ┌──────────────────────────────┐  ┌──────────────────────────────┐          │
│  │  SQS: benchman-worker-<id1>  │  │  SQS: benchman-worker-<id2>  │  ...     │
│  └──────────────┬───────────────┘  └───────────────┬──────────────┘          │
│                 │                                  │                         │
│                 │  READY / EXEC_RESULT (direct SQS)│                         │
│                 └──────────────────┬───────────────┘                         │
│                                    ▼                                         │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │  SQS Queue: benchman-results  ◄── controller polls                     │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  ┌──────────────────────────────────────┐  ┌──────────────────────────────┐  │
│  │  IAM                                 │  │  CloudWatch Logs             │  │
│  │  ecsTaskExecutionRole                │  │  /ecs/benchman-task          │  │
│  │   ECR pull + CloudWatch Logs write   │  │  (worker container stdout)   │  │
│  │  benchman-task-role                  │  └──────────────────────────────┘  │
│  │   SNS CreateTopic, Subscribe, Recv   │                                    │
│  │   SQS Create, Delete, Send, Recv     │                                    │
│  └──────────────────────────────────────┘                                    │
└──────────────────────────────────────────────────────────────────────────────┘
```

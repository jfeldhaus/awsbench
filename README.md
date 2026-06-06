# AWS Bench

This project implements a distributed workload generation tool that enables programs to execute concurrently within containers across an AWS ECS (Elastic Container Service) cluster. The project contains several different database workload programs which can be executed against relational databases including PostgreSQL, MySQL, Oracle and Aurora. However, any program available in the container can be configured to execute.

The BenchMan program is responsible for implementing these capabilities. BenchMan runs in two modes. In the *controller* mode a single instance of BenchMan runs on an EC2 instance. The controller is responsible for deploying containers to ECS and sending commands to containers. The ECS containers run the BenchMan program in *worker* mode. In worker mode the program is responsible for processing commands sent to it by the controller and returning results from those commands to the controller.

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

## Workload Programs

- [Tptbm](src/main/java/com/awsbench/workloads/tptbm/Tptbm.java)- This is a simple OLTP workload containing a single table with a composite primary key. The workload simulates a telecommunications application.

- [DNA](src/main/java/com/awsbench/workloads/dna/DNASeq.java)- This is a more complicated OLTP workload that simulates a DNA sequencing application.

- [DWWork](src/main/java/com/awsbench/workloads/dw/DWWork.java)- This is a data warehouse workload simulation using a star schema with dimension tables.

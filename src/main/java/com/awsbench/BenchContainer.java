package com.awsbench;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ecs.EcsClient;
import software.amazon.awssdk.services.ecs.model.AssignPublicIp;
import software.amazon.awssdk.services.ecs.model.DescribeTasksResponse;
import software.amazon.awssdk.services.ecs.model.Failure;
import software.amazon.awssdk.services.ecs.model.LaunchType;
import software.amazon.awssdk.services.ecs.model.RunTaskResponse;
import software.amazon.awssdk.services.ecs.model.Task;

import java.util.ArrayList;
import java.util.List;

//-----------------------------------------------------------
//
// BenchContainer
//
// Encapsulates all ECS container operations on behalf of
// BenchMan. Launches Fargate tasks, tracks their ARNs,
// waits for them to reach RUNNING, and stops them on shutdown.
//
//-----------------------------------------------------------

class BenchContainer {

  // -------------------------------------------------------
  // State
  // -------------------------------------------------------

  private final Region region;
  private EcsClient ecs;

  private String clusterName;
  private String taskDefinition;
  private String subnetId;

  private final List<String> runningTaskArns = new ArrayList<>();

  // -------------------------------------------------------
  // Constructor
  // -------------------------------------------------------

  public BenchContainer(Region region) {
    this.region = region;
  }

  // -------------------------------------------------------
  // Lifecycle
  // -------------------------------------------------------

  // Creates the ECS client and stores configuration needed to launch tasks.
  public void initialize(String clusterName, String taskDefinition, String subnetId) {
    this.ecs            = EcsClient.builder().region(region).build();
    this.clusterName    = clusterName;
    this.taskDefinition = taskDefinition;
    this.subnetId       = subnetId;
  }

  // Stops all running tasks, waits for them to reach STOPPED, then closes the ECS client.
  public void shutdown() {
    stopAllTasks();
    waitForTasksStopped(60_000);
    if (ecs != null)
      ecs.close();
  }

  // -------------------------------------------------------
  // Container operations
  // -------------------------------------------------------

  // Launches count Fargate tasks and tracks their ARNs.
  // RunTask accepts at most 10 tasks per call, so large counts are batched.
  public void launchTasks(int count) {
    if (count <= 0) return;

    int remaining = count;
    while (remaining > 0) {
      int batch = Math.min(remaining, 10);
      RunTaskResponse response = ecs.runTask(r -> r
          .cluster(clusterName)
          .taskDefinition(taskDefinition)
          .launchType(LaunchType.FARGATE)
          .count(batch)
          .networkConfiguration(n -> n.awsvpcConfiguration(v -> v
              .subnets(subnetId)
              .assignPublicIp(AssignPublicIp.ENABLED))));

      for (Task task : response.tasks()) {
        runningTaskArns.add(task.taskArn());
        System.out.println("Launched task: " + task.taskArn());
      }

      for (Failure failure : response.failures()) {
        System.err.println("Task launch failure [" + failure.arn() + "]: " + failure.reason());
      }

      remaining -= batch;
    }
  }

  // Stops all tracked running tasks.
  public void stopAllTasks() {
    for (String taskArn : runningTaskArns) {
      stopTask(taskArn);
    }
    runningTaskArns.clear();
  }

  // Stops a single task by its ARN.
  public void stopTask(String taskArn) {
    ecs.stopTask(r -> r
        .cluster(clusterName)
        .task(taskArn)
        .reason("BenchMan shutdown"));
    System.out.println("Stopped task: " + taskArn);
  }

  // Blocks until all tracked tasks have reached the RUNNING state,
  // or until maxWaitMs milliseconds have elapsed. Returns true if
  // all tasks are running, false if the timeout was reached.
  // Tasks that stop unexpectedly (launch failures) are removed from tracking.
  public boolean waitForTasksRunning(long maxWaitMs) {
    long deadline = System.currentTimeMillis() + maxWaitMs;
    List<String> pending = new ArrayList<>(runningTaskArns);
    int failedCount = 0;

    while (!pending.isEmpty() && System.currentTimeMillis() < deadline) {
      DescribeTasksResponse resp = ecs.describeTasks(r -> r
          .cluster(clusterName)
          .tasks(pending));

      for (Task task : resp.tasks()) {
        String status = task.lastStatus();
        if ("RUNNING".equals(status)) {
          pending.remove(task.taskArn());
          System.out.println("Task running: " + task.taskArn());
        } else if ("STOPPED".equals(status)) {
          String reason = task.stoppedReason() != null ? task.stoppedReason() : "unknown";
          System.err.println("Task stopped unexpectedly: " + task.taskArn() + " — " + reason);
          pending.remove(task.taskArn());
          runningTaskArns.remove(task.taskArn());
          failedCount++;
        }
      }

      if (!pending.isEmpty()) {
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
    }

    return pending.isEmpty() && failedCount == 0;
  }

  // Blocks until all tracked tasks have reached the STOPPED state,
  // or until maxWaitMs milliseconds have elapsed.
  public void waitForTasksStopped(long maxWaitMs) {
    long deadline = System.currentTimeMillis() + maxWaitMs;

    while (!runningTaskArns.isEmpty() && System.currentTimeMillis() < deadline) {
      DescribeTasksResponse resp = ecs.describeTasks(r -> r
          .cluster(clusterName)
          .tasks(runningTaskArns));

      for (Task task : resp.tasks()) {
        if ("STOPPED".equals(task.lastStatus()))
          runningTaskArns.remove(task.taskArn());
      }

      if (!runningTaskArns.isEmpty()) {
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
  }

  // Returns a snapshot of the currently tracked running task ARNs.
  public List<String> getRunningTaskArns() {
    return List.copyOf(runningTaskArns);
  }

  // Returns the number of currently tracked running tasks.
  public int getRunningTaskCount() {
    return runningTaskArns.size();
  }
}

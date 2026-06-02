package com.awsbench;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ecs.EcsClient;
import software.amazon.awssdk.services.ecs.model.AssignPublicIp;
import software.amazon.awssdk.services.ecs.model.DescribeTasksResponse;
import software.amazon.awssdk.services.ecs.model.Failure;
import software.amazon.awssdk.services.ecs.model.LaunchType;
import software.amazon.awssdk.services.ecs.model.RunTaskResponse;
import software.amazon.awssdk.services.ecs.model.Task;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.CreateTopicResponse;
import software.amazon.awssdk.services.sns.model.ListSubscriptionsByTopicResponse;
import software.amazon.awssdk.services.sns.model.SubscribeResponse;
import software.amazon.awssdk.services.sns.model.Subscription;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

//-----------------------------------------------------------
//
// BenchMan.java
//
// This file contains four classes:
//
//   BenchMan        — top-level orchestrator; runs as either a
//                     controller (broadcasts commands to workers)
//                     or a worker (receives commands and runs the
//                     TptbmAws benchmark). Entry point for both modes.
//
//   BenchContainer  — wraps AWS ECS; launches and stops Fargate
//                     tasks on behalf of the controller.
//
//   BenchMssg       — wraps AWS SNS and SQS; used by the controller
//                     to publish commands and collect results.
//
//   BenchWorker     — used in worker mode; creates a per-instance
//                     SQS command queue, subscribes it to the SNS
//                     topic, and sends results back to the controller.
//
//   BenchConfig     — loads and validates benchman.properties at
//                     startup; provides typed accessors for all
//                     configuration values.
//
// The SNS topic and SQS results queue are created on first run and
// reused on subsequent runs. They are not deleted on shutdown.
//
//-----------------------------------------------------------

public class BenchMan {

  // -------------------------------------------------------
  // Mode
  // -------------------------------------------------------

  static public final String MODE_CONTROLLER = "controller";
  static public final String MODE_WORKER     = "worker";

  static private String  mode            = null;
  static private int     expectedWorkers = 0;
  static private String  propsPath       = null;
  static public  boolean verbose         = false;
  static public  boolean debug           = false;

  // -------------------------------------------------------
  // State
  // -------------------------------------------------------

  private BenchMssg      mssg;
  private BenchContainer container;

  // -------------------------------------------------------
  // Lifecycle
  // -------------------------------------------------------

  // Provisions all SNS, SQS, and ECS resources.
  // Must be called before any other methods.
  public void initialize() {
    Region region = BenchConfig.getRegion("benchman.region");
    mssg = new BenchMssg(region);
    mssg.initialize(
        BenchConfig.getString("benchman.command.topic.name"),
        BenchConfig.getString("benchman.results.queue.name"));

    if (!debug) {
      container = new BenchContainer(region);
      container.initialize(
          BenchConfig.getString("benchman.cluster.name"),
          BenchConfig.getString("benchman.task.definition"),
          BenchConfig.getString("benchman.subnet.id"));
    }

    System.out.println("BenchMan initialized.");
    System.out.println("  Command topic : " + mssg.getCommandTopicArn());
    System.out.println("  Results queue : " + mssg.getResultsQueueUrl());
    System.out.println("  ECS cluster   : " + BenchConfig.getString("benchman.cluster.name"));
  }

  // Stops all running containers and releases all AWS resources.
  public void shutdown() {
    if (container != null)
      container.shutdown();
    if (mssg != null)
      mssg.shutdown();

    System.out.println("BenchMan shut down.");
  }

  static private void logVerbose(String direction, String json) {
    System.out.println("[" + direction + "] " + BenchMessage.prettyPrint(json));
  }

  // Polls the results queue until expectedCount messages of the given type
  // arrive or maxWaitMs elapses. Messages of any other type are consumed and
  // discarded — this is the stale-message fix.
  public List<String> waitForResults(int expectedCount, long maxWaitMs, String expectedType) {
    List<String> results = new ArrayList<>();
    long deadline = System.currentTimeMillis() + maxWaitMs;

    while (results.size() < expectedCount && System.currentTimeMillis() < deadline) {
      for (Message msg : mssg.receiveMessages()) {
        String body = BenchMssg.extractSnsBody(msg.body());
        mssg.deleteMessage(msg.receiptHandle());
        String type = BenchMessage.getType(body);
        if (expectedType.equals(type)) {
          if (verbose) logVerbose("RECV", body);
          results.add(body);
        } else {
          System.out.println("Discarding stale '" + type + "' message (waiting for '" + expectedType + "')");
          if (verbose) logVerbose("DISC", body);
        }
      }
    }

    return results;
  }

  // -------------------------------------------------------
  // Entry point
  // -------------------------------------------------------

  static private void usage() {
    System.err.println(
        "\nUsage:\n\n" +
        "  BenchMan { -h | --help }\n\n" +
        "  BenchMan -mode controller -workers <n> [-props <file>] [-v] [-debug]\n\n" +
        "  BenchMan -mode worker [-props <file>] [-v]\n\n" +
        "Options:\n\n" +
        "  -h | --help            Print this message and exit.\n\n" +
        "  -mode controller       Provision AWS resources (SNS topic, SQS queue)\n" +
        "                         and broadcast commands to workers.\n\n" +
        "  -mode worker           Subscribe to the controller's SNS command topic,\n" +
        "                         listen for commands, and run the TptbmAws benchmark.\n\n" +
        "  -workers <n>           Required with '-mode controller'. Sets the number of\n" +
        "                         ECS Fargate tasks to launch. The controller waits for\n" +
        "                         exactly N workers to send READY before broadcasting\n" +
        "                         START, and for N DONE signals before shutting down.\n\n" +
        "  -props <file>          Path to a Java properties file containing AWS resource\n" +
        "                         names, region, and timeout settings. If omitted, the\n" +
        "                         program looks for benchman.properties in the current\n" +
        "                         working directory. The program exits if the file is not\n" +
        "                         found or any required property is missing.\n\n" +
        "  -v                     Verbose output. Prints the full JSON of every message\n" +
        "                         sent and received.\n\n" +
        "  -debug                 Debug mode (controller only). Skips ECS task launch\n" +
        "                         and waits for workers that are already running locally\n" +
        "                         (e.g. started via docker run). Useful for local testing\n" +
        "                         without deploying to ECS.\n");
    System.exit(1);
  }

  static private void parseArgs(String[] args) {
    int i = 0;
    while (i < args.length) {
      if (args[i].equalsIgnoreCase("-h") || args[i].equalsIgnoreCase("--help")) {
        usage();
      } else if (args[i].equalsIgnoreCase("-mode")) {
        if (++i >= args.length) {
          System.err.println("'-mode' requires a value: " + MODE_CONTROLLER + " or " + MODE_WORKER);
          System.exit(1);
        }
        String val = args[i++];
        if (!val.equals(MODE_CONTROLLER) && !val.equals(MODE_WORKER)) {
          System.err.println("Invalid value for '-mode': '" + val + "'. Must be '" +
              MODE_CONTROLLER + "' or '" + MODE_WORKER + "'.");
          System.exit(1);
        }
        mode = val;
      } else if (args[i].equalsIgnoreCase("-workers")) {
        if (++i >= args.length) {
          System.err.println("'-workers' requires a positive integer argument.");
          System.exit(1);
        }
        try {
          expectedWorkers = Integer.parseInt(args[i++]);
        } catch (NumberFormatException e) {
          System.err.println("'-workers' requires a positive integer argument.");
          System.exit(1);
        }
        if (expectedWorkers <= 0) {
          System.err.println("'-workers' requires a positive, non-zero integer.");
          System.exit(1);
        }
      } else if (args[i].equalsIgnoreCase("-props")) {
        if (++i >= args.length) {
          System.err.println("'-props' requires a file path argument.");
          System.exit(1);
        }
        propsPath = args[i++];
      } else if (args[i].equalsIgnoreCase("-v")) {
        verbose = true;
        i++;
      } else if (args[i].equalsIgnoreCase("-debug")) {
        debug = true;
        i++;
      } else {
        System.err.println("Unknown argument: '" + args[i] + "'");
        usage();
      }
    }

    if (mode == null) {
      System.err.println("'-mode' is required. Valid values: " + MODE_CONTROLLER + ", " + MODE_WORKER);
      usage();
    }

    if (expectedWorkers > 0 && !MODE_CONTROLLER.equals(mode)) {
      System.err.println("'-workers' is only valid with '-mode controller'.");
      System.exit(1);
    }

    if (MODE_CONTROLLER.equals(mode) && expectedWorkers <= 0) {
      System.err.println("'-workers N' is required with '-mode controller'.");
      usage();
    }
  }

  static private void runController() {
    int workerCount = expectedWorkers;

    BenchMan manager = new BenchMan();
    try {
      manager.initialize();

      long readyTimeout  = BenchConfig.getInt("benchman.worker.ready.timeout.ms");
      long resultTimeout = BenchConfig.getInt("benchman.result.wait.timeout.ms");

      if (debug) {
        System.out.println("[DEBUG] Skipping ECS launch — expecting " + workerCount +
            " locally running worker(s).");
      } else {
        System.out.println("Launching " + workerCount + " ECS task(s)...");
        manager.container.launchTasks(workerCount);

        System.out.println("Waiting for tasks to reach RUNNING state...");
        if (!manager.container.waitForTasksRunning(readyTimeout)) {
          System.out.println("Timed out waiting for ECS tasks to start. Aborting.");
          return;
        }
      }

      System.out.println("Waiting for " + workerCount + " worker(s) to be ready...");
      List<String> readySignals = manager.waitForResults(workerCount, readyTimeout, "READY");
      if (readySignals.size() < workerCount) {
        System.out.println("Timed out waiting for workers (" +
            readySignals.size() + "/" + workerCount + " ready). Aborting.");
        return;
      }

      String commandId  = UUID.randomUUID().toString();
      String startJson  = BenchMessage.start(commandId, Map.of());
      System.out.println("All workers ready. Sending START...");
      if (verbose) logVerbose("SEND", startJson);
      manager.mssg.publishCommand(startJson);

      List<String> results = manager.waitForResults(workerCount, resultTimeout, "DONE");
      if (results.size() < workerCount)
        System.out.println("Timed out: received " + results.size() + "/" + workerCount + " DONE responses.");
      else
        results.forEach(r -> System.out.println("Received DONE from: " + BenchMessage.getField(r, "workerId")));
    } finally {
      manager.shutdown();
    }
  }

  static private void runWorker() {
    BenchWorker worker = new BenchWorker(BenchConfig.getRegion("benchman.region"));
    Runtime.getRuntime().addShutdownHook(new Thread(worker::shutdown));
    try {
      worker.initialize(
          BenchConfig.getString("benchman.command.topic.name"),
          BenchConfig.getString("benchman.results.queue.name"));
      String readyJson = BenchMessage.ready(worker.getWorkerId());
      if (verbose) logVerbose("SEND", readyJson);
      worker.sendResult(readyJson);
      System.out.println("Sent READY. Waiting for commands...");
      outer:
      while (true) {
        for (Message msg : worker.receiveCommand()) {
          String body = BenchMssg.extractSnsBody(msg.body());
          String type = BenchMessage.getType(body);
          System.out.println("Received command: " + type);
          if (verbose) logVerbose("RECV", body);
          worker.deleteCommand(msg.receiptHandle());
          if ("START".equals(type)) {
            String commandId = BenchMessage.getField(body, "commandId");
            // TODO: run TptbmAws benchmark here
            String doneJson  = BenchMessage.done(worker.getWorkerId(), commandId);
            if (verbose) logVerbose("SEND", doneJson);
            worker.sendResult(doneJson);
            System.out.println("Sent DONE.");
            break outer;
          }
        }
      }
    } finally {
      worker.shutdown();
    }
  }

  public static void main(String[] args) {
    parseArgs(args);
    BenchConfig.load(propsPath);
    if (MODE_CONTROLLER.equals(mode))
      runController();
    else
      runWorker();
  }
}

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
  public void launchTasks(int count) {
    if (count <= 0) return;

    RunTaskResponse response = ecs.runTask(r -> r
        .cluster(clusterName)
        .taskDefinition(taskDefinition)
        .launchType(LaunchType.FARGATE)
        .count(count)
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

//-----------------------------------------------------------
//
// BenchMssg
//
// Encapsulates all SNS and SQS operations on behalf of
// the controller. Owns the AWS clients, command topic ARN,
// and results queue URL and ARN for the session.
//
// The SNS topic and SQS queue are not deleted on shutdown;
// they persist between runs and are reused via getOrCreateQueue().
//
//-----------------------------------------------------------

class BenchMssg {

  // -------------------------------------------------------
  // State
  // -------------------------------------------------------

  private final Region region;
  private SnsClient sns;
  private SqsClient sqs;

  private String commandTopicArn;
  private String resultsQueueUrl;
  private String resultsQueueArn;

  // -------------------------------------------------------
  // Constructor
  // -------------------------------------------------------

  public BenchMssg(Region region) {
    this.region = region;
  }

  // -------------------------------------------------------
  // Lifecycle
  // -------------------------------------------------------

  // Creates AWS clients and provisions the SNS topic and SQS queue.
  public void initialize(String topicName, String queueName) {
    sns = SnsClient.builder().region(region).build();
    sqs = SqsClient.builder().region(region).build();

    commandTopicArn = createTopic(topicName);
    resultsQueueUrl = getOrCreateQueue(queueName);
    resultsQueueArn = getQueueArn(resultsQueueUrl);
  }

  // Closes AWS clients. Topic and queue are left intact for reuse on the next run.
  public void shutdown() {
    if (sns != null)
      sns.close();
    if (sqs != null)
      sqs.close();
  }

  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  public String getCommandTopicArn() { return commandTopicArn; }
  public String getResultsQueueUrl() { return resultsQueueUrl; }
  public String getResultsQueueArn() { return resultsQueueArn; }

  // -------------------------------------------------------
  // SNS operations
  // -------------------------------------------------------

  // Creates an SNS topic and returns its ARN.
  public String createTopic(String name) {
    CreateTopicResponse response = sns.createTopic(r -> r.name(name));
    return response.topicArn();
  }

  // Deletes an SNS topic by ARN.
  public void deleteTopic(String topicArn) {
    sns.deleteTopic(r -> r.topicArn(topicArn));
  }

  // Publishes a command string to the command topic.
  public void publishCommand(String command) {
    sns.publish(r -> r
        .topicArn(commandTopicArn)
        .message(command)
        .subject("BenchMan-Command"));
  }

  // Publishes an arbitrary message to any topic ARN.
  public void publishMessage(String topicArn, String subject, String body) {
    sns.publish(r -> r
        .topicArn(topicArn)
        .message(body)
        .subject(subject));
  }

  // Subscribes an SQS queue to an SNS topic and returns the subscription ARN.
  public String subscribeSqsToTopic(String topicArn, String queueArn) {
    SubscribeResponse response = sns.subscribe(r -> r
        .topicArn(topicArn)
        .protocol("sqs")
        .endpoint(queueArn)
        .returnSubscriptionArn(true));
    return response.subscriptionArn();
  }

  // Cancels a subscription by its ARN.
  public void unsubscribe(String subscriptionArn) {
    sns.unsubscribe(r -> r.subscriptionArn(subscriptionArn));
  }

  // Lists all subscriptions for the command topic.
  public List<Subscription> listSubscriptions() {
    ListSubscriptionsByTopicResponse response =
        sns.listSubscriptionsByTopic(r -> r.topicArn(commandTopicArn));
    return response.subscriptions();
  }

  // -------------------------------------------------------
  // SQS operations
  // -------------------------------------------------------

  // Creates an SQS queue and returns its URL.
  public String createQueue(String name) {
    CreateQueueResponse response = sqs.createQueue(r -> r.queueName(name));
    return response.queueUrl();
  }

  // Returns the URL of an existing queue, or creates it if it doesn't exist.
  // Avoids the 60-second restriction that applies when re-creating a deleted queue.
  public String getOrCreateQueue(String name) {
    try {
      return sqs.getQueueUrl(r -> r.queueName(name)).queueUrl();
    } catch (QueueDoesNotExistException e) {
      return createQueue(name);
    }
  }

  // Deletes the SQS queue at the given URL.
  public void deleteQueue(String queueUrl) {
    sqs.deleteQueue(r -> r.queueUrl(queueUrl));
  }

  // Returns the ARN of a queue given its URL.
  public String getQueueArn(String queueUrl) {
    GetQueueAttributesResponse response = sqs.getQueueAttributes(r -> r
        .queueUrl(queueUrl)
        .attributeNamesWithStrings("QueueArn"));
    return response.attributesAsStrings().get("QueueArn");
  }

  // Attaches a resource policy that allows the given SNS topic to send to the queue.
  public void setQueuePolicy(String queueUrl, String queueArn, String topicArn) {
    String policy = """
        {
          "Version": "2012-10-17",
          "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "sns.amazonaws.com"},
            "Action": "sqs:SendMessage",
            "Resource": "%s",
            "Condition": {"ArnEquals": {"aws:SourceArn": "%s"}}
          }]
        }
        """.formatted(queueArn, topicArn);

    sqs.setQueueAttributes(r -> r
        .queueUrl(queueUrl)
        .attributesWithStrings(Map.of("Policy", policy)));
  }

  // Receives messages from the results queue using long polling.
  public List<Message> receiveMessages() {
    ReceiveMessageResponse response = sqs.receiveMessage(r -> r
        .queueUrl(resultsQueueUrl)
        .maxNumberOfMessages(BenchConfig.getInt("benchman.sqs.max.messages"))
        .waitTimeSeconds(BenchConfig.getInt("benchman.sqs.poll.wait.seconds")));
    return response.messages();
  }

  // Deletes a single message from the results queue after processing.
  public void deleteMessage(String receiptHandle) {
    sqs.deleteMessage(r -> r
        .queueUrl(resultsQueueUrl)
        .receiptHandle(receiptHandle));
  }

  // Discards all messages currently in the results queue.
  public void purgeQueue() {
    sqs.purgeQueue(r -> r.queueUrl(resultsQueueUrl));
  }

  // Returns the approximate number of messages visible in the results queue.
  public int getQueueDepth() {
    GetQueueAttributesResponse response = sqs.getQueueAttributes(r -> r
        .queueUrl(resultsQueueUrl)
        .attributeNamesWithStrings("ApproximateNumberOfMessages"));
    String value = response.attributesAsStrings().get("ApproximateNumberOfMessages");
    return (value != null) ? Integer.parseInt(value) : 0;
  }

  // -------------------------------------------------------
  // Utility
  // -------------------------------------------------------

  // SNS wraps the original payload in a JSON envelope when delivering to SQS.
  // This extracts the value of the "Message" field from that envelope.
  // For messages sent directly to SQS (no envelope), the body is returned as-is.
  public static String extractSnsBody(String sqsBody) {
    String message = BenchMessage.getField(sqsBody, "Message");
    return (message != null) ? message : sqsBody;
  }
}

//-----------------------------------------------------------
//
// BenchWorker
//
// Runs inside a container. Discovers the controller's SNS
// command topic and SQS results queue by name, creates its
// own temporary SQS command queue, and subscribes it to the
// topic so it receives every broadcast command.
//
// Call initialize() before use and shutdown() on exit to
// unsubscribe and delete the temporary queue.
//
//-----------------------------------------------------------

class BenchWorker {

  private final Region region;
  private SnsClient sns;
  private SqsClient sqs;

  private String workerId;
  private String commandTopicArn;
  private String commandQueueUrl;
  private String subscriptionArn;
  private String resultsQueueUrl;

  public BenchWorker(Region region) {
    this.region = region;
  }

  public String getWorkerId() { return workerId; }

  // Discovers the controller's topic and results queue by name,
  // creates a unique per-worker command queue, and subscribes it.
  public void initialize(String topicName, String resultsQueueName) {
    sns = SnsClient.builder().region(region).build();
    sqs = SqsClient.builder().region(region).build();

    // createTopic is idempotent — returns the existing ARN if the topic exists
    commandTopicArn = sns.createTopic(r -> r.name(topicName)).topicArn();

    resultsQueueUrl = sqs.getQueueUrl(r -> r.queueName(resultsQueueName)).queueUrl();

    workerId        = "benchman-worker-" + UUID.randomUUID();
    commandQueueUrl = sqs.createQueue(r -> r.queueName(workerId)).queueUrl();

    String commandQueueArn = sqs.getQueueAttributes(r -> r
            .queueUrl(commandQueueUrl)
            .attributeNamesWithStrings("QueueArn"))
        .attributesAsStrings().get("QueueArn");

    String policy = """
        {
          "Version": "2012-10-17",
          "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "sns.amazonaws.com"},
            "Action": "sqs:SendMessage",
            "Resource": "%s",
            "Condition": {"ArnEquals": {"aws:SourceArn": "%s"}}
          }]
        }
        """.formatted(commandQueueArn, commandTopicArn);

    sqs.setQueueAttributes(r -> r
        .queueUrl(commandQueueUrl)
        .attributesWithStrings(Map.of("Policy", policy)));

    subscriptionArn = sns.subscribe(r -> r
            .topicArn(commandTopicArn)
            .protocol("sqs")
            .endpoint(commandQueueArn)
            .returnSubscriptionArn(true))
        .subscriptionArn();

    System.out.println("Worker initialized.");
    System.out.println("  Command queue : " + commandQueueUrl);
    System.out.println("  Subscribed to : " + commandTopicArn);
    System.out.println("  Results queue : " + resultsQueueUrl);
  }

  // Unsubscribes, deletes the temporary command queue, and closes clients.
  // Safe to call more than once — returns immediately after the first call.
  public synchronized void shutdown() {
    if (sns == null && sqs == null) return;
    if (subscriptionArn != null) {
      sns.unsubscribe(r -> r.subscriptionArn(subscriptionArn));
      subscriptionArn = null;
    }
    if (commandQueueUrl != null) {
      sqs.deleteQueue(r -> r.queueUrl(commandQueueUrl));
      commandQueueUrl = null;
    }
    sns.close(); sns = null;
    sqs.close(); sqs = null;
    System.out.println("Worker shut down.");
  }

  // Long-polls the worker's command queue for incoming messages.
  public List<Message> receiveCommand() {
    return sqs.receiveMessage(r -> r
            .queueUrl(commandQueueUrl)
            .maxNumberOfMessages(BenchConfig.getInt("benchman.sqs.max.messages"))
            .waitTimeSeconds(BenchConfig.getInt("benchman.sqs.poll.wait.seconds")))
        .messages();
  }

  // Deletes a command message after processing.
  public void deleteCommand(String receiptHandle) {
    sqs.deleteMessage(r -> r
        .queueUrl(commandQueueUrl)
        .receiptHandle(receiptHandle));
  }

  // Sends a result string directly to the controller's results queue.
  public void sendResult(String result) {
    sqs.sendMessage(r -> r
        .queueUrl(resultsQueueUrl)
        .messageBody(result));
  }
}

//-----------------------------------------------------------
//
// BenchConfig
//
// Loads and validates the benchman.properties file.
// Call load() once at startup before accessing any property.
// All required keys are checked upfront; the program exits
// with a clear error if the file or any key is missing.
//
//-----------------------------------------------------------

class BenchConfig {

  private static final String[] REQUIRED_KEYS = {
    "benchman.region",
    "benchman.command.topic.name",
    "benchman.results.queue.name",
    "benchman.cluster.name",
    "benchman.task.definition",
    "benchman.subnet.id",
    "benchman.sqs.poll.wait.seconds",
    "benchman.sqs.max.messages",
    "benchman.worker.ready.timeout.ms",
    "benchman.result.wait.timeout.ms",
  };

  private static Properties props = null;

  // Loads properties from the given path, or from benchman.properties in the
  // working directory if path is null. Exits on missing file or missing keys.
  public static void load(String path) {
    File file = (path != null) ? new File(path) : new File("benchman.properties");
    if (!file.exists()) {
      if (path != null)
        System.err.println("Properties file not found: " + file.getAbsolutePath());
      else
        System.err.println("Properties file not found: " + file.getAbsolutePath() +
            "\nUse -props <file> to specify a different location.");
      System.exit(1);
    }

    props = new Properties();
    try (FileInputStream fis = new FileInputStream(file)) {
      props.load(fis);
    } catch (IOException e) {
      System.err.println("Failed to read properties file '" + file.getAbsolutePath() +
          "': " + e.getMessage());
      System.exit(1);
    }

    validate();
    System.out.println("Loaded configuration from: " + file.getAbsolutePath());
  }

  // Checks all required keys are present and reports every missing one before exiting.
  private static void validate() {
    List<String> missing = new ArrayList<>();
    for (String key : REQUIRED_KEYS) {
      if (props.getProperty(key) == null)
        missing.add(key);
    }
    if (!missing.isEmpty()) {
      System.err.println("Missing required properties:");
      missing.forEach(k -> System.err.println("  " + k));
      System.exit(1);
    }
  }

  public static String getString(String key) {
    return props.getProperty(key).trim();
  }

  public static int getInt(String key) {
    String val = getString(key);
    try {
      return Integer.parseInt(val);
    } catch (NumberFormatException e) {
      System.err.println("Property '" + key + "' must be an integer, got: '" + val + "'");
      System.exit(1);
      return 0;
    }
  }

  public static Region getRegion(String key) {
    return Region.of(getString(key));
  }
}

//-----------------------------------------------------------
//
// BenchMessage
//
// Builds and parses the structured JSON messages exchanged
// between the controller and workers.
//
// Command messages (controller → workers via SNS):
//   PING, START, STOP
//
// Result messages (workers → controller via SQS):
//   READY, PONG, RESULT, ERROR
//
// All messages carry a "type" discriminator so receivers can
// filter by type and safely discard messages they don't expect.
//
//-----------------------------------------------------------

class BenchMessage {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  // -------------------------------------------------------
  // Command builders (controller → workers)
  // -------------------------------------------------------

  public static String start(String commandId, Map<String, Object> benchmark) {
    LinkedHashMap<String, Object> m = new LinkedHashMap<>();
    m.put("type",      "START");
    m.put("commandId", commandId);
    m.put("timestamp", Instant.now().toString());
    m.put("benchmark", benchmark);
    return toJson(m);
  }

  public static String stop(String commandId) {
    LinkedHashMap<String, Object> m = new LinkedHashMap<>();
    m.put("type",      "STOP");
    m.put("commandId", commandId);
    m.put("timestamp", Instant.now().toString());
    return toJson(m);
  }

  // -------------------------------------------------------
  // Result builders (workers → controller)
  // -------------------------------------------------------

  public static String ready(String workerId) {
    LinkedHashMap<String, Object> m = new LinkedHashMap<>();
    m.put("type",      "READY");
    m.put("workerId",  workerId);
    m.put("timestamp", Instant.now().toString());
    return toJson(m);
  }

  public static String done(String workerId, String commandId) {
    LinkedHashMap<String, Object> m = new LinkedHashMap<>();
    m.put("type",      "DONE");
    m.put("workerId",  workerId);
    m.put("commandId", commandId);
    m.put("timestamp", Instant.now().toString());
    return toJson(m);
  }

  public static String result(String workerId, String commandId, Map<String, Object> resultData) {
    LinkedHashMap<String, Object> m = new LinkedHashMap<>();
    m.put("type",      "RESULT");
    m.put("workerId",  workerId);
    m.put("commandId", commandId);
    m.put("timestamp", Instant.now().toString());
    m.put("result",    resultData);
    return toJson(m);
  }

  public static String error(String workerId, String commandId, String message, String detail) {
    LinkedHashMap<String, Object> err = new LinkedHashMap<>();
    err.put("message", message);
    err.put("detail",  detail);
    LinkedHashMap<String, Object> m = new LinkedHashMap<>();
    m.put("type",      "ERROR");
    m.put("workerId",  workerId);
    m.put("commandId", commandId);
    m.put("timestamp", Instant.now().toString());
    m.put("error",     err);
    return toJson(m);
  }

  // -------------------------------------------------------
  // Parsing
  // -------------------------------------------------------

  public static String getType(String json) {
    return getField(json, "type");
  }

  public static String getField(String json, String field) {
    try {
      @SuppressWarnings("unchecked")
      Map<String, Object> map = MAPPER.readValue(json, Map.class);
      Object val = map.get(field);
      return val != null ? val.toString() : null;
    } catch (Exception e) {
      return null;
    }
  }

  // -------------------------------------------------------
  // Helpers
  // -------------------------------------------------------

  public static String prettyPrint(String json) {
    try {
      Object obj = MAPPER.readValue(json, Object.class);
      return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
    } catch (Exception e) {
      return json;
    }
  }

  private static String toJson(Object obj) {
    try {
      return MAPPER.writeValueAsString(obj);
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize message: " + e.getMessage(), e);
    }
  }
}

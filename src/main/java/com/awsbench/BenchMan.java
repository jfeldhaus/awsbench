package com.awsbench;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

//-----------------------------------------------------------
//
// BenchMan
//
// Top-level orchestrator. Runs as either a controller
// (provisions AWS resources, deploys ECS tasks, broadcasts
// commands, and collects results) or a worker (subscribes
// to the SNS command topic, receives commands, and runs
// the TptbmAws benchmark).
//
// Entry point for both modes. Configuration is loaded from
// benchman.properties via BenchConfig before any work begins.
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
  static public  boolean nodeploy        = false;

  // -------------------------------------------------------
  // State
  // -------------------------------------------------------

  private BenchMessaging      mssg;
  private BenchContainer container;

  // -------------------------------------------------------
  // Lifecycle
  // -------------------------------------------------------

  // Provisions all SNS, SQS, and ECS resources.
  // Must be called before any other methods.
  public void initialize() {
    Region region = BenchConfig.getRegion("benchman.region");
    mssg = new BenchMessaging(region);
    mssg.initialize(
        BenchConfig.getString("benchman.command.topic.name"),
        BenchConfig.getString("benchman.results.queue.name"));

    if (!nodeploy) {
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
    System.out.println("[" + direction + "] " + BenchPayload.prettyPrint(json));
  }

  // Polls the results queue until expectedCount messages of the given type
  // arrive or maxWaitMs elapses. Messages of any other type are consumed and
  // discarded — this is the stale-message fix.
  public List<String> waitForResults(int expectedCount, long maxWaitMs, String expectedType) {
    List<String> results = new ArrayList<>();
    long deadline = System.currentTimeMillis() + maxWaitMs;

    while (results.size() < expectedCount && System.currentTimeMillis() < deadline) {
      for (Message msg : mssg.receiveMessages()) {
        String body = BenchMessaging.extractSnsBody(msg.body());
        mssg.deleteMessage(msg.receiptHandle());
        String type = BenchPayload.getType(body);
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
        "  BenchMan -mode controller -workers <n> [-props <file>] [-v] [-nodeploy]\n\n" +
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
        "  -nodeploy              Only valid with '-mode controller'. Skips ECS task\n" +
        "                         launch and waits for workers that are already running\n" +
        "                         locally (e.g. started via docker run). Useful for\n" +
        "                         local testing without deploying to ECS.\n");
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
      } else if (args[i].equalsIgnoreCase("-nodeploy")) {
        nodeploy = true;
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

      if (nodeploy) {
        System.out.println("Skipping ECS launch — expecting " + workerCount +
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

      String commandId = UUID.randomUUID().toString();
      String startJson = BenchPayload.start(commandId, Map.of());
      System.out.println("All workers ready. Sending START...");
      if (verbose) logVerbose("SEND", startJson);
      manager.mssg.publishCommand(startJson);

      List<String> results = manager.waitForResults(workerCount, resultTimeout, "DONE");
      if (results.size() < workerCount)
        System.out.println("Timed out: received " + results.size() + "/" + workerCount + " DONE responses.");
      else
        results.forEach(r -> System.out.println("Received DONE from: " + BenchPayload.getField(r, "workerId")));
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
      String readyJson = BenchPayload.ready(worker.getWorkerId());
      if (verbose) logVerbose("SEND", readyJson);
      worker.sendResult(readyJson);
      System.out.println("Sent READY. Waiting for commands...");
      outer:
      while (true) {
        for (Message msg : worker.receiveCommand()) {
          String body = BenchMessaging.extractSnsBody(msg.body());
          String type = BenchPayload.getType(body);
          System.out.println("Received command: " + type);
          if (verbose) logVerbose("RECV", body);
          worker.deleteCommand(msg.receiptHandle());
          if ("START".equals(type)) {
            String commandId = BenchPayload.getField(body, "commandId");
            // TODO: run TptbmAws benchmark here
            String doneJson = BenchPayload.done(worker.getWorkerId(), commandId);
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

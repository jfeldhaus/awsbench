package com.awsbench;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
// to the SNS command topic, receives EXEC commands, and
// executes the workload defined in each command payload).
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
  static private String  configPath      = null;
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

  // Provisions SNS and SQS resources. Also provisions ECS resources
  // unless -nodeploy is set. Must be called before any other methods.
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

  // Stops all running ECS tasks and closes AWS clients.
  // The SNS topic and SQS results queue are left intact for reuse on the next run.
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

  // Polls the results queue until expectedCount messages of the given type and
  // sessionId arrive, or maxWaitMs elapses. Messages with the wrong type or a
  // mismatched sessionId are consumed and discarded.
  public List<String> waitForResults(int expectedCount, long maxWaitMs,
      String expectedType, String sessionId) {
    List<String> results = new ArrayList<>();
    long deadline = System.currentTimeMillis() + maxWaitMs;

    while (results.size() < expectedCount && System.currentTimeMillis() < deadline) {
      for (Message msg : mssg.receiveMessages()) {
        String body = BenchMessaging.extractSnsBody(msg.body());
        mssg.deleteMessage(msg.receiptHandle());
        String type = BenchPayload.getType(body);
        if (!expectedType.equals(type)) {
          System.out.println("Discarding stale '" + type + "' message (waiting for '" + expectedType + "')");
          if (verbose) logVerbose("DISC", body);
          continue;
        }
        String msgSession = BenchPayload.getField(body, "sessionId");
        if (!sessionId.equals(msgSession)) {
          System.out.println("Discarding '" + type + "' message with wrong session ID.");
          if (verbose) logVerbose("DISC", body);
          continue;
        }
        if (verbose) logVerbose("RECV", body);
        results.add(body);
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
        "  BenchMan -mode controller -workers <n> -config <file> [-props <file>] [-v] [-nodeploy]\n\n" +
        "  BenchMan -mode worker [-props <file>] [-v]\n\n" +
        "Options:\n\n" +
        "  -h | --help            Print this message and exit.\n\n" +
        "  -mode controller       Provision AWS resources (SNS topic, SQS queue)\n" +
        "                         and broadcast commands to workers.\n\n" +
        "  -mode worker           Subscribe to the controller's SNS command topic,\n" +
        "                         wait for READY_REQUEST, then handle EXEC commands\n" +
        "                         until STOP is received or the idle timeout expires.\n\n" +
        "  -workers <n>           Required with '-mode controller'. Sets the number of\n" +
        "                         ECS Fargate tasks to launch. The controller sends\n" +
        "                         READY_REQUEST, waits for N READY signals, executes\n" +
        "                         commands from the -config document, then sends STOP.\n\n" +
        "  -config <file>         Required with '-mode controller'. Path to the benchmark\n" +
        "                         document (JSON) defining the commands to execute, their\n" +
        "                         order, variable substitutions, wait behaviour, and\n" +
        "                         which workers each command targets.\n\n" +
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
      } else if (args[i].equalsIgnoreCase("-config")) {
        if (++i >= args.length) {
          System.err.println("'-config' requires a file path argument.");
          System.exit(1);
        }
        configPath = args[i++];
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

    if (MODE_CONTROLLER.equals(mode) && configPath == null) {
      System.err.println("'-config <file>' is required with '-mode controller'.");
      usage();
    }

    if (configPath != null && !MODE_CONTROLLER.equals(mode)) {
      System.err.println("'-config' is only valid with '-mode controller'.");
      System.exit(1);
    }
  }

  static private void runController() {
    int workerCount = expectedWorkers;

    // Load and validate the benchmark document before any AWS activity.
    BenchDoc doc = BenchDoc.load(configPath);
    List<String> errors = doc.validate(workerCount);
    if (!errors.isEmpty()) {
      System.err.println("Benchmark document is invalid:");
      errors.forEach(e -> System.err.println("  " + e));
      return;
    }
    System.out.println("Configuration: " + doc.getName());

    BenchMan manager = new BenchMan();
    try {
      manager.initialize();

      long readyTimeout  = BenchConfig.getInt("benchman.worker.ready.timeout.sec") * 1000L;
      long resultTimeout = BenchConfig.getInt("benchman.result.wait.timeout.sec") * 1000L;

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

      String sessionId = UUID.randomUUID().toString();
      System.out.println("Session ID: " + sessionId);

      // Resend READY_REQUEST every retryInterval seconds until all workers respond.
      // Workers may not be subscribed yet when the first send occurs, so retrying
      // ensures late-subscribing workers receive the request.
      long retryIntervalMs = BenchConfig.getInt("benchman.ready_request.retry.sec") * 1000L;
      long readyDeadline   = System.currentTimeMillis() + readyTimeout;
      List<String> readySignals = new ArrayList<>();

      while (readySignals.size() < workerCount && System.currentTimeMillis() < readyDeadline) {
        String readyRequestJson = BenchPayload.readyRequest(sessionId);
        if (readySignals.isEmpty())
          System.out.println("Sending READY_REQUEST to " + workerCount + " worker(s)...");
        else
          System.out.println("Resending READY_REQUEST (" +
              readySignals.size() + "/" + workerCount + " ready so far)...");
        if (verbose) logVerbose("SEND", readyRequestJson);
        manager.mssg.publishCommand(readyRequestJson);

        long batchTimeout = Math.min(retryIntervalMs, readyDeadline - System.currentTimeMillis());
        if (batchTimeout <= 0) break;
        readySignals.addAll(
            manager.waitForResults(workerCount - readySignals.size(), batchTimeout, "READY", sessionId));
      }

      if (readySignals.size() < workerCount) {
        System.out.println("Timed out waiting for workers (" +
            readySignals.size() + "/" + workerCount + " ready). Aborting.");
        return;
      }

      // Designate the first worker to respond as the leader for "targets: one" commands.
      String leaderId = BenchPayload.getField(readySignals.get(0), "workerId");
      System.out.println("All workers ready. Leader: " + leaderId);

      // Execute commands in order. For wait=false commands, accumulate the expected
      // result count and collect all pending results after the loop completes.
      int pendingResultCount = 0;

      for (BenchCmd command : doc.getCommandsInOrder()) {
        String resolvedCmd = doc.resolveCmd(command.getCmd());
        String commandId   = UUID.randomUUID().toString();
        int    targetCount = command.expectedResultCount(workerCount);

        String execJson = BenchPayload.exec(sessionId, commandId,
            resolvedCmd, command.getTargetsString(), leaderId);

        System.out.printf("Sending EXEC [%d] '%s'  targets=%s  cmd=%s%n",
            command.getCmdSeq(), command.getDescription(),
            command.getTargetsString(), resolvedCmd);
        if (verbose) logVerbose("SEND", execJson);
        manager.mssg.publishCommand(execJson);

        if (command.isWait()) {
          List<String> results = manager.waitForResults(targetCount, resultTimeout, "EXEC_RESULT", sessionId);
          if (results.size() < targetCount)
            System.out.printf("  Timed out: %d/%d result(s) received.%n", results.size(), targetCount);
          else
            results.forEach(r -> System.out.println(
                "  EXEC_RESULT from: " + BenchPayload.getField(r, "workerId")));
        } else {
          pendingResultCount += targetCount;
          System.out.println("  Not waiting for result (wait=false).");
        }
      }

      // Collect any results from wait=false commands.
      if (pendingResultCount > 0) {
        System.out.println("Collecting " + pendingResultCount + " pending result(s)...");
        List<String> pending = manager.waitForResults(pendingResultCount, resultTimeout, "EXEC_RESULT", sessionId);
        if (pending.size() < pendingResultCount)
          System.out.printf("Timed out: %d/%d pending result(s) received.%n",
              pending.size(), pendingResultCount);
        else
          pending.forEach(r -> System.out.println(
              "EXEC_RESULT from: " + BenchPayload.getField(r, "workerId")));
      }

      String stopJson = BenchPayload.stop(sessionId, UUID.randomUUID().toString());
      System.out.println("Sending STOP to all workers...");
      if (verbose) logVerbose("SEND", stopJson);
      manager.mssg.publishCommand(stopJson);
    } finally {
      manager.shutdown();
    }
  }

  // Returns true if this worker should execute the EXEC command based on targets.
  // "all" or absent → always execute; "one" → only the leader executes;
  // numeric targets are not yet implemented and default to executing.
  static private boolean isTargeted(String workerId, String targets, String leaderId) {
    if (targets == null || "all".equals(targets)) return true;
    if ("one".equals(targets)) return workerId.equals(leaderId);
    return true;
  }

  static private void runWorker() {
    BenchWorker worker = new BenchWorker(BenchConfig.getRegion("benchman.region"));
    Runtime.getRuntime().addShutdownHook(new Thread(worker::shutdown));
    try {
      worker.initialize(
          BenchConfig.getString("benchman.command.topic.name"),
          BenchConfig.getString("benchman.results.queue.name"));
      long deadline = System.currentTimeMillis()
          + BenchConfig.getInt("benchman.worker.start.timeout.sec") * 1000L;

      // Phase 1: wait for READY_REQUEST and extract the session ID.
      System.out.println("Waiting for READY_REQUEST...");
      String sessionId = null;
      while (sessionId == null && System.currentTimeMillis() < deadline) {
        for (Message msg : worker.receiveCommand()) {
          String body = BenchMessaging.extractSnsBody(msg.body());
          String type = BenchPayload.getType(body);
          worker.deleteCommand(msg.receiptHandle());
          if ("READY_REQUEST".equals(type)) {
            sessionId = BenchPayload.getField(body, "sessionId");
            if (verbose) logVerbose("RECV", body);
            System.out.println("Received READY_REQUEST (session: " + sessionId + ")");
            break;
          }
          System.out.println("Discarding unexpected '" + type + "' before READY_REQUEST.");
          if (verbose) logVerbose("DISC", body);
        }
      }

      if (sessionId == null) {
        System.out.println("Timed out waiting for READY_REQUEST. Shutting down.");
        return;
      }

      // Phase 2: send READY, then persistently handle EXEC commands until STOP
      // or until no command arrives within the idle timeout.
      String readyJson = BenchPayload.ready(worker.getWorkerId(), sessionId);
      if (verbose) logVerbose("SEND", readyJson);
      worker.sendResult(readyJson);
      System.out.println("Sent READY. Waiting for commands...");

      long idleTimeoutMs = BenchConfig.getInt("benchman.worker.idle.timeout.sec") * 1000L;
      long idleDeadline  = System.currentTimeMillis() + idleTimeoutMs;

      loop:
      while (System.currentTimeMillis() < idleDeadline) {
        for (Message msg : worker.receiveCommand()) {
          String body = BenchMessaging.extractSnsBody(msg.body());
          String type = BenchPayload.getType(body);
          worker.deleteCommand(msg.receiptHandle());

          String msgSession = BenchPayload.getField(body, "sessionId");
          if (!sessionId.equals(msgSession)) {
            System.out.println("Discarding '" + type + "' with wrong session ID.");
            if (verbose) logVerbose("DISC", body);
            continue;
          }

          if ("EXEC".equals(type)) {
            if (verbose) logVerbose("RECV", body);
            String commandId = BenchPayload.getField(body, "commandId");
            String targets   = BenchPayload.getField(body, "targets");
            String leaderId  = BenchPayload.getField(body, "leader_id");

            if (!isTargeted(worker.getWorkerId(), targets, leaderId)) {
              System.out.println("Skipping EXEC (not targeted for '" + targets + "'). Waiting for next command...");
            } else {
              String cmd = BenchPayload.getField(body, "cmd");
              System.out.println("Executing: " + cmd);

              long   startMs    = System.currentTimeMillis();
              int    returnCode = -1;
              String output     = "";

              try {
                ProcessBuilder pb = new ProcessBuilder("sh", "-c", cmd);
                pb.redirectErrorStream(true); // merge stderr into stdout
                Process process = pb.start();

                StringBuilder sb = new StringBuilder();
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(process.getInputStream()))) {
                  String line;
                  while ((line = reader.readLine()) != null)
                    sb.append(line).append("\n");
                }
                output     = sb.toString();
                returnCode = process.waitFor();
              } catch (IOException | InterruptedException e) {
                output = "Error executing command: " + e.getMessage();
                if (e instanceof InterruptedException) Thread.currentThread().interrupt();
              }

              long durationMs = System.currentTimeMillis() - startMs;
              System.out.printf("Completed: return_code=%d  duration_ms=%d%n", returnCode, durationMs);

              String execResultJson = BenchPayload.execResult(
                  worker.getWorkerId(), sessionId, commandId,
                  returnCode, durationMs, output);
              if (verbose) logVerbose("SEND", execResultJson);
              worker.sendResult(execResultJson);
              System.out.println("Sent EXEC_RESULT. Waiting for next command...");
            }
            idleDeadline = System.currentTimeMillis() + idleTimeoutMs;
          } else if ("STOP".equals(type)) {
            if (verbose) logVerbose("RECV", body);
            System.out.println("Received STOP. Shutting down.");
            break loop;
          } else {
            System.out.println("Discarding unexpected '" + type + "'.");
            if (verbose) logVerbose("DISC", body);
          }
        }
      }
      if (System.currentTimeMillis() >= idleDeadline)
        System.out.println("Idle timeout reached. No command received. Shutting down.");
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

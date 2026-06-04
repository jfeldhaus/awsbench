package com.awsbench;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;
import java.util.Map;
import java.util.UUID;

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
            .waitTimeSeconds(BenchConfig.getInt("benchman.sqs.poll.wait.sec")))
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

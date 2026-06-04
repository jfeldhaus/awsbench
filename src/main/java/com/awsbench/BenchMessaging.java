package com.awsbench;

import software.amazon.awssdk.regions.Region;
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

import java.util.List;
import java.util.Map;

//-----------------------------------------------------------
//
// BenchMessaging
//
// Encapsulates all SNS and SQS operations on behalf of
// the controller. Owns the AWS clients, command topic ARN,
// and results queue URL and ARN for the session.
//
// The SNS topic and SQS queue are not deleted on shutdown;
// they persist between runs and are reused via getOrCreateQueue().
//
//-----------------------------------------------------------

class BenchMessaging {

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

  public BenchMessaging(Region region) {
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
        .waitTimeSeconds(BenchConfig.getInt("benchman.sqs.poll.wait.sec")));
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
    String message = BenchPayload.getField(sqsBody, "Message");
    return (message != null) ? message : sqsBody;
  }
}

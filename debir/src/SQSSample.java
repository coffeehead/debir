/*
 * This is my own version of the sample at
 * http://docs.aws.amazon.com/AWSToolkitEclipse/latest/GettingStartedGuide/tke_java_apps.html
 * 
 * If you run this sample application repeatedly, you should wait at least 60 seconds between subsequent runs. 
 * Amazon SQS requires that at least 60 seconds elapse after deleting a queue before creating a queue with the 
 * same name.
 */

import java.util.List;
import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

/**
 * This sample demonstrates how to make basic requests to Amazon SQS using the AWS SDK for Java.
 * <p>
 * <b>Prerequisites:</b> You must have a valid Amazon Web Services developer account, and be signed up to use Amazon SQS. For more information on Amazon SQS, see http://aws.amazon.com/sqs.
 * <p>
 * WANRNING:</b> To avoid accidental leakage of your credentials, DO NOT keep the credentials file in your source directory.
 */

public class SQSSample {

	private static final String AWS_DEFAULT_USER_PROFILE = "default";
	private static final Regions DEFAULT_REGION = Regions.US_EAST_1;
	private static final String QUEUE_NAME = "SQSSampleQueue";
	private static final String SAMPLE_MESSAGE = "Sample Message";
	private static final int FIRST = 0;

	public static void main(String[] args) throws Exception {

		// setup
		AmazonSQS client = new AmazonSQSClient(getCredentials(AWS_DEFAULT_USER_PROFILE));
		client.setRegion(Region.getRegion(DEFAULT_REGION));

		try {
			String sampleQueue = createQueue(client, QUEUE_NAME);
			printQueueNames(client);
			sendMessage(client, sampleQueue, SAMPLE_MESSAGE);
			List<Message> messages = receiveMessages(client, sampleQueue);
			prettyPrint(messages);
			deleteMessage(client, sampleQueue, messages, FIRST);
			deleteQueue(client, sampleQueue);
		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it " + "to Amazon SQS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered " + "a serious internal problem while trying to communicate with SQS, such as not "
					+ "being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}

	private static AWSCredentials getCredentials(String profileName) {
		// The ProfileCredentialsProvider will look in your default location for your credentials
		// http://java.awsblog.com/post/TxRE9V31UFN860/Secure-Local-Development-with-the-ProfileCredentialsProvider

		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider(profileName).getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file.", e);
		}
		return credentials;
	}

	private static String createQueue(AmazonSQS client, String queueName) {
		CreateQueueRequest cqr = new CreateQueueRequest(queueName);
		CreateQueueResult result = client.createQueue(cqr);
		return result.getQueueUrl();
	}

	private static void printQueueNames(AmazonSQS client) {
		for (String queueUrl : client.listQueues().getQueueUrls()) {
			System.out.println("QueueUrl: " + queueUrl);
		}
	}

	private static void sendMessage(AmazonSQS client, String queueUrl, String messageText) {
		SendMessageRequest smr = new SendMessageRequest(queueUrl, messageText);
		client.sendMessage(smr);
	}

	private static List<Message> receiveMessages(AmazonSQS client, String queueUrl) {
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
		return client.receiveMessage(receiveMessageRequest).getMessages();
	}

	private static void deleteMessage(AmazonSQS client, String queueUrl, List<Message> messages, int index) {
		String messageRecieptHandle = messages.get(index).getReceiptHandle();
		DeleteMessageRequest dmr = new DeleteMessageRequest(queueUrl, messageRecieptHandle);
		client.deleteMessage(dmr);
	}

	private static void deleteQueue(AmazonSQS client, String queueUrl) {
		DeleteQueueRequest dqr = new DeleteQueueRequest(queueUrl);
		client.deleteQueue(dqr);
	}

	private static void prettyPrint(List<Message> messages) {
		for (Message message : messages) {
			System.out.println("  Message");
			System.out.println("    MessageId:     " + message.getMessageId());
			System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
			System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
			System.out.println("    Body:          " + message.getBody());
			for (Entry<String, String> entry : message.getAttributes().entrySet()) {
				System.out.println("  Attribute");
				System.out.println("    Name:  " + entry.getKey());
				System.out.println("    Value: " + entry.getValue());
			}
		}
		System.out.println();
	}
}

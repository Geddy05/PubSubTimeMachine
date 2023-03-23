package org.apache.beam.examples;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.joda.time.Instant;

public class PublishTest {
    public static void main(String... args) throws Exception {
        // TODO(developer): Replace these variables before running the sample.
        String projectId = "projectId";
        String topicId = "topicId";

        publishWithErrorHandlerExample(projectId, topicId);
    }

    public static List<String> example1() {
//        This is a happy path, each state is triggered at the elapsed time interval.
//        At 10:30 , Key 1 - state S1 with urgency of 5 mins is inserted ( trigger point is at 10:35)
//        At 10:34, Key 1 - state S2 with urgency of 2 mins is inserted (trigger point is at 10:36)
//        At 10:30, Key 2 - state S1 with urgency of 5 mins is inserted (trigger point is at 10:35)
//        At 10:35, Key 2 - state S2 with urgency of 2 mins is inserted (trigger point is at 10:37)
//        In the above case, S1 will be triggered at 10.35  and s2 will be triggered at 10:36 for Key 1. For key2, S1 will be triggered at 10.35 and S2 will be triggered at 10.37.

        long ts1 = Instant.now().getMillis();
        StateMachineObject stateMachineObject1 =  new StateMachineObject(ts1, "S1",1,150*1000);
        long ts2 = Instant.now().getMillis();
        StateMachineObject stateMachineObject2 =  new StateMachineObject(ts2, "S2",1,360*1000);
        long ts3 = Instant.now().getMillis();
        StateMachineObject stateMachineObject3 =  new StateMachineObject(ts3, "S1",2,190*1000);
        long ts4 = Instant.now().getMillis();
        StateMachineObject stateMachineObject4 =  new StateMachineObject(ts4, "S2",2,480*1000);
        System.out.println("item " + stateMachineObject1.getKey() +
                " ts: " + stateMachineObject1.getTriggerInMillis()+
                " humanreadable TS: " + Instant.ofEpochMilli(stateMachineObject1.getTriggerInMillis()));

        System.out.println("item " + stateMachineObject2.getKey() +
                " ts: " + stateMachineObject2.getTriggerInMillis()+
                " humanreadable TS: " + Instant.ofEpochMilli(stateMachineObject2.getTriggerInMillis()));

        System.out.println("item " + stateMachineObject3.getKey() +
                " ts: " + stateMachineObject3.getTriggerInMillis()+
                " humanreadable TS: " + Instant.ofEpochMilli(stateMachineObject3.getTriggerInMillis()));

        System.out.println("item " + stateMachineObject4.getKey() +
                " ts: " + stateMachineObject4.getTriggerInMillis()+
                " humanreadable TS: " + Instant.ofEpochMilli(stateMachineObject4.getTriggerInMillis()));


        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        try {
            String json1 = ow.writeValueAsString(stateMachineObject1);
            String json2 = ow.writeValueAsString(stateMachineObject2);
            String json3 = ow.writeValueAsString(stateMachineObject3);
            String json4 = ow.writeValueAsString(stateMachineObject4);
            List<String> messages = Arrays.asList(json1,json2,json3,json4);
            return messages;

        } catch (JsonProcessingException e) {
            return null;
        }
    }

    public static void publishWithErrorHandlerExample(String projectId, String topicId)
            throws IOException, InterruptedException {
        TopicName topicName = TopicName.of(projectId, topicId);
        Publisher publisher = null;

        try {
            // Create a publisher instance with default settings bound to the topic
            publisher = Publisher.newBuilder(topicName).build();

            List<String> messages = example1();

            for (final String message : messages) {
                ByteString data = ByteString.copyFromUtf8(message);
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

                // Once published, returns a server-assigned message id (unique within the topic)
                ApiFuture<String> future = publisher.publish(pubsubMessage);

                // Add an asynchronous callback to handle success / failure
                ApiFutures.addCallback(
                        future,
                        new ApiFutureCallback<String>() {

                            @Override
                            public void onFailure(Throwable throwable) {
                                if (throwable instanceof ApiException) {
                                    ApiException apiException = ((ApiException) throwable);
                                    // details on the API exception
                                    System.out.println(apiException.getStatusCode().getCode());
                                    System.out.println(apiException.isRetryable());
                                }
                                System.out.println("Error publishing message : " + message);
                            }

                            @Override
                            public void onSuccess(String messageId) {
                                // Once published, returns server-assigned message ids (unique within the topic)
                                System.out.println("Published message ID: " + messageId);
                            }
                        },
                        MoreExecutors.directExecutor());
            }
        } finally {
            if (publisher != null) {
                // When finished with the publisher, shutdown to free up resources.
                publisher.shutdown();
                publisher.awaitTermination(1, TimeUnit.MINUTES);
            }
        }
    }
}

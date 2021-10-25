package com.cybereason.xdr.transformer;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.pubsub.v1.ProjectTopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;

@SpringBootApplication
public class TransformerApplication {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransformerApplication.class);
    private static final String PUB_TOPIC = "trans2idm";
    private static final String PROJECT_NAME = "status-local";


    public static void main(String[] args) {
        createTopic();
        SpringApplication.run(TransformerApplication.class, args);
    }

    public static void createTopic() {
        CredentialsProvider credentialsProvider = NoCredentialsProvider.create();
        ManagedChannel channel = ManagedChannelBuilder.forTarget("localhost:8085").usePlaintext().build();
        TransportChannelProvider channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
        TopicAdminClient topicAdmin;
        ProjectTopicName topicName = ProjectTopicName.of(PROJECT_NAME, PUB_TOPIC);
        try {
            topicAdmin = TopicAdminClient.create(
                    TopicAdminSettings.newBuilder()
                            .setTransportChannelProvider(channelProvider)
                            .setCredentialsProvider(credentialsProvider)
                            .build());
            topicAdmin.createTopic(topicName);
            LOGGER.info("Topic " + PUB_TOPIC + " Created!");
        } catch (AlreadyExistsException e) {
            LOGGER.error("Topic " + PUB_TOPIC + " Already exists");
        } catch (IOException e) {
            LOGGER.error("An error occurred ", e.getMessage(), e);
        }
    }
}

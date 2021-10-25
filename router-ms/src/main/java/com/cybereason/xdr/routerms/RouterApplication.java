package com.cybereason.xdr.routerms;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.cloud.spring.pubsub.core.PubSubTemplate;
import com.google.cloud.spring.pubsub.integration.inbound.PubSubInboundChannelAdapter;
import com.google.cloud.spring.pubsub.integration.outbound.PubSubMessageHandler;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PushConfig;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

import java.io.IOException;

@SpringBootApplication
public class RouterApplication {
    private static final Logger LOGGER = LoggerFactory.getLogger(RouterApplication.class);
    private static final String SUB_TOPIC = "threat2router";
    private static final String PUB_TOPIC = "router2transparency";
    private static final String PROJECT_NAME = "status-local";
    private static final String SUBSCRIBER_NAME = "router-subscriber";

    private static final CredentialsProvider credentialsProvider = NoCredentialsProvider.create();
    private static final ManagedChannel channel = ManagedChannelBuilder.forTarget("localhost:8085").usePlaintext().build();
    private static final TransportChannelProvider channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
    private static final ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(PROJECT_NAME, SUBSCRIBER_NAME);
    private static final ProjectTopicName topicName = ProjectTopicName.of(PROJECT_NAME, SUB_TOPIC);


    public static void main(String[] args) {
        createTopic();
        createSubscription();
        SpringApplication.run(RouterApplication.class, args);
    }

    @Bean
    @ServiceActivator(inputChannel = "pubsubOutputChannel")
    public MessageHandler messageSender(PubSubTemplate pubsubTemplate) {
        return new PubSubMessageHandler(pubsubTemplate, PUB_TOPIC);
    }

    public static void createSubscription() {
        try {
            SubscriptionAdminSettings subscriptionAdminSettings = SubscriptionAdminSettings.newBuilder()
                    .setCredentialsProvider(credentialsProvider)
                    .setTransportChannelProvider(channelProvider)
                    .build();

            SubscriberStubSettings subscriberStubSettings = SubscriberStubSettings.newBuilder()
                    .setTransportChannelProvider(channelProvider)
                    .setCredentialsProvider(credentialsProvider)
                    .build();

            SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(subscriptionAdminSettings);
            GrpcSubscriberStub.create(subscriberStubSettings);
            subscriptionAdminClient.createSubscription(subscriptionName, topicName, PushConfig.getDefaultInstance(), 0);
        } catch (AlreadyExistsException e) {
            LOGGER.info("Subscription " + subscriptionName.getSubscription() + " Already exists");
        } catch (IOException e) {
            LOGGER.error("An error occurred ", e.getMessage(), e);
        }
    }

    public static void createTopic() {
        TopicAdminClient topicAdmin;
        ProjectTopicName topicName = ProjectTopicName.of("status-local", PUB_TOPIC);
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

    @Bean
    public PubSubInboundChannelAdapter messageChannelAdapter(
            @Qualifier("myInputChannel") MessageChannel inputChannel,
            PubSubTemplate pubSubTemplate) {
        PubSubInboundChannelAdapter adapter =
                new PubSubInboundChannelAdapter(pubSubTemplate, SUBSCRIBER_NAME);
        adapter.setOutputChannel(inputChannel);

        return adapter;
    }

    @Bean
    public MessageChannel myInputChannel() {
        return new DirectChannel();
    }


    @ServiceActivator(inputChannel = "myInputChannel")
    public void messageReceiver(String payload) {
        LOGGER.info("Message arrived! Payload: " + payload);
        messagingGateway.sendToPubsub(payload + ", router");
        LOGGER.info("Message Sent to: " + PUB_TOPIC);

    }

    @MessagingGateway(defaultRequestChannel = "pubsubOutputChannel")
    public interface PubsubOutboundGateway {
        void sendToPubsub(String text);
    }

    @Autowired
    private PubsubOutboundGateway messagingGateway;
}

package com.cybereason.xdr.transformer.controller;

import com.google.cloud.spring.pubsub.core.PubSubTemplate;
import com.google.cloud.spring.pubsub.integration.outbound.PubSubMessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.messaging.MessageHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TransformerController {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransformerController.class);
    private static final String PUB_TOPIC = "trans2idm";

    @Bean
    @ServiceActivator(inputChannel = "pubsubOutputChannel")
    public MessageHandler messageSender(PubSubTemplate pubsubTemplate) {
        return new PubSubMessageHandler(pubsubTemplate, PUB_TOPIC);
    }

    @MessagingGateway(defaultRequestChannel = "pubsubOutputChannel")
    public interface PubsubOutboundGateway {
        void sendToPubsub(String text);
    }

    @Autowired
    private PubsubOutboundGateway messagingGateway;


    @GetMapping("/publish")
    public String publishMessage() {
        messagingGateway.sendToPubsub("Hello! This is transformer");
        LOGGER.info("Message Sent!");
        return "Message published successfully";
    }
}

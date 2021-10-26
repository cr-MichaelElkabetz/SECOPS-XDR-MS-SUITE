package com.cybereason.xdr.transformer.controller;

import com.cybereason.xdr.transformer.model.Response;
import com.google.cloud.spring.pubsub.core.PubSubTemplate;
import com.google.cloud.spring.pubsub.integration.outbound.PubSubMessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.messaging.MessageHandler;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping(path = "/", produces = MediaType.APPLICATION_JSON_VALUE)
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

    @PostMapping("publish")
    public ResponseEntity<Response> publishMessage(@RequestBody String message) {
        messagingGateway.sendToPubsub(message + ", transform");
        LOGGER.info("Message Sent! message content: " + message);
        Response response = new Response("Message Received in topic! message content: " + message);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }
}

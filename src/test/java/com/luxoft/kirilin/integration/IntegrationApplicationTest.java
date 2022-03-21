package com.luxoft.kirilin.integration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.MessageChannel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
class IntegrationApplicationTest {

    @Autowired
    private MessageChannel incomeChannel;
    @Autowired
    private PublishSubscribeChannel outgoingChannel;

    @Test
    public void shouldReceiveTransformResultTest() throws InterruptedException {
        List<String> input = Arrays.asList("Item1", "Item2");
        incomeChannel.send(MessageBuilder
                .withPayload(input).build());

        List<String> results = new ArrayList<>();
        outgoingChannel.subscribe(msg -> results.add(msg.getPayload().toString()));

        for (int i = 10; results.size() < 2 && i >= 0; i--){
            Thread.sleep(1000);
            if(Thread.interrupted()){
                System.exit(1);
            }
        }

        assertEquals(2, results.size());
        assertEquals("1metI", results.get(0));
        assertEquals("ITEM2", results.get(1));
    }

    @Test
    void contextLoads() {
    }
}
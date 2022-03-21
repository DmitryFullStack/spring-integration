package com.luxoft.kirilin.integration;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.Transformer;
import org.springframework.integration.channel.ExecutorChannel;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.integration.transformer.AbstractTransformer;
import org.springframework.integration.transformer.GenericTransformer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

@SpringBootApplication
@EnableIntegration
public class IntegrationApplication {

    public static final Supplier<Long> MILLIS = () -> ThreadLocalRandom.current().nextLong(5000, 10000);

    public static void main(String[] args) {
        var ctx = SpringApplication.run(IntegrationApplication.class, args);

        PublishSubscribeChannel inputChannel = ctx.getBean("incomeChannel", PublishSubscribeChannel.class);

        List<String> input = Arrays.asList("Item1", "Item2", "Item3", "Item4");
        List<String> input2 = Arrays.asList("Item5", "Item6", "Item7", "Item8", "Item9", "Item10");
        inputChannel.send(MessageBuilder
                .withPayload(input).build());
        inputChannel.send(MessageBuilder
                .withPayload(input2).build());
    }

    @Bean
    public IntegrationFlow entryPoint() {
        return IntegrationFlows.from("incomeChannel")
                .split()
                .transform(Message.class, addTargetChannelNameTransformer())
                .log()
                .route("headers['channelName']")
                .get();
    }

    @Bean
    public IntegrationFlow aggregateResults() {
        return IntegrationFlows.from("aggregateChannel")
                .resequence()
                .channel("outgoingChannel")
                .handle(msgConsumer())
                .get();
    }

    @Bean
    @Transformer(inputChannel = "upperCaseChannel", outputChannel = "aggregateChannel")
    AbstractTransformer toUpperCaseTransformer() {
        return new AbstractTransformer() {
            @Override
            protected Object doTransform(Message<?> message) {
                try {
                    Thread.sleep(MILLIS.get());
                } catch (InterruptedException ignored) {
                }
                return message.getPayload().toString().toUpperCase();
            }
        };
    }

    @Bean
    @Transformer(inputChannel = "reverseChannel", outputChannel = "aggregateChannel")
    AbstractTransformer reverseTransformer() {
        return new AbstractTransformer() {
            @Override
            protected Object doTransform(Message<?> message) {
                try {
                    Thread.sleep(MILLIS.get());
                } catch (InterruptedException ignored) {
                }
                return new StringBuilder(message.getPayload().toString())
                        .reverse().toString();
            }
        };
    }

    @Bean
    public MessageChannel upperCaseChannel() {
        return new ExecutorChannel(Executors.newSingleThreadExecutor());
    }

    @Bean
    public MessageChannel reverseChannel() {
        return new ExecutorChannel(Executors.newSingleThreadExecutor());
    }

    @Bean
    public MessageChannel aggregateChannel() {
        return new PublishSubscribeChannel();
    }

    @Bean
    public MessageChannel incomeChannel() {
        return new PublishSubscribeChannel();
    }

    @Bean
    public PublishSubscribeChannel outgoingChannel() {
        return new PublishSubscribeChannel();
    }

    @Bean
    public MessageHandler msgConsumer() {
        return msg -> System.out.println(msg.getPayload());
    }

    private GenericTransformer<Message, Message<Object>> addTargetChannelNameTransformer() {
        return msg -> {
            int sequenceNumber = Objects.requireNonNull(msg.getHeaders().get("sequenceNumber", Integer.class));
            return MessageBuilder
                    .withPayload(msg.getPayload())
                    .copyHeaders(msg.getHeaders())
                    .setHeader("channelName",
                            sequenceNumber % 2 == 0 ? "upperCaseChannel" : "reverseChannel")
                    .build();
        };
    }
}

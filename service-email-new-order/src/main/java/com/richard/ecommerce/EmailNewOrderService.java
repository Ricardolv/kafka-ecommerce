package com.richard.ecommerce;

import com.richard.ecommerce.consumer.ConsumerService;
import com.richard.ecommerce.consumer.ServiceRunner;
import com.richard.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.ExecutionException;

import static com.richard.ecommerce.KafkaTopics.ECOMMERCE_SEND_EMAIL;

public class EmailNewOrderService implements ConsumerService<Order> {

    private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>();

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        new ServiceRunner(EmailNewOrderService::new).start(1);

    }

    @Override
    public String getConsumerGroup() {
        return EmailNewOrderService.class.getSimpleName();
    }

    @Override
    public String getTopic() {
        return ECOMMERCE_SEND_EMAIL.name();
    }

    @Override
    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {

        var message = record.value();

        System.out.println("---------------------------------------");
        System.out.println("Processing new order, prepering email");
        System.out.println("chave: " + record.key());
        System.out.println("message: " + record.value());
        System.out.println("order: " + message.getPayLoad());
        System.out.println("particao: " + record.partition());
        System.out.println("offiset: " + record.offset());

        var order = message.getPayLoad();
        var id = message.getId().continueWith(EmailNewOrderService.class.getSimpleName());
        var emailCode = "Tahanks you for your order! We are processing your order!";

        emailDispatcher.send(ECOMMERCE_SEND_EMAIL.name(), order.getEmail(), id, emailCode);

    }

}

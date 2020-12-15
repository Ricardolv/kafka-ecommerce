package com.richard.ecommerce;


import com.richard.ecommerce.consumer.ConsumerService;
import com.richard.ecommerce.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import static com.richard.ecommerce.KafkaTopics.ECOMMERCE_SEND_EMAIL;

public class EmailService implements ConsumerService<String> {

    public static void main(String[] args) {
        new ServiceRunner(EmailService::new).start(5);
    }

    @Override
    public String getConsumerGroup() {
        return EmailService.class.getSimpleName();
    }

    @Override
    public String getTopic() {
        return ECOMMERCE_SEND_EMAIL.name();
    }

    @Override
    public void parse(ConsumerRecord<String, Message<String>> record) {
        System.out.println("---------------------------------------");
        System.out.println("Send email!");
        System.out.println("chave: " + record.key());
        System.out.println("valor: " + record.value());
        System.out.println("particao: " + record.partition());
        System.out.println("offiset: " + record.offset());

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Email set");
    }


}

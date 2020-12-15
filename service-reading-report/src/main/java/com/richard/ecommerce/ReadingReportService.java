package com.richard.ecommerce;

import com.richard.ecommerce.consumer.ConsumerService;
import com.richard.ecommerce.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static com.richard.ecommerce.KafkaTopics.ECOMMERCE_USER_GENERATE_READING_REPORT;

public class ReadingReportService implements ConsumerService<User> {

    private static final Path SOURCE = new File("src/main/resources/report.txt").toPath();

    public static void main(String[] args) {

        new ServiceRunner(ReadingReportService::new).start(5);

    }

    @Override
    public void parse(ConsumerRecord<String, Message<User>> record) throws IOException {

        System.out.println("---------------------------------------");
        System.out.println("Processing report for " + record.value());

        var message = record.value();;
        var user = message.getPayLoad();
        var target = new File(user.getReportPath());
        IO.copyto(SOURCE, target);
        IO.append(target, "Created for " + user.getUuid());

        System.out.println("File created: " + target.getAbsolutePath());

    }

    @Override
    public String getTopic() {
        return ECOMMERCE_USER_GENERATE_READING_REPORT.name();
    }

    @Override
    public String getConsumerGroup() {
        return ReadingReportService.class.getSimpleName();
    }
}

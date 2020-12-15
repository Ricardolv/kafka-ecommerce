package com.richard.ecommerce.consumer;

import com.richard.ecommerce.ConsumerFunction;
import com.richard.ecommerce.KafkaTopics;
import com.richard.ecommerce.Message;
import com.richard.ecommerce.dispatcher.GsonSerializer;
import com.richard.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {

    private final KafkaConsumer<String, Message<T>> consumer;
    private ConsumerFunction<T> parse;

    public KafkaService(String groupID, String topic, ConsumerFunction<T> parse, Map<String, String> properties) {
        this(parse, groupID, properties);
        consumer.subscribe(Collections.singleton(topic));
    }

    public KafkaService(String groupID, Pattern topic, ConsumerFunction<T> parse, Map<String, String> properties) {
        this(parse, groupID, properties);
        consumer.subscribe(topic);
    }

    private KafkaService(ConsumerFunction<T> parse, String groupID, Map<String, String> properties) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(getProperties(groupID, properties));
    }

    public void run() throws ExecutionException, InterruptedException {
        try( var dispatcher = new KafkaDispatcher<>()) {
            while (true) {
                ConsumerRecords<String, Message<T>> records;
                records = consumer.poll(Duration.ofMillis(100));

                if (!records.isEmpty()) {
                    for (var record : records) {
                        try {
                            parse.consume(record);
                        } catch (Exception e) {
                            // only catches Exception because no matter which Exc3ception
                            // i want to recover and parse the next one
                            // so far, just logging the exception for this message
                            e.printStackTrace();
                            var message = record.value();
                            dispatcher.send(KafkaTopics.ECOMMERCE_DEAD_LETTER.name(),
                                            message.getId().toString(),
                                            message.getId().continueWith("DeadLetter"),
                                            new GsonSerializer().serialize("", message));
                        }
                    }
                }
            }
        }

    }

    private Properties getProperties(String groupID, Map<String, String> overrideProperties) {
        var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //earliest - latest
        properties.putAll(overrideProperties);
        return properties;
    }

    @Override
    public void close() {
        this.consumer.close();
    }
}

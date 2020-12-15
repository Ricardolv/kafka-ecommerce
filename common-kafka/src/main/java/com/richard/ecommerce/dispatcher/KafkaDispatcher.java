package com.richard.ecommerce.dispatcher;

import com.richard.ecommerce.CorrelationId;
import com.richard.ecommerce.Message;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.util.Objects.nonNull;

public class KafkaDispatcher<T> implements Closeable {

    private final KafkaProducer<String, Message<T>> producer;

    public KafkaDispatcher() {
       this.producer = new KafkaProducer<>(getProperties());
    }

    public void send(String topic, String key, CorrelationId correlationId, T payLoad) throws ExecutionException, InterruptedException {
        Future<RecordMetadata> future = sendAsync(topic, key, correlationId, payLoad);
        future.get();
    }

    public Future<RecordMetadata> sendAsync(String topic, String key, CorrelationId correlationId, T payLoad) {

        var value = new Message<>(correlationId.continueWith("_" + topic), payLoad);
        Callback callback = (data, ex) -> {

            if (nonNull(ex)) {
                ex.printStackTrace();
            } else {
                System.out.println("Sucesso mensagem particao: " + data.partition() + " / topico: " + data.topic() + "/ offset: " + data.offset() + "/ timestemp: " + data.timestamp());
            }

        };

        var record = new ProducerRecord<>(topic, key, value);
        return producer.send(record, callback);
    }

    private static Properties getProperties() {
        var properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
    }

    @Override
    public void close() {
        this.producer.close();
    }
}

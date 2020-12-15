package com.richard.ecommerce;

import com.richard.LocalDatabase;
import com.richard.ecommerce.consumer.ConsumerService;
import com.richard.ecommerce.consumer.ServiceRunner;
import com.richard.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import static com.richard.ecommerce.KafkaTopics.*;

public class FraudDetectorService implements ConsumerService<Order>  {

    private final LocalDatabase database;

    private FraudDetectorService() throws SQLException {
        this.database = new LocalDatabase("frauds_database");
        this.database.createIfNotExists("create table Orders (" +
                                            "uuid varchar(200) primary key, " +
                                            "is_fraud boolean)");
    }

    public static void main(String[] args) {
        new ServiceRunner<>(FraudDetectorService::new).start(1);
    }

    @Override
    public String getTopic() {
        return  ECOMMERCE_NEW_ORDER.name();
    }

    @Override
    public String getConsumerGroup() {
        return FraudDetectorService.class.getSimpleName();
    }

    private final KafkaDispatcher<Order> orderKafkaDispatcher = new KafkaDispatcher<>();

    @Override
    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException, SQLException {

        var message = record.value();
        var order = message.getPayLoad();

        System.out.println("---------------------------------------");
        System.out.println("Processing new order, cheking for fraud");
        System.out.println("chave: " + record.key());
        System.out.println("message: " + record.value());
        System.out.println("value: " + record.value());
        System.out.println("particao: " + record.partition());
        System.out.println("offiset: " + record.offset());

        if (wasProcessed(order)) {
            System.out.println("Order "+ order.getOrderId() +" was already processed");
            return;
        }

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (isFraud(order)) {

            this.database.update("insert into Orders (uuid, is_fraud) values (?, true)", order.getOrderId());

            // pretending that the fraud happens when the amount is >= 4500
            System.out.println("Order is a fraud!!!!");
            orderKafkaDispatcher.send(ECOMMERCE_ORDER_REJECTED.name(), order.getEmail(), message.getId().continueWith(FraudDetectorService.class.getSimpleName()), order);
        } else {

            this.database.update("insert into Orders (uuid, is_fraud) values (?, false)", order.getOrderId());
            System.out.println("Approved: " + order);
            orderKafkaDispatcher.send(ECOMMERCE_ORDER_APPROVED.name(), order.getEmail(), message.getId().continueWith(FraudDetectorService.class.getSimpleName()), order);
        }

    }

    private boolean wasProcessed(Order order) throws SQLException {
        var results = this.database.query("select uuid from Orders where uuid = ? limit 1", order.getOrderId());
        return results.next();
    }

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }

}

package com.richard.ecommerce;

import com.richard.LocalDatabase;
import com.richard.ecommerce.consumer.ConsumerService;
import com.richard.ecommerce.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.SQLException;
import java.util.UUID;

import static com.richard.ecommerce.KafkaTopics.ECOMMERCE_NEW_ORDER;

public class CreateUserService implements ConsumerService<Order> {

    private final LocalDatabase database;

    private CreateUserService() throws SQLException {
      this.database = new LocalDatabase("users_database");
      this.database.createIfNotExists("create table Users (" +
                                          "uuid varchar(200) primary key, " +
                                          "email varchar(200))");
    }

    public static void main(String[] args) {
       new ServiceRunner<>(CreateUserService::new).start(1);
    }

    @Override
    public String getTopic() {
        return ECOMMERCE_NEW_ORDER.name();
    }

    @Override
    public String getConsumerGroup() {
        return CreateUserService.class.getSimpleName();
    }

    @Override
    public void parse(ConsumerRecord<String, Message<Order>> record) throws SQLException {

        System.out.println("---------------------------------------");
        System.out.println("Processing new order, cheking for user");
        System.out.println("valor: " + record.value());

        var order = record.value().getPayLoad();

        if (isNewUser(order.getEmail())) {
            insertNewUser(order.getEmail());
        }

    }

    private void insertNewUser(String email) throws SQLException {
        var uuid = UUID.randomUUID().toString();
        this.database.update("insert into Users (uuid, email) values (?,?)", uuid, email);

        System.out.println("User uuid: " + uuid + " and email: " + email + "add");
    }

    private boolean isNewUser(String email) throws SQLException {
        var results = this.database.query("select uuid from Users where email = ? limit 1", email);
        return !results.next();
    }

}

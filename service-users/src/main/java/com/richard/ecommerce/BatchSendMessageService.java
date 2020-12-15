package com.richard.ecommerce;

import com.richard.ecommerce.consumer.KafkaService;
import com.richard.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.richard.ecommerce.KafkaTopics.ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS;

public class BatchSendMessageService {

    private final Connection connection;
    private final KafkaDispatcher<User> userDispatcher = new KafkaDispatcher<>();

    public BatchSendMessageService() throws SQLException {
        String url = "jdbc:sqlite:target/users_databases.db";
        this.connection = DriverManager.getConnection(url);

        try {
            this.connection.createStatement().execute("create table Users (uuid varcahr(200) primary key, email varchar(200))");
        } catch (SQLException ex) {
            /// be careful, the sql could be wrong, be realy careful
            ex.printStackTrace();
        }

    }

    public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException {

        var batchSendMessageService = new BatchSendMessageService();
        try (var service = new KafkaService(BatchSendMessageService.class.getSimpleName(),
                ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS.name(),
                batchSendMessageService::parse,
                Map.of())) {
            service.run();
        }

    }

    private void parse(ConsumerRecord<String, Message<String>> record) throws ExecutionException, InterruptedException, SQLException {

        System.out.println("---------------------------------------");
        System.out.println("Processing new batch");

        var message = record.value();
        System.out.println("Message: " + message.getPayLoad());

        for (User user : getAllUsers()) {
            userDispatcher.sendAsync(message.getPayLoad(),
                    user.getUuid(),
                    message.getId().continueWith(BatchSendMessageService.class.getSimpleName()),
                    user);
            System.out.println("Enviei para "  + user);
        }

    }

    private List<User> getAllUsers() throws SQLException {

        var results = this.connection.prepareStatement("select uuid from Users").executeQuery();
        List<User> users = new ArrayList<>();

        while (results.next()) {
            users.add(new User(results.getString(1)));
        }

        return users;
    }
}

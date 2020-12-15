package com.richard.ecommerce;

import com.richard.ecommerce.dispatcher.KafkaDispatcher;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static com.richard.ecommerce.KafkaTopics.ECOMMERCE_NEW_ORDER;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

       try (var orderDispatcher = new KafkaDispatcher<Order>()) {
               for (var i = 0; i < 10; i++) {

               var orderId = UUID.randomUUID().toString();
               var amount = BigDecimal.valueOf(Math.random() * 5000 + 1);
               var email = Math.random() + "@email.com";
               var order = new Order(orderId, email, amount);
               var id = new CorrelationId(NewOrderMain.class.getSimpleName());

               orderDispatcher.send(ECOMMERCE_NEW_ORDER.name(), email, id, order);
           }
       }
    }

}

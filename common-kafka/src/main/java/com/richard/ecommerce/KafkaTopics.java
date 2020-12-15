package com.richard.ecommerce;

public enum KafkaTopics {

    ECOMMERCE_NEW_ORDER,
    ECOMMERCE_SEND_EMAIL,
    ECOMMERCE_ORDER_APPROVED,
    ECOMMERCE_ORDER_REJECTED,
    ECOMMERCE_USER_GENERATE_READING_REPORT,
    ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS,
    ECOMMERCE_DEAD_LETTER;
}

package com.richard.ecommerce.consumer;

public interface ServiceFactory<T> {

    ConsumerService<T> create() throws Exception;

}

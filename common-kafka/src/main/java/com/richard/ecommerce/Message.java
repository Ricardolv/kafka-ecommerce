package com.richard.ecommerce;

public class Message<T> {

    private final CorrelationId id;
    private final T payLoad;

    public Message(CorrelationId id, T payLoad) {
        this.id = id;
        this.payLoad = payLoad;
    }

    public CorrelationId getId() {
        return id;
    }

    public T getPayLoad() {
        return payLoad;
    }

    @Override
    public String toString() {
        return "Message{" +
                "id=" + id +
                ", payLoad=" + payLoad +
                '}';
    }
}

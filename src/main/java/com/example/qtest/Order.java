package com.example.qtest;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class Order {
    @JsonProperty("orderId")
    private String orderId;

    @JsonProperty("orderItems")
    private List<OrderItem> orderItems;

    @JsonProperty("timestamp")
    private long timestamp;

    @JsonProperty("status")
    private String status; // "PENDING", "ACCEPTED", "REJECTED", "RELEASED"

    @JsonProperty("chefName")
    private String chefName;

    public Order() {
    }

    public Order(String orderId, List<OrderItem> orderItems) {
        this.orderId = orderId;
        this.orderItems = orderItems;
        this.timestamp = System.currentTimeMillis();
        this.status = "PENDING";
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public List<OrderItem> getOrderItems() {
        return orderItems;
    }

    public void setOrderItems(List<OrderItem> orderItems) {
        this.orderItems = orderItems;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getChefName() {
        return chefName;
    }

    public void setChefName(String chefName) {
        this.chefName = chefName;
    }
}


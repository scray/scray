package org.scray.sync.rest.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Objects;
import java.util.UUID;

import org.scray.sync.rest.extensions.http_publish.dto.Delivery;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Subscription {

    private String env;
    private String type;
    private Delivery delivery;

    public Subscription() {
    }

    public Subscription(String id, String env, String type, Delivery delivery) {
        this.env = env;
        this.type = type;
        this.delivery = delivery;
    }


    public String getEnv() {
        return env;
    }

    public void setEnv(String env) {
        this.env = env;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Delivery getDelivery() {
        return delivery;
    }

    public void setDelivery(Delivery delivery) {
        this.delivery = delivery;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Subscription that)) return false;
        return Objects.equals(env, that.env)
                && Objects.equals(type, that.type)
                && Objects.equals(delivery, that.delivery);
    }

    @Override
    public int hashCode() {
        return Objects.hash(env, type, delivery);
    }

    @Override
    public String toString() {
        return "Subscription{env='" + env + "', type='" + type
                + "', delivery=" + delivery + '}';
    }
}




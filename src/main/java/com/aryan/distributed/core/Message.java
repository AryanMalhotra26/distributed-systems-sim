package com.aryan.distributed.core;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class Message {
    public enum Type {
        ELECTION,
        OK,
        COORDINATOR,
        RING_ELECTION,
        RING_COORDINATOR,
        PAXOS_PREPARE,
        PAXOS_PROMISE,
        PAXOS_ACCEPT_REQUEST,
        PAXOS_ACCEPTED,
        PAXOS_COMMIT,
        HEARTBEAT
    }

    private final Type type;
    private final int from;
    private final int to;
    private final long createdAtMillis;
    private final Map<String, Object> payload;

    public Message(Type type, int from, int to, Map<String, Object> payload) {
        this.type = Objects.requireNonNull(type, "type");
        this.from = from;
        this.to = to;
        this.createdAtMillis = System.currentTimeMillis();
        this.payload = Collections.unmodifiableMap(new HashMap<>(payload == null ? Map.of() : payload));
    }

    public static Message of(Type type, int from, int to) {
        return new Message(type, from, to, Map.of());
    }

    public Message with(String key, Object value) {
        Map<String, Object> values = new HashMap<>(payload);
        values.put(key, value);
        return new Message(type, from, to, values);
    }

    public Type getType() {
        return type;
    }

    public int getFrom() {
        return from;
    }

    public int getTo() {
        return to;
    }

    public long getCreatedAtMillis() {
        return createdAtMillis;
    }

    public Map<String, Object> getPayload() {
        return payload;
    }

    public int payloadAsInt(String key, int defaultValue) {
        Object value = payload.get(key);
        return value instanceof Number number ? number.intValue() : defaultValue;
    }

    public long payloadAsLong(String key, long defaultValue) {
        Object value = payload.get(key);
        return value instanceof Number number ? number.longValue() : defaultValue;
    }

    public String payloadAsString(String key) {
        Object value = payload.get(key);
        return value == null ? null : value.toString();
    }

    public Object payload(String key) {
        return payload.get(key);
    }
}

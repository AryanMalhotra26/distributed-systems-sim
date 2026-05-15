package com.aryan.distributed.core;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class MessageBus implements AutoCloseable {
    private final ConcurrentHashMap<Integer, BlockingQueue<Message>> inboxes = new ConcurrentHashMap<>();
    private final NetworkSimulator networkSimulator;

    public MessageBus() {
        this(new NetworkSimulator());
    }

    public MessageBus(NetworkSimulator networkSimulator) {
        this.networkSimulator = networkSimulator;
    }

    public void register(Node node) {
        inboxes.put(node.getId(), node.inbox());
    }

    public void unregister(int nodeId) {
        inboxes.remove(nodeId);
    }

    public boolean isRegistered(int nodeId) {
        return inboxes.containsKey(nodeId);
    }

    public Set<Integer> registeredNodeIds() {
        return Set.copyOf(inboxes.keySet());
    }

    public void send(Message message) {
        networkSimulator.route(message, () -> {
            BlockingQueue<Message> destination = inboxes.get(message.getTo());
            if (destination != null) {
                destination.offer(message);
            }
        });
    }

    public void broadcast(int from, Collection<Integer> recipients, Message.Type type, Map<String, Object> payload) {
        for (Integer recipient : recipients) {
            send(new Message(type, from, recipient, payload));
        }
    }

    @Override
    public void close() {
        networkSimulator.close();
    }
}

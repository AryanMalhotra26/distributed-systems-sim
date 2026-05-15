package com.aryan.distributed.core;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class Node implements Runnable {
    private final int id;
    private final MessageBus messageBus;
    private final BlockingQueue<Message> inbox = new LinkedBlockingQueue<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread worker;

    protected Node(int id, MessageBus messageBus) {
        this.id = id;
        this.messageBus = Objects.requireNonNull(messageBus, "messageBus");
        this.messageBus.register(this);
    }

    public int getId() {
        return id;
    }

    public MessageBus getMessageBus() {
        return messageBus;
    }

    public BlockingQueue<Message> inbox() {
        return inbox;
    }

    public boolean isRunning() {
        return running.get();
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            worker = new Thread(this, getClass().getSimpleName() + "-" + id);
            worker.start();
        }
    }

    public void shutdown() {
        if (running.compareAndSet(true, false)) {
            messageBus.unregister(id);
            if (worker != null) {
                worker.interrupt();
            }
            onStop();
        }
    }

    protected void send(Message message) {
        messageBus.send(message);
    }

    protected void send(Message.Type type, int to) {
        send(Message.of(type, id, to));
    }

    @Override
    public void run() {
        onStart();
        try {
            while (running.get()) {
                Message message = inbox.poll(100, TimeUnit.MILLISECONDS);
                if (message != null) {
                    handleMessage(message);
                }
            }
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
        } finally {
            running.set(false);
        }
    }

    protected void onStart() {
    }

    protected void onStop() {
    }

    protected abstract void handleMessage(Message message);
}

package dev.dess.broker;

import java.io.IOException;

final class ConnectionWriter implements Runnable {

    private final ClientConnection connection;

    ConnectionWriter(ClientConnection connection) {
        this.connection = connection;
    }

    @Override
    public void run() {
        Thread.currentThread().setName("dess-writer-" + connection.id());
        try {
            while (!connection.isClosing()) {
                Subscription subscription = connection.pollReady();
                if (subscription != null) {
                    deliver(subscription);
                }
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    private void deliver(Subscription subscription) {
        Topic topic = subscription.topic();
        Topic.Delivery delivery = topic.claim(subscription);
        if (delivery == null) {
            topic.settle(subscription);
            return;
        }
        connection.beginWriting();
        try {
            connection.writeDeliver(topic.name(), delivery.offset(), delivery.payload());
        } catch (IOException ex) {
            connection.cleanup();
            return;
        } finally {
            connection.endWriting();
        }
        topic.advance(subscription);
    }
}

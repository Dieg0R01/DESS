package dev.dess.broker;

import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.ErrorCode;
import dev.dess.common.protocol.Frame;
import dev.dess.common.protocol.FrameCodec;
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
                if (connection.isSlow()) {
                    connection.failSlow();
                    return;
                }
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
        if (connection.isSlow()) {
            connection.failSlow();
            return;
        }
        Topic topic = subscription.topic();
        Topic.Delivery delivery = topic.claim(subscription);
        if (delivery == null) {
            topic.settle(subscription);
            return;
        }
        byte[] payload = delivery.payload();
        if (payload == null) {
            try {
                payload = topic.retainedPayload(delivery.filePosition());
            } catch (IOException ex) {
                connection.cleanup();
                return;
            }
        }
        topic.handOff(subscription);
        connection.beginWriting();
        try {
            connection.writeDeliver(topic.name(), delivery.offset(), payload);
        } catch (IOException ex) {
            connection.cleanup();
            return;
        } finally {
            connection.endWriting();
        }
        if (connection.isSlow()) {
            connection.failSlow();
            return;
        }
        topic.advance(subscription);
    }

    static Frame slowError() {
        return new Frame(Command.ERROR, FrameCodec.errorHeader(ErrorCode.SLOW_CONSUMER.name(), "slow consumer"), new byte[0]);
    }
}

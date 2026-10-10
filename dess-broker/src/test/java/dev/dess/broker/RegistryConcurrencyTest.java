package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.Frame;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("dess-accept-threads")
class RegistryConcurrencyTest {

    @Test
    void planLeavesTheSubscriberSetOfEachTopic() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start(config())) {
            List<Actor> actors = new ArrayList<>();
            try {
                for (int index = 0; index < 100; index++) {
                    ProtocolClient client = ProtocolClient.connect(harness.port());
                    client.setReadTimeoutMillis(15_000);
                    client.hello();
                    actors.add(new Actor(index, Long.parseLong(clientId(logs, client)), client));
                }
                logs.quiet();
                AtomicReference<Throwable> failure = new AtomicReference<>();
                List<Thread> threads = new ArrayList<>();
                CountDownLatch started = new CountDownLatch(actors.size());
                for (Actor actor : actors) {
                    Thread thread = new Thread(() -> runPlan(actor, started, failure), "dess-registry-" + actor.index());
                    threads.add(thread);
                    thread.start();
                }
                assertTrue(started.await(5, TimeUnit.SECONDS));
                for (Thread thread : threads) {
                    thread.join(20_000);
                    assertFalse(thread.isAlive(), thread.getName());
                }
                assertNull(failure.get());
                for (int topic = 0; topic < 10; topic++) {
                    Set<Long> expected = new HashSet<>();
                    for (Actor actor : actors) {
                        if (actor.index() % 10 == topic && actor.index() % 3 != 0) {
                            expected.add(actor.id());
                        }
                    }
                    assertEquals(expected, harness.subscribers("t" + topic), "t" + topic);
                }
            } finally {
                for (Actor actor : actors) {
                    actor.client().close();
                }
            }
        }
    }

    @Test
    void disconnectDuringSubscribeLeavesTheClientOnNoTopic() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start(config())) {
            for (int attempt = 0; attempt < 200; attempt++) {
                try (ProtocolClient client = ProtocolClient.connect(harness.port())) {
                    client.setReadTimeoutMillis(2_000);
                    client.hello();
                    String id = clientId(logs, client);
                    String topic = "race-" + attempt;
                    CountDownLatch writing = new CountDownLatch(1);
                    AtomicReference<Throwable> failure = new AtomicReference<>();
                    Thread closer = new Thread(() -> abort(client, writing, failure), "dess-registry-closer");
                    closer.start();
                    assertTrue(writing.await(2, TimeUnit.SECONDS));
                    try {
                        client.subscribe(topic);
                    } catch (IOException ex) {
                        // The closer dropped the socket while the subscribe was in flight.
                    }
                    closer.join(2_000);
                    assertFalse(closer.isAlive());
                    assertNull(failure.get());
                    assertTrue(logs.await(Duration.ofSeconds(2), "event=client.disconnected", "clientId=" + id + " "), logs.lines()::toString);
                    assertTrue(harness.subscribers(topic).isEmpty(), topic);
                }
            }
        }
    }

    private static void runPlan(Actor actor, CountDownLatch started, AtomicReference<Throwable> failure) {
        String topic = "t" + (actor.index() % 10);
        byte[] body = new byte[] {1};
        try {
            started.countDown();
            long end = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (System.nanoTime() < end && failure.get() == null) {
                actor.client().subscribe(topic);
                actor.client().publish(topic, body);
                readUntil(actor.client(), Command.ACK, Command.ERROR);
                actor.client().unsubscribe(topic);
            }
            if (actor.index() % 3 != 0) {
                actor.client().subscribe(topic);
            } else {
                actor.client().unsubscribe(topic);
            }
            actor.client().ping();
            readUntil(actor.client(), Command.PONG);
        } catch (Throwable ex) {
            failure.compareAndSet(null, ex);
        }
    }

    private static void abort(ProtocolClient client, CountDownLatch writing, AtomicReference<Throwable> failure) {
        writing.countDown();
        try {
            client.abort();
        } catch (IOException ex) {
            failure.compareAndSet(null, ex);
        }
    }

    private static Frame readUntil(ProtocolClient client, Command... commands) throws IOException {
        while (true) {
            Frame frame = client.readFrame();
            for (Command command : commands) {
                if (frame.command() == command) {
                    return frame;
                }
            }
        }
    }

    private static String clientId(LogCapture logs, ProtocolClient client) throws InterruptedException {
        String remote = "remoteAddress=" + client.endpoint();
        assertTrue(logs.await(Duration.ofSeconds(2), "event=client.connected", remote));
        for (String line : logs.lines()) {
            if (line.contains("event=client.connected") && line.contains(remote)) {
                int start = line.indexOf("clientId=") + "clientId=".length();
                int end = line.indexOf(' ', start);
                return line.substring(start, end < 0 ? line.length() : end);
            }
        }
        throw new AssertionError(logs.lines());
    }

    private static BrokerConfig config() {
        return new BrokerConfig(0, 128, BrokerConfig.DEFAULT_MAX_PAYLOAD_BYTES, 120_000,
                BrokerConfig.DEFAULT_MAX_IN_FLIGHT, 1024, 1024, 120_000, 200,
                BrokerConfig.DEFAULT_ACK_TIMEOUT_MILLIS, true, List.of());
    }

    private record Actor(int index, long id, ProtocolClient client) {
    }
}

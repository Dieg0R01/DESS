# DESS — Distributed Event Streaming System

> A publish/subscribe message broker written from scratch in plain Java, built as the practical project for a **Distributed Systems** course in Computer Engineering.

DESS is a small event streaming platform inspired by the architecture of systems like Apache Kafka and RabbitMQ. Producers publish events to named **topics**, a central **broker** buffers, persists and routes those events, and consumers subscribed to a topic receive them in real time. An HTTP **admin API** allows the broker to be inspected and managed from the outside.

The goal is not to compete with production brokers, but to implement by hand — on top of the JDK — the building blocks that a distributed system relies on: networking over TCP and UDP, multithreading and synchronization, durable storage, XML configuration, TLS encryption, message integrity and a web management interface.

> **Status:** design phase. No code has been written yet. This document describes the target architecture and the incremental plan to build it.

---

## Table of contents

- [Goals and non-goals](#goals-and-non-goals)
- [Architecture](#architecture)
- [Message lifecycle](#message-lifecycle)
- [Wire protocol](#wire-protocol)
- [Module map](#module-map)
- [Technology stack](#technology-stack)
- [Configuration](#configuration)
- [Getting started](#getting-started)
- [Development phases](#development-phases)
- [Open design questions](#open-design-questions)

---

## Goals and non-goals

### Goals

- **Reliable, ordered delivery** between producers and consumers over persistent TCP connections.
- **Concurrent broker engine** able to serve many clients at once without one slow client blocking the rest.
- **Decoupled ingestion and delivery** through per-topic in-memory queues (producer–consumer pattern).
- **Durability:** messages survive a broker restart thanks to an append-only log on disk.
- **Zero-configuration discovery:** clients find the broker on the local network via UDP broadcast.
- **Security:** TLS-encrypted channels, certificate-based authentication of producers and SHA-256 integrity checks on every message.
- **Observability and control:** a REST API to list, create and delete topics and to read runtime metrics.

### Non-goals (for now)

- Multi-broker clustering, replication or leader election.
- Topic partitioning and consumer groups.
- Exactly-once delivery semantics.
- Compatibility with the Kafka, AMQP or MQTT wire protocols.

These may be revisited once the five core phases are complete.

---

## Architecture

The system is made of three kinds of independent processes that communicate over the network, plus an administrative client.

```mermaid
flowchart LR
    subgraph Clients
        PA["Producer A"]
        PB["Producer B"]
        C1["Consumer 1<br/>subscribed to topic X"]
        C2["Consumer 2<br/>subscribed to topic Y"]
    end

    ADM["Admin client<br/>(HTTP)"]

    subgraph Broker["DESS Broker"]
        direction TB
        DISC["Discovery responder<br/>(UDP)"]
        NET["Connection acceptor<br/>(TCP / TLS)"]
        POOL["Client handler<br/>thread pool"]
        REG["Topic registry<br/>(ConcurrentHashMap)"]
        Q["Per-topic queues<br/>(BlockingQueue)"]
        DSP["Topic dispatchers"]
        LOG["Append-only log writer"]
        API["Admin REST API"]
        CFG["Config loader<br/>(JAXB)"]
    end

    DISK[("Disk<br/>message logs + config.xml")]

    PA -- "PUBLISH over TLS" --> NET
    PB -- "PUBLISH over TLS" --> NET
    C1 -- "SUBSCRIBE over TLS" --> NET
    C2 -- "SUBSCRIBE over TLS" --> NET
    Clients -. "UDP broadcast discovery" .-> DISC

    NET --> POOL
    POOL --> REG
    POOL --> LOG
    POOL --> Q
    Q --> DSP
    DSP -- "deliver over TLS" --> C1
    DSP -- "deliver over TLS" --> C2

    ADM -- "REST / HTTP" --> API
    API --> REG

    LOG --> DISK
    CFG --> DISK
```

### Components

| Component | Responsibility |
|-----------|----------------|
| **Producer** | Generates events (audit logs, metrics, transactions…) and publishes them to a topic. |
| **Consumer** | Subscribes to one or more topics and processes incoming messages as they arrive. |
| **Broker** | Accepts connections, validates and persists messages, buffers them per topic and fans them out to subscribers. |
| **Admin client** | Uses the broker's REST API to inspect state, manage topics and read metrics. |

### Broker internals

- **Connection acceptor.** A `ServerSocket` (later `SSLServerSocket`) runs `accept()` in a loop and hands every new socket to an `ExecutorService`, so serving one client never blocks the others. `setSoTimeout()` is used to detect clients that disappear silently.
- **Topic registry.** A `ConcurrentHashMap<String, Topic>` holds the topic catalogue and each topic's set of active subscribers, allowing safe concurrent reads and writes.
- **Per-topic queues.** Each topic owns a bounded `BlockingQueue`. Client handler threads `put()` incoming messages; a dedicated dispatcher thread per topic `take()`s them and delivers them to every subscriber, blocking naturally when the queue is empty.
- **Flow control.** A `Semaphore` caps the number of in-flight, not-yet-delivered messages held in memory, applying back-pressure to producers when the broker is saturated.
- **Delivery acknowledgements.** When a message requires confirmation from *N* consumers, the broker waits on a `CountDownLatch(N)` until all ACKs arrive (or a timeout expires).
- **Persistence.** Every accepted message is appended to a binary log through `DataOutputStream` over a `BufferedOutputStream`, so it can be replayed after a restart.
- **Discovery responder.** A `DatagramSocket` listens on a well-known UDP port. Clients broadcast a discovery request and the broker replies with its IP address and TCP port, resolved through `NetworkInterface` / `InetAddress`.
- **Admin API.** A lightweight embedded HTTP server exposes resource-oriented endpoints for management and monitoring.

---

## Message lifecycle

The end-to-end journey of a single message, once all phases are in place:

```mermaid
sequenceDiagram
    autonumber
    participant C as Consumer
    participant P as Producer
    participant B as Broker
    participant D as Disk log
    participant A as Admin

    Note over B: Startup: load config.xml (JAXB),<br/>open TLS port and UDP discovery port

    C->>B: UDP broadcast: DISCOVER
    B-->>C: UDP reply: broker IP + TLS port
    C->>B: TLS connect, SUBSCRIBE sales
    B->>B: Register subscriber in topic registry

    P->>B: TLS connect (client certificate)
    P->>P: Compute SHA-256 of payload
    P->>B: PUBLISH sales, payload, hash
    B->>B: Recompute hash and verify integrity
    B->>D: Append message to binary log
    B->>B: put() into the "sales" queue
    B->>B: Dispatcher take()s the message
    B-->>C: DELIVER sales, payload, hash
    C-->>B: ACK

    A->>B: GET /metrics
    B-->>A: 200 OK (throughput, threads, memory)
```

---

## Wire protocol

Clients and broker talk through a simple, line-oriented text protocol over TCP. Each frame is a single line terminated by `\n`, with fields separated by `:`.

| Command | Direction | Format | Description |
|---------|-----------|--------|-------------|
| `SUBSCRIBE` | client → broker | `SUBSCRIBE:<topic>` | Register the connection as a subscriber of `<topic>`. |
| `UNSUBSCRIBE` | client → broker | `UNSUBSCRIBE:<topic>` | Stop receiving messages from `<topic>`. |
| `PUBLISH` | client → broker | `PUBLISH:<topic>:<payload>:<sha256>` | Publish a message. The hash is verified by the broker. |
| `DELIVER` | broker → client | `DELIVER:<topic>:<payload>:<sha256>` | A message pushed to a subscriber. |
| `ACK` | both | `ACK:<message-id>` | Confirmation of reception / processing. |
| `ERROR` | broker → client | `ERROR:<code>:<reason>` | Rejected request (unknown topic, bad hash, unauthorized…). |

Discovery uses a separate UDP exchange (`DISCOVER` request, broker address in the reply). The exact framing, payload encoding and message identifiers are still open — see [Open design questions](#open-design-questions).

---

## Module map

DESS is organized as a multi-module build. Each module maps to one part of the system and has a clearly bounded responsibility.

```text
dess/
├── dess-common/        Shared protocol and domain model
├── dess-broker/        The broker server
├── dess-client/        Producer and consumer client library + demo apps
├── dess-admin/         Command-line admin client for the REST API
└── config/             Sample config.xml, keystores and scripts (keytool)
```

| Module | Main packages | Responsibility |
|--------|---------------|----------------|
| `dess-common` | `protocol`, `model`, `security` | Frame encoding/decoding, `Message` and `Topic` value types, SHA-256 hashing utilities. No networking and no threads. |
| `dess-broker` | `net`, `engine`, `storage`, `config`, `discovery`, `admin` | TCP/TLS acceptor and client handlers; topic registry, queues and dispatchers; append-only log; JAXB configuration; UDP discovery responder; embedded REST API. |
| `dess-client` | `producer`, `consumer`, `discovery` | Reusable `Producer` and `Consumer` classes that handle discovery, connection, hashing and ACKs, plus runnable demo applications. |
| `dess-admin` | `cli`, `http` | Admin tool built on `java.net.http.HttpClient` to call the REST API. |

Dependency direction is strictly one-way: `dess-broker`, `dess-client` and `dess-admin` depend on `dess-common`; none of them depend on each other.

### Mapping to course topics

| Course topic | Where it lives |
|--------------|----------------|
| TCP sockets (`Socket`, `ServerSocket`) | `dess-broker/net`, `dess-client` |
| UDP datagrams and broadcast (`DatagramSocket`, `NetworkInterface`) | `dess-broker/discovery`, `dess-client/discovery` |
| Thread pools (`ExecutorService`) | `dess-broker/net` |
| Concurrent collections and blocking queues | `dess-broker/engine` |
| Synchronizers (`CountDownLatch`, `Semaphore`) | `dess-broker/engine` |
| Java I/O streams and file persistence | `dess-broker/storage` |
| XML binding (JAXB) | `dess-broker/config` |
| TLS with JSSE (`SSLSocket`, `keytool`) | `dess-broker/net`, `dess-client`, `config/` |
| Hashing and integrity (SHA-256) | `dess-common/security` |
| REST services and HTTP clients | `dess-broker/admin`, `dess-admin` |

---

## Technology stack

| Concern | Choice |
|---------|--------|
| Language | Java 21 (LTS) |
| Build | Maven (multi-module) |
| Networking | `java.net` (TCP/UDP), `javax.net.ssl` (JSSE) |
| Concurrency | `java.util.concurrent` |
| Persistence | `java.io` binary streams |
| Configuration | Jakarta XML Binding (JAXB) |
| Admin API | Embedded HTTP server (JAX-RS implementation or the JDK `HttpServer`, to be decided) |
| Admin client | `java.net.http.HttpClient` |
| Testing | JUnit 5 |

Frameworks that would hide the distributed-systems mechanics (Spring, Netty, Kafka client libraries, etc.) are intentionally avoided. Note that JAXB has not shipped with the JDK since Java 11, so it is pulled in as an explicit Jakarta dependency.

---

## Configuration

The broker reads its settings from `config.xml` at startup, mapped to Java objects through JAXB (`@XmlRootElement`, `@XmlElement`, `Unmarshaller`). Runtime changes, such as topics created through the API, can be written back with a `Marshaller`.

Tentative shape:

```xml
<broker>
    <network>
        <tcpPort>8443</tcpPort>
        <discoveryPort>9999</discoveryPort>
        <adminPort>8080</adminPort>
        <clientTimeoutMs>30000</clientTimeoutMs>
    </network>
    <engine>
        <queueCapacity>10000</queueCapacity>
        <maxInFlightMessages>50000</maxInFlightMessages>
        <handlerThreads>32</handlerThreads>
    </engine>
    <storage>
        <logDirectory>./data/logs</logDirectory>
    </storage>
    <security>
        <keystore>./config/broker.keystore</keystore>
        <truststore>./config/broker.truststore</truststore>
    </security>
    <topics>
        <topic>sales</topic>
        <topic>sensor-temperature</topic>
    </topics>
</broker>
```

---

## Getting started

> 🚧 **Placeholder.** This section will be filled in as each phase lands.

### Prerequisites

- JDK 21+
- Maven 3.9+

### Build

```bash
# TODO: available from Phase 1
mvn clean verify
```

### Run the broker

```bash
# TODO: available from Phase 1
java -jar dess-broker/target/dess-broker.jar --config config/config.xml
```

### Run a consumer and a producer

```bash
# TODO: available from Phase 1
java -jar dess-client/target/dess-client.jar consume --topic sales
java -jar dess-client/target/dess-client.jar produce --topic sales --message "hello"
```

### Generate TLS certificates

```bash
# TODO: available from Phase 4 (keytool scripts under config/)
```

### Query the admin API

```bash
# TODO: available from Phase 5
curl http://localhost:8080/metrics
```

---

## Development phases

The system is built incrementally in five sequential phases. Each phase produces a working, demonstrable system and adds one layer of distributed-systems concepts on top of the previous one.

```mermaid
flowchart LR
    F1["Phase 1<br/>Basic sockets"] --> F2["Phase 2<br/>Advanced concurrency"]
    F2 --> F3["Phase 3<br/>Persistence & XML"]
    F3 --> F4["Phase 4<br/>Security & UDP discovery"]
    F4 --> F5["Phase 5<br/>REST admin API"]
```

### Phase 1 — Basic sockets

Multithreaded TCP server, plus a producer client and a consumer client exchanging plain-text messages.

- `ServerSocket` accept loop delegating each connection to an `ExecutorService`.
- First version of the text protocol (`SUBSCRIBE`, `PUBLISH`, `DELIVER`).
- Disconnection detection with `setSoTimeout()` and graceful close with `shutdownOutput()`.

**Done when:** with the broker running, a consumer subscribed to a topic receives every message a producer publishes to it, in order, and closing either client does not affect the broker or other clients.

### Phase 2 — Advanced concurrency

Organize topics in a `ConcurrentHashMap` and decouple reception from delivery with per-topic `BlockingQueue`s.

- Topic registry with thread-safe subscriber sets.
- One dispatcher thread per topic (producer–consumer pattern).
- `Semaphore`-based flow control and `CountDownLatch`-based delivery acknowledgements.

**Done when:** several producers and consumers on different topics run concurrently without message loss or cross-topic leakage, and a slow consumer does not stall delivery to the others.

### Phase 3 — Persistence and XML

Load the broker configuration from `config.xml` with JAXB and persist messages to disk with Java I/O.

- JAXB-mapped configuration model with validation of required fields.
- Append-only binary log per topic (`DataOutputStream` + `BufferedOutputStream`).
- Log replay on startup.

**Done when:** the broker starts using only the values in `config.xml`, and messages published before a broker restart are still available after it.

### Phase 4 — Security and UDP discovery

Add automatic discovery over UDP and replace plain sockets with TLS.

- UDP discovery responder in the broker and discovery logic in the clients.
- `SSLServerSocket` / `SSLSocket` with keystore and truststore generated by `keytool`.
- Mandatory client certificates for producers.
- SHA-256 hash verification on every published message.

**Done when:** clients connect without a hard-coded broker address, all traffic is encrypted, producers without a valid certificate are rejected, and a message with a tampered hash is refused with an `ERROR` frame.

### Phase 5 — REST admin API

Expose an HTTP API for management and monitoring, plus a CLI client to use it.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/topics` | List active topics and the number of retained messages per topic. |
| `POST` | `/topics` | Create a topic. Body: `{"name": "sensor-temperature"}`. |
| `DELETE` | `/topics/{name}` | Delete a topic and disconnect its subscribers. |
| `GET` | `/metrics` | Active threads, memory usage and messages per second. |

**Done when:** every endpoint above works against a live broker, and the admin CLI can perform the same operations using `java.net.http.HttpClient`.

---

## Open design questions

| ID | Question | Status |
|----|----------|--------|
| Q-01 | How are payloads containing `:` or newlines encoded on the wire (escaping, Base64, length-prefixed frames)? | Open |
| Q-02 | How are message IDs generated and assigned (per topic offset, UUID, broker sequence)? | Open |
| Q-03 | What happens to messages published to a topic with no subscribers: retained, dropped or replayed on subscribe? | Open |
| Q-04 | How does a consumer resume after a reconnect: from the latest message or from its last acknowledged one? | Open |
| Q-05 | Do consumers also need certificates, or only producers? | Open |
| Q-06 | Should UDP also carry periodic presence heartbeats, in addition to discovery? | Open |
| Q-07 | Which HTTP stack serves the admin API: a JAX-RS implementation or the JDK built-in `HttpServer`? | Open |

These will be resolved as the corresponding phases are planned in detail.

---

## License

To be defined.

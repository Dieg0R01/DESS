package dev.dess.broker;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;
import java.util.List;

@XmlRootElement(name = "broker")
@XmlAccessorType(XmlAccessType.FIELD)
public final class ConfigDocument {

    @XmlElement
    Network network;

    @XmlElement
    Engine engine;

    @XmlElement
    Storage storage;

    @XmlElement
    Topics topics;

    @XmlElement
    Logging logging;

    @XmlElement
    Security security;

    @XmlElement
    Discovery discovery;

    public ConfigDocument() {
    }

    @XmlAccessorType(XmlAccessType.FIELD)
    public static final class Network {

        @XmlElement
        Integer port;

        @XmlElement
        Integer maxConnections;

        @XmlElement
        Integer maxPayloadBytes;

        @XmlElement
        Integer idleTimeoutMs;

        public Network() {
        }
    }

    @XmlAccessorType(XmlAccessType.FIELD)
    public static final class Engine {

        @XmlElement
        Integer maxInFlight;

        @XmlElement
        Integer topicQueueCapacity;

        @XmlElement
        Integer outboundCapacity;

        @XmlElement
        Integer slowConsumerGraceMs;

        @XmlElement
        Integer publishWaitMs;

        @XmlElement
        Integer ackTimeoutMs;

        @XmlElement
        Integer threadKeepAliveMs;

        @XmlElement
        Integer shutdownTimeoutMs;

        public Engine() {
        }
    }

    @XmlAccessorType(XmlAccessType.FIELD)
    public static final class Storage {

        @XmlElement
        String logDirectory;

        @XmlElement
        String durability;

        @XmlElement
        Integer syncIntervalMs;

        public Storage() {
        }
    }

    @XmlAccessorType(XmlAccessType.FIELD)
    public static final class Topics {

        @XmlAttribute
        Boolean autoCreate;

        @XmlElement(name = "topic")
        List<String> topic;

        public Topics() {
        }
    }

    @XmlAccessorType(XmlAccessType.FIELD)
    public static final class Logging {

        @XmlElement
        String level;

        public Logging() {
        }
    }

    @XmlAccessorType(XmlAccessType.FIELD)
    public static final class Security {

        @XmlElement
        String keyStore;

        @XmlElement
        String trustStore;

        @XmlElement
        String passwordFile;

        public Security() {
        }
    }

    @XmlAccessorType(XmlAccessType.FIELD)
    public static final class Discovery {

        @XmlAttribute
        Boolean enabled;

        @XmlAttribute
        Boolean presence;

        @XmlElement
        Integer port;

        public Discovery() {
        }
    }
}

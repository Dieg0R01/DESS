package dev.dess.broker;

import dev.dess.common.protocol.TopicName;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Unmarshaller;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

final class ConfigLoader {

    private static final Schema SCHEMA = schema();

    private static final JAXBContext CONTEXT = context();

    private static final ConfigDocument DEFAULTS = defaults();

    private ConfigLoader() {
    }

    static Result load(Path file) throws IOException {
        String name = file.toAbsolutePath().toString();
        List<String> problems = new ArrayList<>();
        problems.addAll(validate(new StreamSource(file.toFile()), name));
        ConfigDocument document;
        try {
            document = unmarshal(new StreamSource(file.toFile()));
        } catch (JAXBException ex) {
            problems.add(name + ": " + flatten(ex.getMessage()));
            return new Result(null, List.copyOf(problems));
        }
        Resolved resolved = resolve(document);
        check(problems, name, resolved);
        if (!problems.isEmpty()) {
            return new Result(null, List.copyOf(problems));
        }
        BrokerConfig config = new BrokerConfig(
                resolved.port,
                resolved.maxConnections,
                resolved.maxPayloadBytes,
                resolved.idleTimeoutMillis,
                resolved.maxInFlight,
                resolved.topicQueueCapacity,
                resolved.outboundCapacity,
                resolved.slowConsumerGraceMillis,
                resolved.publishWaitMillis,
                resolved.ackTimeoutMillis,
                resolved.autoCreateTopics,
                resolved.topics,
                resolved.logDirectory,
                resolved.durability,
                resolved.syncIntervalMillis,
                resolved.logLevel,
                resolved.threadKeepAliveMillis,
                resolved.shutdownTimeoutMillis,
                resolved.keyStore,
                resolved.trustStore,
                resolved.passwordFile,
                resolved.discoveryEnabled,
                resolved.discoveryPort,
                resolved.presenceEnabled,
                resolved.adminBindAddress,
                resolved.adminPort,
                file.toAbsolutePath());
        return new Result(config, List.of());
    }

    private static void check(List<String> problems, String file, Resolved resolved) {
        reject(problems, file, resolved.port < 0 || resolved.port > 65535, "port must be between 0 and 65535");
        reject(problems, file, resolved.maxConnections < 1, "maxConnections must be at least 1");
        reject(problems, file, resolved.maxPayloadBytes < 1, "maxPayloadBytes must be at least 1");
        reject(problems, file, resolved.idleTimeoutMillis < 1, "idleTimeoutMillis must be at least 1");
        reject(problems, file, resolved.maxInFlight < 1, "maxInFlight must be at least 1");
        reject(problems, file, resolved.topicQueueCapacity < 1, "topicQueueCapacity must be at least 1");
        reject(problems, file, resolved.outboundCapacity < 1, "outboundCapacity must be at least 1");
        reject(problems, file, resolved.outboundCapacity > resolved.topicQueueCapacity, "outboundCapacity must not exceed topicQueueCapacity");
        reject(problems, file, resolved.slowConsumerGraceMillis < 1, "slowConsumerGraceMillis must be at least 1");
        reject(problems, file, resolved.publishWaitMillis < 0, "publishWaitMillis must be at least 0");
        reject(problems, file, resolved.ackTimeoutMillis < 1, "ackTimeoutMillis must be at least 1");
        reject(problems, file, resolved.syncIntervalMillis < 1, "syncIntervalMillis must be at least 1");
        reject(problems, file, !knownLevel(resolved.logLevel), "logLevel must be SEVERE, WARNING, INFO or FINE");
        reject(problems, file, resolved.threadKeepAliveMillis < 1, "threadKeepAliveMillis must be at least 1");
        reject(problems, file, resolved.shutdownTimeoutMillis < 1, "shutdownTimeoutMillis must be at least 1");
        reject(problems, file, resolved.logDirectoryText == null || resolved.logDirectoryText.isBlank(), "logDirectory is required");
        reject(problems, file, resolved.durability == null, "durability must be ALWAYS or INTERVAL");
        reject(problems, file, resolved.keyStoreText == null || resolved.keyStoreText.isBlank(), "keyStore is required");
        reject(problems, file, resolved.trustStoreText == null || resolved.trustStoreText.isBlank(), "trustStore is required");
        reject(problems, file, resolved.passwordFileText == null || resolved.passwordFileText.isBlank(), "passwordFile is required");
        reject(problems, file, resolved.discoveryPort < 0 || resolved.discoveryPort > 65535, "discoveryPort must be between 0 and 65535");
        reject(problems, file, resolved.adminBindAddress == null || resolved.adminBindAddress.isBlank(), "adminBindAddress is required");
        reject(problems, file, !resolvable(resolved.adminBindAddress), "adminBindAddress is not resolvable");
        reject(problems, file, resolved.adminPort < 0 || resolved.adminPort > 65535, "adminPort must be between 0 and 65535");
        Set<String> seen = new HashSet<>();
        for (String topic : resolved.topics) {
            if (!TopicName.isValid(topic)) {
                problems.add(file + ": invalid topic name " + topic);
            }
            if (!seen.add(topic)) {
                problems.add(file + ": duplicate topic name " + topic);
            }
        }
    }

    private static boolean resolvable(String address) {
        if (address == null || address.isBlank()) {
            return true;
        }
        try {
            InetAddress.getByName(address);
            return true;
        } catch (UnknownHostException ex) {
            return false;
        }
    }

    private static boolean knownLevel(String level) {
        return "SEVERE".equals(level) || "WARNING".equals(level) || "INFO".equals(level) || "FINE".equals(level);
    }

    private static void reject(List<String> problems, String file, boolean failed, String message) {
        if (failed) {
            problems.add(file + ": " + message);
        }
    }

    private static Resolved resolve(ConfigDocument document) {
        ConfigDocument.Network network = document.network;
        ConfigDocument.Engine engine = document.engine;
        ConfigDocument.Storage storage = document.storage;
        ConfigDocument.Topics topics = document.topics;
        ConfigDocument.Logging logging = document.logging;
        ConfigDocument.Security security = document.security;
        ConfigDocument.Discovery discovery = document.discovery;
        ConfigDocument.Admin admin = document.admin;
        ConfigDocument.Network defaultNetwork = required(DEFAULTS.network, "network");
        ConfigDocument.Engine defaultEngine = required(DEFAULTS.engine, "engine");
        ConfigDocument.Storage defaultStorage = required(DEFAULTS.storage, "storage");
        ConfigDocument.Topics defaultTopics = required(DEFAULTS.topics, "topics");
        ConfigDocument.Logging defaultLogging = required(DEFAULTS.logging, "logging");
        ConfigDocument.Security defaultSecurity = required(DEFAULTS.security, "security");
        ConfigDocument.Discovery defaultDiscovery = required(DEFAULTS.discovery, "discovery");
        ConfigDocument.Admin defaultAdmin = required(DEFAULTS.admin, "admin");
        String logDirectoryText = storage == null ? null : storage.logDirectory;
        if (logDirectoryText == null) {
            logDirectoryText = requiredText(defaultStorage.logDirectory, "logDirectory");
        }
        String durabilityText = storage == null ? null : storage.durability;
        if (durabilityText == null) {
            durabilityText = requiredText(defaultStorage.durability, "durability");
        }
        String level = logging == null ? null : logging.level;
        if (level == null) {
            level = requiredText(defaultLogging.level, "level");
        }
        boolean autoCreate = topics != null && topics.autoCreate != null
                ? topics.autoCreate
                : required(defaultTopics.autoCreate, "autoCreate");
        List<String> names = topics != null
                ? topics.topic == null ? List.of() : List.copyOf(topics.topic)
                : defaultTopics.topic == null ? List.of() : List.copyOf(defaultTopics.topic);
        String keyStoreText = security == null || security.keyStore == null
                ? requiredText(defaultSecurity.keyStore, "keyStore")
                : security.keyStore;
        String trustStoreText = security == null || security.trustStore == null
                ? requiredText(defaultSecurity.trustStore, "trustStore")
                : security.trustStore;
        String passwordFileText = security == null || security.passwordFile == null
                ? requiredText(defaultSecurity.passwordFile, "passwordFile")
                : security.passwordFile;
        boolean discoveryEnabled = discovery != null && discovery.enabled != null
                ? discovery.enabled
                : required(defaultDiscovery.enabled, "enabled");
        boolean presenceEnabled = discovery != null && discovery.presence != null
                ? discovery.presence
                : required(defaultDiscovery.presence, "presence");
        return new Resolved(
                pick(network == null ? null : network.port, defaultNetwork.port, "port"),
                pick(network == null ? null : network.maxConnections, defaultNetwork.maxConnections, "maxConnections"),
                pick(network == null ? null : network.maxPayloadBytes, defaultNetwork.maxPayloadBytes, "maxPayloadBytes"),
                pick(network == null ? null : network.idleTimeoutMs, defaultNetwork.idleTimeoutMs, "idleTimeoutMs"),
                pick(engine == null ? null : engine.maxInFlight, defaultEngine.maxInFlight, "maxInFlight"),
                pick(engine == null ? null : engine.topicQueueCapacity, defaultEngine.topicQueueCapacity, "topicQueueCapacity"),
                pick(engine == null ? null : engine.outboundCapacity, defaultEngine.outboundCapacity, "outboundCapacity"),
                pick(engine == null ? null : engine.slowConsumerGraceMs, defaultEngine.slowConsumerGraceMs, "slowConsumerGraceMs"),
                pick(engine == null ? null : engine.publishWaitMs, defaultEngine.publishWaitMs, "publishWaitMs"),
                pick(engine == null ? null : engine.ackTimeoutMs, defaultEngine.ackTimeoutMs, "ackTimeoutMs"),
                pick(engine == null ? null : engine.threadKeepAliveMs, defaultEngine.threadKeepAliveMs, "threadKeepAliveMs"),
                pick(engine == null ? null : engine.shutdownTimeoutMs, defaultEngine.shutdownTimeoutMs, "shutdownTimeoutMs"),
                autoCreate,
                names,
                logDirectoryText,
                absolute(logDirectoryText),
                durability(durabilityText),
                pick(storage == null ? null : storage.syncIntervalMs, defaultStorage.syncIntervalMs, "syncIntervalMs"),
                level,
                keyStoreText,
                absolute(keyStoreText),
                trustStoreText,
                absolute(trustStoreText),
                passwordFileText,
                absolute(passwordFileText),
                discoveryEnabled,
                pick(discovery == null ? null : discovery.port, defaultDiscovery.port, "port"),
                presenceEnabled,
                admin == null || admin.bindAddress == null || admin.bindAddress.isBlank()
                        ? requiredText(defaultAdmin.bindAddress, "bindAddress")
                        : admin.bindAddress,
                pick(admin == null ? null : admin.port, defaultAdmin.port, "port"));
    }

    private static int pick(Integer value, Integer fallback, String setting) {
        Integer chosen = value != null ? value : required(fallback, setting);
        return chosen;
    }

    private static Durability durability(String raw) {
        if ("ALWAYS".equals(raw)) {
            return Durability.ALWAYS;
        }
        if ("INTERVAL".equals(raw)) {
            return Durability.INTERVAL;
        }
        return null;
    }

    private static Path absolute(String text) {
        if (text == null || text.isBlank()) {
            return Path.of(".");
        }
        Path path = Path.of(text);
        Path absolute = path.isAbsolute() ? path : Path.of(System.getProperty("user.dir")).resolve(path);
        return absolute.toAbsolutePath().normalize();
    }

    private static <T> T required(T value, String setting) {
        if (value == null) {
            throw new IllegalStateException("defaults.xml is missing " + setting);
        }
        return value;
    }

    private static String requiredText(String value, String setting) {
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("defaults.xml is missing " + setting);
        }
        return value;
    }

    private static List<String> validate(StreamSource source, String file) throws IOException {
        List<SAXParseException> errors = new ArrayList<>();
        Validator validator = SCHEMA.newValidator();
        validator.setErrorHandler(new CollectingHandler(errors));
        try {
            validator.validate(source);
        } catch (SAXException ex) {
            if (errors.isEmpty()) {
                return List.of(file + ": " + flatten(ex.getMessage()));
            }
        }
        List<String> problems = new ArrayList<>();
        for (SAXParseException error : errors) {
            problems.add(file + ": " + flatten(error.getMessage()));
        }
        return problems;
    }

    private static ConfigDocument unmarshal(StreamSource source) throws JAXBException {
        Unmarshaller unmarshaller = CONTEXT.createUnmarshaller();
        return (ConfigDocument) unmarshaller.unmarshal(source);
    }

    private static String flatten(String message) {
        if (message == null || message.isBlank()) {
            return "invalid configuration";
        }
        return message.replaceAll("\\s+", " ").trim();
    }

    private static Schema schema() {
        URL resource = ConfigLoader.class.getResource("/config.xsd");
        if (resource == null) {
            throw new IllegalStateException("config.xsd is missing");
        }
        try {
            SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            return factory.newSchema(resource);
        } catch (SAXException ex) {
            throw new IllegalStateException("config.xsd is invalid", ex);
        }
    }

    private static JAXBContext context() {
        try {
            return JAXBContext.newInstance(ConfigDocument.class);
        } catch (JAXBException ex) {
            throw new IllegalStateException("Could not map the configuration document", ex);
        }
    }

    private static ConfigDocument defaults() {
        try (InputStream input = ConfigLoader.class.getResourceAsStream("/defaults.xml")) {
            if (input == null) {
                throw new IllegalStateException("defaults.xml is missing");
            }
            StreamSource source = new StreamSource(input);
            source.setSystemId("defaults.xml");
            List<String> problems = validate(source, "defaults.xml");
            if (!problems.isEmpty()) {
                throw new IllegalStateException(String.join(" ", problems));
            }
        } catch (IOException ex) {
            throw new IllegalStateException("defaults.xml is unreadable", ex);
        }
        try (InputStream input = ConfigLoader.class.getResourceAsStream("/defaults.xml")) {
            if (input == null) {
                throw new IllegalStateException("defaults.xml is missing");
            }
            StreamSource source = new StreamSource(input);
            source.setSystemId("defaults.xml");
            return unmarshal(source);
        } catch (IOException | JAXBException ex) {
            throw new IllegalStateException("defaults.xml is unreadable", ex);
        }
    }

    record Result(BrokerConfig config, List<String> problems) {
    }

    private record Resolved(
            int port,
            int maxConnections,
            int maxPayloadBytes,
            int idleTimeoutMillis,
            int maxInFlight,
            int topicQueueCapacity,
            int outboundCapacity,
            int slowConsumerGraceMillis,
            int publishWaitMillis,
            int ackTimeoutMillis,
            int threadKeepAliveMillis,
            int shutdownTimeoutMillis,
            boolean autoCreateTopics,
            List<String> topics,
            String logDirectoryText,
            Path logDirectory,
            Durability durability,
            int syncIntervalMillis,
            String logLevel,
            String keyStoreText,
            Path keyStore,
            String trustStoreText,
            Path trustStore,
            String passwordFileText,
            Path passwordFile,
            boolean discoveryEnabled,
            int discoveryPort,
            boolean presenceEnabled,
            String adminBindAddress,
            int adminPort) {
    }

    private static final class CollectingHandler implements ErrorHandler {

        private final List<SAXParseException> errors;

        private CollectingHandler(List<SAXParseException> errors) {
            this.errors = errors;
        }

        @Override
        public void warning(SAXParseException exception) {
            errors.add(exception);
        }

        @Override
        public void error(SAXParseException exception) {
            errors.add(exception);
        }

        @Override
        public void fatalError(SAXParseException exception) {
            errors.add(exception);
        }
    }
}

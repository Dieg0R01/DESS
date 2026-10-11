package dev.dess.broker;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import javax.net.ssl.SSLSocket;

final class TlsHandshake {

    private TlsHandshake() {
    }

    static Outcome open(Socket raw, TlsMaterial material, int timeoutMillis) {
        try {
            raw.setSoTimeout(timeoutMillis);
            byte[] prefix = new byte[11];
            int read = readFully(raw.getInputStream(), prefix);
            if (read < 11) {
                return Outcome.fail(HandshakeReason.CLOSED, null, null);
            }
            if ((prefix[0] & 0xff) != 0x16) {
                return Outcome.fail(HandshakeReason.PLAINTEXT, null, null);
            }
            int legacy = ((prefix[9] & 0xff) << 8) | (prefix[10] & 0xff);
            boolean old = legacy < 0x0303;
            SSLSocket ssl = wrap(raw, material, prefix);
            ssl.setSoTimeout(old ? Math.min(timeoutMillis, 250) : timeoutMillis);
            try {
                ssl.startHandshake();
            } catch (Exception ex) {
                if (old) {
                    return Outcome.fail(HandshakeReason.PROTOCOL_VERSION, null, ssl);
                }
                return classify(ex, ssl);
            }
            if (old) {
                return Outcome.fail(HandshakeReason.PROTOCOL_VERSION, null, ssl);
            }
            return Outcome.ok(ssl);
        } catch (SocketTimeoutException ex) {
            return Outcome.fail(HandshakeReason.TIMEOUT, null, null);
        } catch (EOFException | SocketException ex) {
            return Outcome.fail(HandshakeReason.CLOSED, null, null);
        } catch (IOException ex) {
            return Outcome.fail(HandshakeReason.FAILED, ex.getMessage(), null);
        }
    }

    private static SSLSocket wrap(Socket raw, TlsMaterial material, byte[] prefix) throws IOException {
        SSLSocket ssl = (SSLSocket) material.factory().createSocket(raw, new ByteArrayInputStream(prefix), true);
        ssl.setUseClientMode(false);
        ssl.setEnabledProtocols(TlsMaterial.PROTOCOLS);
        ssl.setWantClientAuth(true);
        return ssl;
    }

    private static Outcome classify(Throwable failure, SSLSocket ssl) {
        RejectedCertificate rejected = find(failure);
        if (rejected != null) {
            return Outcome.fail(rejected.reason(), null, ssl);
        }
        if (contains(failure, SocketTimeoutException.class)) {
            return Outcome.fail(HandshakeReason.TIMEOUT, null, ssl);
        }
        if (contains(failure, EOFException.class) || contains(failure, SocketException.class)) {
            return Outcome.fail(HandshakeReason.CLOSED, null, ssl);
        }
        String detail = failure.getMessage() == null ? failure.toString() : failure.getMessage();
        return Outcome.fail(HandshakeReason.FAILED, detail, ssl);
    }

    private static RejectedCertificate find(Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof RejectedCertificate rejected) {
                return rejected;
            }
            current = current.getCause();
        }
        return null;
    }

    private static boolean contains(Throwable failure, Class<? extends Throwable> type) {
        Throwable current = failure;
        while (current != null) {
            if (type.isInstance(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static int readFully(InputStream input, byte[] buffer) throws IOException {
        int read = 0;
        while (read < buffer.length) {
            int n = input.read(buffer, read, buffer.length - read);
            if (n < 0) {
                return read;
            }
            read += n;
        }
        return read;
    }

    record Outcome(SSLSocket socket, HandshakeReason reason, String detail) {

        static Outcome ok(SSLSocket socket) {
            return new Outcome(socket, null, null);
        }

        static Outcome fail(HandshakeReason reason, String detail, SSLSocket socket) {
            return new Outcome(socket, reason, detail);
        }

        boolean failed() {
            return reason != null;
        }
    }
}

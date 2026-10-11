package dev.dess.broker;

import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.security.auth.x500.X500Principal;

final class ClientIdentity {

    enum Role {
        PRODUCER,
        CONSUMER,
        BROKER
    }

    private final String name;
    private final Set<Role> roles;

    private ClientIdentity(String name, Set<Role> roles) {
        this.name = name;
        this.roles = Set.copyOf(roles);
    }

    static ClientIdentity parse(X509Certificate leaf) {
        try {
            LdapName subject = new LdapName(leaf.getSubjectX500Principal().getName(X500Principal.RFC2253));
            String name = null;
            Set<Role> roles = EnumSet.noneOf(Role.class);
            for (Rdn rdn : subject.getRdns()) {
                String type = rdn.getType();
                String value = String.valueOf(rdn.getValue());
                if ("CN".equalsIgnoreCase(type)) {
                    name = value;
                } else if ("OU".equalsIgnoreCase(type)) {
                    addRole(roles, value);
                }
            }
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("certificate has no CN");
            }
            return new ClientIdentity(name, roles);
        } catch (InvalidNameException ex) {
            throw new IllegalArgumentException("certificate subject is not an LDAP name", ex);
        }
    }

    String name() {
        return name;
    }

    boolean mayPublish() {
        return roles.contains(Role.PRODUCER);
    }

    String rolesText() {
        List<String> names = new ArrayList<>();
        for (Role role : Role.values()) {
            if (!roles.contains(role)) {
                continue;
            }
            names.add(switch (role) {
                case PRODUCER -> "producer";
                case CONSUMER -> "consumer";
                case BROKER -> "broker";
                default -> throw new IllegalStateException("Unhandled role " + role);
            });
        }
        return String.join(",", names);
    }

    private static void addRole(Set<Role> roles, String value) {
        switch (value.toLowerCase(Locale.ROOT)) {
            case "producer" -> roles.add(Role.PRODUCER);
            case "consumer" -> roles.add(Role.CONSUMER);
            case "broker" -> roles.add(Role.BROKER);
            default -> {
            }
        }
    }
}

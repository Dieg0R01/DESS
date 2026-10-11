#!/usr/bin/env bash
# Development certificates for DESS. Existing files are kept unless --force.
set -euo pipefail

USAGE='Usage: generate-certs.sh [--out DIR] [--force] [--client NAME --role producer|consumer] [--validity-days N]'

script_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
out_dir="${script_dir}/pki"
force=0
client_name=""
client_role=""
validity_days=365

die() {
    printf '%s\n' "$1" >&2
    exit "${2:-1}"
}

usage_error() {
    printf '%s\n' "$USAGE" >&2
    exit 2
}

require_value() {
    if [ "$#" -lt 2 ] || [ -z "${2:-}" ] || [[ "${2}" == -* ]]; then
        usage_error
    fi
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --out)
            require_value "$@"
            out_dir="$2"
            shift 2
            ;;
        --force)
            force=1
            shift
            ;;
        --client)
            require_value "$@"
            client_name="$2"
            shift 2
            ;;
        --role)
            require_value "$@"
            client_role="$2"
            shift 2
            ;;
        --validity-days)
            require_value "$@"
            validity_days="$2"
            shift 2
            ;;
        -h|--help)
            printf '%s\n' "$USAGE"
            exit 0
            ;;
        *)
            usage_error
            ;;
    esac
done

if [ -n "$client_name" ] && [ -z "$client_role" ]; then
    usage_error
fi
if [ -z "$client_name" ] && [ -n "$client_role" ]; then
    usage_error
fi
if [ -n "$client_role" ] && [ "$client_role" != "producer" ] && [ "$client_role" != "consumer" ]; then
    usage_error
fi
if ! [[ "$validity_days" =~ ^[1-9][0-9]*$ ]]; then
    usage_error
fi
if [ -n "$client_name" ] && ! [[ "$client_name" =~ ^[A-Za-z0-9._-]+$ ]]; then
    usage_error
fi

if [ -n "${JAVA_HOME:-}" ] && [ -x "${JAVA_HOME}/bin/keytool" ]; then
    keytool="${JAVA_HOME}/bin/keytool"
elif command -v keytool >/dev/null 2>&1; then
    keytool="$(command -v keytool)"
else
    die "keytool not found"
fi

mkdir -p "$out_dir"
out_dir="$(CDPATH= cd -- "$out_dir" && pwd)"

password_file="${out_dir}/password"
ca_store="${out_dir}/ca.p12"
trust_store="${out_dir}/trust.p12"
broker_store="${out_dir}/broker.p12"
producer_store="${out_dir}/producer.p12"
consumer_store="${out_dir}/consumer.p12"

base_files=("$password_file" "$ca_store" "$trust_store" "$broker_store" "$producer_store" "$consumer_store")

if [ "$force" -eq 1 ]; then
    for file in "${base_files[@]}"; do
        rm -f "$file"
    done
fi

work="$(mktemp -d "${out_dir}/.work.XXXXXX")"
cleanup() {
    rm -rf "$work"
}
trap cleanup EXIT

report() {
    local verb="$1"
    local file="$2"
    printf '%s %s\n' "$verb" "$file"
}

ensure_password() {
    if [ -f "$password_file" ]; then
        report kept "$password_file"
        return
    fi
    set +o pipefail
    LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom | head -c 24 >"$password_file"
    set -o pipefail
    chmod 600 "$password_file"
    if [ "$(wc -c <"$password_file" | tr -d ' ')" != "24" ]; then
        die "could not generate a 24-character password"
    fi
    report created "$password_file"
}

password() {
    tr -d '\n' <"$password_file"
}

run_keytool() {
    local log="${work}/keytool.log"
    if ! "$keytool" "$@" >"$log" 2>&1; then
        cat "$log" >&2
        die "keytool failed"
    fi
}

ensure_ca() {
    if [ -f "$ca_store" ]; then
        report kept "$ca_store"
        return
    fi
    run_keytool -genkeypair \
        -alias ca \
        -keyalg EC \
        -groupname secp256r1 \
        -validity "$validity_days" \
        -dname "CN=DESS Development CA" \
        -ext "BC=ca:true,pathlen:1" \
        -keystore "$ca_store" \
        -storetype PKCS12 \
        -storepass "$(password)" \
        -keypass "$(password)"
    report created "$ca_store"
}

export_ca_cert() {
    run_keytool -exportcert \
        -alias ca \
        -keystore "$ca_store" \
        -storetype PKCS12 \
        -storepass "$(password)" \
        -rfc \
        -file "${work}/ca.crt"
}

ensure_trust() {
    if [ -f "$trust_store" ]; then
        report kept "$trust_store"
        return
    fi
    export_ca_cert
    run_keytool -importcert \
        -alias ca \
        -file "${work}/ca.crt" \
        -keystore "$trust_store" \
        -storetype PKCS12 \
        -storepass "$(password)" \
        -noprompt
    report created "$trust_store"
}

sign_and_import() {
    local store="$1"
    local alias_name="$2"
    local san="${3:-}"
    run_keytool -certreq \
        -alias "$alias_name" \
        -keystore "$store" \
        -storetype PKCS12 \
        -storepass "$(password)" \
        -file "${work}/${alias_name}.csr"
    local ext_args=(-ext "KU=digitalSignature,keyEncipherment")
    if [ -n "$san" ]; then
        ext_args+=(-ext "$san")
    fi
    run_keytool -gencert \
        -alias ca \
        -keystore "$ca_store" \
        -storetype PKCS12 \
        -storepass "$(password)" \
        -infile "${work}/${alias_name}.csr" \
        -outfile "${work}/${alias_name}.crt" \
        -validity "$validity_days" \
        -rfc \
        "${ext_args[@]}"
    export_ca_cert
    run_keytool -importcert \
        -alias ca \
        -file "${work}/ca.crt" \
        -keystore "$store" \
        -storetype PKCS12 \
        -storepass "$(password)" \
        -noprompt
    run_keytool -importcert \
        -alias "$alias_name" \
        -file "${work}/${alias_name}.crt" \
        -keystore "$store" \
        -storetype PKCS12 \
        -storepass "$(password)" \
        -noprompt
}

ensure_identity() {
    local store="$1"
    local alias_name="$2"
    local dname="$3"
    local san="${4:-}"
    if [ -f "$store" ]; then
        report kept "$store"
        return
    fi
    local gen_args=(
        -genkeypair
        -alias "$alias_name"
        -keyalg EC
        -groupname secp256r1
        -validity "$validity_days"
        -dname "$dname"
        -keystore "$store"
        -storetype PKCS12
        -storepass "$(password)"
        -keypass "$(password)"
    )
    if [ -n "$san" ]; then
        gen_args+=(-ext "$san")
    fi
    run_keytool "${gen_args[@]}"
    sign_and_import "$store" "$alias_name" "$san"
    report created "$store"
}

ensure_password
ensure_ca
ensure_trust
ensure_identity "$broker_store" broker "CN=broker, OU=broker" "SAN=dns:localhost,ip:127.0.0.1"
ensure_identity "$producer_store" producer "CN=producer, OU=producer"
ensure_identity "$consumer_store" consumer "CN=consumer, OU=consumer"

if [ -n "$client_name" ]; then
    ensure_identity "${out_dir}/${client_name}.p12" "$client_name" "CN=${client_name}, OU=${client_role}"
fi

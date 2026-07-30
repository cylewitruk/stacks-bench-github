#!/usr/bin/env bash
# Minimal private-PKI helper. Keep the CA directory offline and backed up.
set -euo pipefail
umask 077

usage() {
    echo "usage:" >&2
    echo "  $0 init-ca CA_DIR" >&2
    echo "  $0 server CA_DIR OUT_DIR DNS_NAME" >&2
    echo "  $0 worker CA_DIR OUT_DIR WORKER_UUID" >&2
    exit 2
}

[[ $# -ge 2 ]] || usage
action=$1
ca_dir=$2

case "$action" in
    init-ca)
        [[ $# -eq 2 ]] || usage
        mkdir -p "$ca_dir"
        [[ ! -e "$ca_dir/ca.key" && ! -e "$ca_dir/ca.crt" ]] || {
            echo "refusing to overwrite an existing CA" >&2
            exit 1
        }
        openssl genpkey -algorithm EC \
            -pkeyopt ec_paramgen_curve:P-256 -aes-256-cbc -out "$ca_dir/ca.key"
        openssl req -x509 -new -sha256 -days 3650 \
            -key "$ca_dir/ca.key" -out "$ca_dir/ca.crt" \
            -subj "/CN=SBGH private worker CA" \
            -addext 'basicConstraints=critical,CA:TRUE' \
            -addext 'keyUsage=critical,keyCertSign,cRLSign'
        chmod 0600 "$ca_dir/ca.key"
        chmod 0644 "$ca_dir/ca.crt"
        ;;
    server)
        [[ $# -eq 4 ]] || usage
        out_dir=$3
        dns_name=$4
        [[ "$dns_name" =~ ^[A-Za-z0-9.-]+$ ]] || {
            echo "server DNS name contains unsafe characters" >&2
            exit 2
        }
        mkdir -p "$out_dir"
        [[ ! -e "$out_dir/server.key" ]] || {
            echo "refusing to overwrite server.key" >&2
            exit 1
        }
        openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 \
            -out "$out_dir/server.key"
        openssl req -new -key "$out_dir/server.key" -out "$out_dir/server.csr" \
            -subj "/CN=$dns_name"
        extension=$(mktemp)
        trap 'rm -f -- "$extension"' EXIT
        printf '%s\n' \
            'basicConstraints=critical,CA:FALSE' \
            'keyUsage=critical,digitalSignature,keyAgreement' \
            'extendedKeyUsage=serverAuth' \
            "subjectAltName=DNS:$dns_name" > "$extension"
        openssl x509 -req -sha256 -days 90 -in "$out_dir/server.csr" \
            -CA "$ca_dir/ca.crt" -CAkey "$ca_dir/ca.key" -CAcreateserial \
            -extfile "$extension" -out "$out_dir/server.crt"
        chmod 0600 "$out_dir/server.key"
        chmod 0644 "$out_dir/server.crt"
        ;;
    worker)
        [[ $# -eq 4 ]] || usage
        out_dir=$3
        worker_id=$4
        [[ "$worker_id" =~ ^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89aAbB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$ ]] || {
            echo "worker UUID has an invalid shape" >&2
            exit 2
        }
        mkdir -p "$out_dir"
        [[ ! -e "$out_dir/client.key" ]] || {
            echo "refusing to overwrite client.key" >&2
            exit 1
        }
        openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 \
            -out "$out_dir/client.key"
        openssl req -new -key "$out_dir/client.key" -out "$out_dir/client.csr" \
            -subj "/CN=sbgh-worker"
        extension=$(mktemp)
        trap 'rm -f -- "$extension"' EXIT
        printf '%s\n' \
            'basicConstraints=critical,CA:FALSE' \
            'keyUsage=critical,digitalSignature,keyAgreement' \
            'extendedKeyUsage=clientAuth' \
            "subjectAltName=URI:urn:sbgh:worker:$worker_id" > "$extension"
        openssl x509 -req -sha256 -days 90 -in "$out_dir/client.csr" \
            -CA "$ca_dir/ca.crt" -CAkey "$ca_dir/ca.key" -CAcreateserial \
            -extfile "$extension" -out "$out_dir/client.crt"
        chmod 0600 "$out_dir/client.key"
        chmod 0644 "$out_dir/client.crt"
        fingerprint=$(openssl x509 -in "$out_dir/client.crt" \
            -noout -fingerprint -sha256)
        fingerprint=${fingerprint#*=}
        fingerprint=${fingerprint//:/}
        printf 'certificate_sha256=%s\n' "${fingerprint,,}"
        ;;
    *)
        usage
        ;;
esac

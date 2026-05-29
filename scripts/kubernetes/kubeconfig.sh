#!/usr/bin/env bash
# Netsy <https://netsy.dev>
# Copyright The Netsy Authors
# SPDX-License-Identifier: Apache-2.0
set -eo pipefail

CURRENT=$(dirname "$(readlink -f "$0")")
CERTS_DIR="${CURRENT}/../../temp/certs"

# check if kubectl command exists
command -v kubectl >/dev/null 2>&1 || { echo >&2 "kubectl is required but it's not installed.  Aborting."; exit 1; }

# check that certs exist
if [ ! -f "${CERTS_DIR}/ca.crt" ] || [ ! -f "${CERTS_DIR}/client.crt" ] || [ ! -f "${CERTS_DIR}/client.key" ]; then
    echo >&2 "Required certificates not found. Run './scripts/certs.sh' first. Aborting."
    exit 1
fi

# Configure cluster
echo "Configuring cluster 'netsy'..."
kubectl config set-cluster netsy \
    --server=https://localhost:6443 \
    --insecure-skip-tls-verify=true

# Configure user with client certificate
echo "Configuring user 'netsy'..."
kubectl config set-credentials netsy \
    --client-certificate="${CERTS_DIR}/client.crt" \
    --client-key="${CERTS_DIR}/client.key"

# Configure context
echo "Configuring context 'netsy'..."
kubectl config set-context netsy \
    --cluster=netsy \
    --user=netsy

# Switch to the context
echo "Switching to context 'netsy'..."
kubectl config use-context netsy

echo "Kubeconfig setup complete. You can now use kubectl and k9s with the 'netsy' context."

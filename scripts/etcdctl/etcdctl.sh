#!/usr/bin/env bash
# Netsy <https://netsy.dev>
# Copyright The Netsy Authors
# SPDX-License-Identifier: Apache-2.0
set -eo pipefail

CURRENT=$(dirname "$(readlink -f "$0")")
CERTS_DIR="${CURRENT}/../../temp/certs"

if [ "$#" -eq 0 ]; then
    echo "usage: $0 <etcdctl command> [args...]" >&2
    echo "examples:" >&2
    echo "  $0 endpoint status" >&2
    echo "  $0 get \"\" --prefix" >&2
    exit 2
fi

USE_NETSY=1
ENDPOINT=127.0.0.1:2378
if [[ "$NETSY" = 0 ]] || [[ "$NETSY" = "false" ]]; then
    USE_NETSY=0
    ENDPOINT=127.0.0.1:2379
fi

CMD=(
    etcdctl "--cert=${CERTS_DIR}/client.crt"
            "--key=${CERTS_DIR}/client.key"
            "--cacert=${CERTS_DIR}/ca.crt"
            "--endpoints=${ENDPOINT}"
            -w json
    "$@"
)
if [ "$USE_NETSY" -eq 0 ]; then
    CMD=(
        etcdctl -w json "$@"
    )
fi

echo "${CMD[@]//$CURRENT\/..\/..\/}"
"${CMD[@]}" | jq .

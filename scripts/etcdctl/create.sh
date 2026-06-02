#!/usr/bin/env bash
# Netsy <https://netsy.dev>
# Copyright The Netsy Authors
# SPDX-License-Identifier: Apache-2.0
set -eo pipefail

CURRENT=$(dirname "$(readlink -f "$0")")

KEY="${1:-examplekey}"
VALUE="${2:-examplevalue}"

# note: with etcdctl, we must have two newlines between each of:
# - compare
# - success requests (get, put, delete)
# - failure requests (range, only on update/delete)
TXN="mod(\"${KEY}\") = \"0\"

put \"${KEY}\" \"${VALUE}\"

"

echo -n "echo '${TXN}' | "
echo "${TXN}" | "${CURRENT}/etcdctl.sh" txn

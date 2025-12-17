#!/bin/bash

# Simple mock server that responds with success to all requests
# Listens on localhost:8080

PORT=8080

handler() {
    echo "------- Request received -------" >&2
    while read -r line; do
        echo "$line" >&2
        [[ -z "${line%$'\r'}" ]] && break
    done
    echo "--------------------------------" >&2
    echo >&2

    printf 'HTTP/1.1 200 OK\r\n'
    printf 'Content-Type: application/json\r\n'
    printf 'Connection: close\r\n'
    printf '\r\n'
    printf '{"success": true}'
}

export -f handler

echo "Starting mock server on localhost:$PORT..."
echo "Press Ctrl+C to stop"

while true; do
    if ! ncat -l -p $PORT -c handler; then
        echo "ncat command failed" >&2
        exit 1
    fi
done

#!/bin/bash
set -e

CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o bin/siprec-ws-bridge .

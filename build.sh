#!/bin/bash
set -ex

CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -ldflags "-X main.Version=$(date +%s)" -o bin/siprec-ws-bridge .

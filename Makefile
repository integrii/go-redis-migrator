.Phony: all

SHELL := /bin/bash # Use bash syntax

build-mac:
	GOOS=darwin GOARCH=amd64 go build -v -o out/

build-linux:
	GOOS=linux GOARCH=amd64 go build -v -o out/
BINARY_NAME = goc

VARS_PKG = go-oak-chunk/v2/vars

BUILD_FLAGS  = -X '${VARS_PKG}.AppName=${BINARY_NAME}'
#BUILD_FLAGS += -X '${VARS_PKG}.AppVersion=$(shell git describe)'
BUILD_FLAGS += -X '${VARS_PKG}.GoVersion=$(shell go version)'
BUILD_FLAGS += -X '${VARS_PKG}.BuildTime=$(shell date +"%Y-%m-%d %H:%M:%S")'
BUILD_FLAGS += -X '${VARS_PKG}.GitCommit=$(shell git rev-parse HEAD)'
BUILD_FLAGS += -X '${VARS_PKG}.GitRemote=$(shell git config --get remote.origin.url)'

all: clean build deploy run

release: build linux test darwin

build:
	GOARCH=amd64 GOOS=linux go build -ldflags="${BUILD_FLAGS}" -o ${BINARY_NAME} main.go

linux:
	@mv ${BINARY_NAME} ${BINARY_NAME}.linux.amd64

test:
	go build -ldflags="${BUILD_FLAGS}" -o ${BINARY_NAME} main.go

darwin:
	@mv ${BINARY_NAME} ${BINARY_NAME}.darwin.arm64

deploy:
	@mv -f ${BINARY_NAME} /usr/local/bin/

run:
	@./${BINARY_NAME} version

clean:
	@go clean
	@rm -f ${BINARY_NAME}
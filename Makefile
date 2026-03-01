BINARY := bin/wartt
GOFLAGS := -trimpath

.PHONY: build install clean

build:
	go build $(GOFLAGS) -o $(BINARY) ./cmd/wartt/

install: build
	sudo cp $(BINARY) /usr/local/bin/wartt

clean:
	rm -f $(BINARY)

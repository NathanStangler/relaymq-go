# relaymq-go

`relaymq-go` is the Golang client SDK for RelayMQ.

## Run Tests

```bash
go test -v ./...
```

## Usage

```go
package main

import (
	"fmt"
	"log"

	"github.com/NathanStangler/relaymq-go"
)

func main() {
	client, err := relaymq.Connect("127.0.0.1:9000")
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Disconnect()

	_, err = client.Subscribe("service.events.>", func(msg relaymq.Message) {
		fmt.Printf("[%s] %s\n", msg.Subject, msg.Payload)
	})
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	err = client.Publish("service.events.created", "status=active")
	if err != nil {
		log.Fatalf("Failed to publish: %v", err)
	}
}
```

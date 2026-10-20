package relaymq_test

import (
	"testing"

	"github.com/NathanStangler/relaymq-go"
)

func TestEndpointParseHostPort(t *testing.T) {
	endpoint, err := relaymq.ParseEndpoint("127.0.0.1:9000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if endpoint.Host != "127.0.0.1" || endpoint.Port != 9000 {
		t.Fatalf("expected 127.0.0.1:9000, got %s:%d", endpoint.Host, endpoint.Port)
	}
	if endpoint.String() != "127.0.0.1:9000" {
		t.Fatalf("expected string 127.0.0.1:9000, got %s", endpoint.String())
	}

	endpoint2, err := relaymq.ParseEndpoint("service.internal:9001")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if endpoint2.Host != "service.internal" || endpoint2.Port != 9001 {
		t.Fatalf("expected service.internal:9001, got %s:%d", endpoint2.Host, endpoint2.Port)
	}
}

func TestEndpointParsePort(t *testing.T) {
	endpoint, err := relaymq.ParseEndpoint("9000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if endpoint.Host != "127.0.0.1" || endpoint.Port != 9000 {
		t.Fatalf("expected 127.0.0.1:9000, got %s:%d", endpoint.Host, endpoint.Port)
	}

	endpoint2, err := relaymq.ParseEndpoint(":9001")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if endpoint2.Host != "127.0.0.1" || endpoint2.Port != 9001 {
		t.Fatalf("expected 127.0.0.1:9001, got %s:%d", endpoint2.Host, endpoint2.Port)
	}
}

func TestEndpointParseHost(t *testing.T) {
	endpoint, err := relaymq.ParseEndpoint("127.0.0.1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if endpoint.Host != "127.0.0.1" || endpoint.Port != 9000 {
		t.Fatalf("expected 127.0.0.1:9000, got %s:%d", endpoint.Host, endpoint.Port)
	}
}

func TestEndpointParseInvalid(t *testing.T) {
	invalidCases := []string{
		"",
		"127.0.0.1:0",
		"127.0.0.1:70000",
		"127.0.0.1:abc",
		"127.0.0.1:",
	}

	for _, tc := range invalidCases {
		_, err := relaymq.ParseEndpoint(tc)
		if err == nil {
			t.Fatalf("expected error for %q, got nil", tc)
		}
	}
}

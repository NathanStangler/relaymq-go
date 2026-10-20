package relaymq_test

import (
	"bufio"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/NathanStangler/relaymq-go"
)

type mockServer struct {
	listener net.Listener
	mu       sync.Mutex
	conns    []net.Conn
	done     chan struct{}
}

func startMockServer(t *testing.T) *mockServer {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start mock server: %v", err)
	}

	ms := &mockServer{
		listener: ln,
		done:     make(chan struct{}),
	}

	go ms.acceptLoop()
	return ms
}

func (ms *mockServer) Addr() string {
	return ms.listener.Addr().String()
}

func (ms *mockServer) Close() {
	close(ms.done)
	_ = ms.listener.Close()
	ms.mu.Lock()
	for _, c := range ms.conns {
		_ = c.Close()
	}
	ms.mu.Unlock()
}

func (ms *mockServer) acceptLoop() {
	for {
		conn, err := ms.listener.Accept()
		if err != nil {
			select {
			case <-ms.done:
				return
			default:
				return
			}
		}

		ms.mu.Lock()
		ms.conns = append(ms.conns, conn)
		ms.mu.Unlock()

		go ms.handleConn(conn)
	}
}

func (ms *mockServer) handleConn(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	subs := make(map[string]string)

	for {
		cmd, err := relaymq.ReadCommand(r)
		if err != nil {
			return
		}

		switch cmd.Type {
		case relaymq.CmdPing:
			_, _ = io.WriteString(conn, relaymq.FormatPong())

		case relaymq.CmdAuth:
			if cmd.AuthToken == "valid_token" {
				_, _ = io.WriteString(conn, relaymq.FormatOk())
			} else {
				_, _ = io.WriteString(conn, relaymq.FormatErr("Invalid Credentials"))
			}

		case relaymq.CmdSub:
			subs[cmd.Subject] = cmd.Sid
			_, _ = io.WriteString(conn, relaymq.FormatOk())

		case relaymq.CmdUnsub:
			for subj, sid := range subs {
				if sid == cmd.Sid {
					delete(subs, subj)
					break
				}
			}
			_, _ = io.WriteString(conn, relaymq.FormatOk())

		case relaymq.CmdPub:
			if cmd.ReplyTo != "" {
				sid, ok := subs[cmd.ReplyTo]
				if !ok {
					sid = "1"
				}
				reply := "reply:" + cmd.Payload
				msg := relaymq.FormatMsg(cmd.ReplyTo, sid, reply)
				_, _ = io.WriteString(conn, msg)
			} else {
				sid, ok := subs[cmd.Subject]
				if !ok {
					sid = "1"
				}
				msg := relaymq.FormatMsg(cmd.Subject, sid, cmd.Payload)
				_, _ = io.WriteString(conn, msg)
			}
		}
	}
}

func TestConnectAndDisconnect(t *testing.T) {
	server := startMockServer(t)
	defer server.Close()

	client := relaymq.NewClient()
	if client.IsConnected() {
		t.Fatalf("expected client to not be connected")
	}

	if err := client.ConnectAddr(server.Addr()); err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	if !client.IsConnected() {
		t.Fatalf("expected client to be connected")
	}

	client.Disconnect()

	if client.IsConnected() {
		t.Fatalf("expected client to be disconnected")
	}
}

func TestPingLatency(t *testing.T) {
	server := startMockServer(t)
	defer server.Close()

	client, err := relaymq.Connect(server.Addr())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer client.Disconnect()

	latency, err := client.Ping(1 * time.Second)
	if err != nil {
		t.Fatalf("ping failed: %v", err)
	}

	if latency < 0 {
		t.Fatalf("expected non-negative latency, got %v", latency)
	}
}

func TestAuthSuccessAndFailure(t *testing.T) {
	server := startMockServer(t)
	defer server.Close()

	badClient, err := relaymq.Connect(server.Addr(), "invalid_token", 500*time.Millisecond)
	if err == nil {
		badClient.Disconnect()
		t.Fatalf("expected auth error with invalid token, got success")
	}

	goodClient, err := relaymq.Connect(server.Addr(), "valid_token", 500*time.Millisecond)
	if err != nil {
		t.Fatalf("expected auth success, got %v", err)
	}
	goodClient.Disconnect()
}

func TestSubscribeAndReceive(t *testing.T) {
	server := startMockServer(t)
	defer server.Close()

	client, err := relaymq.Connect(server.Addr())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer client.Disconnect()

	recvCh := make(chan relaymq.Message, 1)

	sub, err := client.Subscribe("service.events.data", func(msg relaymq.Message) {
		recvCh <- msg
	})
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	if sub.Sid() == "" {
		t.Fatalf("expected non-empty sid")
	}

	if err := client.Publish("service.events.data", "payload"); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	select {
	case msg := <-recvCh:
		if msg.Subject != "service.events.data" {
			t.Fatalf("expected subject 'service.events.data', got %s", msg.Subject)
		}
		if msg.Payload != "payload" {
			t.Fatalf("expected payload 'payload', got %s", msg.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for message")
	}
}

func TestRequestReply(t *testing.T) {
	server := startMockServer(t)
	defer server.Close()

	client, err := relaymq.Connect(server.Addr())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer client.Disconnect()

	reply, err := client.Request("service.echo", "hello", 2*time.Second)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}

	if reply.Payload != "reply:hello" {
		t.Fatalf("expected 'reply:hello', got %s", reply.Payload)
	}
}

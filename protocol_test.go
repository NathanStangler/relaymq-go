package relaymq_test

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/NathanStangler/relaymq-go"
)

func TestProtocolControlCommands(t *testing.T) {
	input := "PING\r\nPONG\r\nOK\r\nERR invalid subject\r\n"
	r := bufio.NewReader(bytes.NewBufferString(input))

	cmd1, err := relaymq.ReadCommand(r)
	if err != nil || cmd1.Type != relaymq.CmdPing {
		t.Fatalf("expected PING, got %v (%v)", cmd1, err)
	}

	cmd2, err := relaymq.ReadCommand(r)
	if err != nil || cmd2.Type != relaymq.CmdPong {
		t.Fatalf("expected PONG, got %v (%v)", cmd2, err)
	}

	cmd3, err := relaymq.ReadCommand(r)
	if err != nil || cmd3.Type != relaymq.CmdOk {
		t.Fatalf("expected OK, got %v (%v)", cmd3, err)
	}

	cmd4, err := relaymq.ReadCommand(r)
	if err != nil || cmd4.Type != relaymq.CmdErr || cmd4.ErrorMessage != "invalid subject" {
		t.Fatalf("expected ERR with 'invalid subject', got %v (%v)", cmd4, err)
	}
}

func TestProtocolSubscribeAndUnsubscribe(t *testing.T) {
	input := "SUB service.message.value 100\r\nUNSUB 100\r\n"
	r := bufio.NewReader(bytes.NewBufferString(input))

	cmd1, err := relaymq.ReadCommand(r)
	if err != nil || cmd1.Type != relaymq.CmdSub || cmd1.Subject != "service.message.value" || cmd1.Sid != "100" {
		t.Fatalf("expected SUB, got %v (%v)", cmd1, err)
	}

	cmd2, err := relaymq.ReadCommand(r)
	if err != nil || cmd2.Type != relaymq.CmdUnsub || cmd2.Sid != "100" {
		t.Fatalf("expected UNSUB, got %v (%v)", cmd2, err)
	}
}

func TestProtocolPublishPayload(t *testing.T) {
	msg := relaymq.FormatPub("service.message.value", "amount=50")
	r := bufio.NewReader(bytes.NewBufferString(msg))

	cmd, err := relaymq.ReadCommand(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cmd.Type != relaymq.CmdPub {
		t.Fatalf("expected PUB, got %v", cmd.Type)
	}
	if cmd.Subject != "service.message.value" {
		t.Fatalf("expected subject 'service.message.value', got %s", cmd.Subject)
	}
	if cmd.Payload != "amount=50" {
		t.Fatalf("expected payload 'amount=50', got %s", cmd.Payload)
	}
	if cmd.ReplyTo != "" {
		t.Fatalf("expected empty replyTo, got %s", cmd.ReplyTo)
	}
}

func TestProtocolPublishWithReplyTo(t *testing.T) {
	wire := relaymq.FormatPub("service.message.value", "amount=50", "box.user_1")
	r := bufio.NewReader(bytes.NewBufferString(wire))

	cmd, err := relaymq.ReadCommand(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cmd.Type != relaymq.CmdPub {
		t.Fatalf("expected PUB, got %v", cmd.Type)
	}
	if cmd.Subject != "service.message.value" {
		t.Fatalf("expected subject 'service.message.value', got %s", cmd.Subject)
	}
	if cmd.ReplyTo != "box.user_1" {
		t.Fatalf("expected replyTo 'box.user_1', got %s", cmd.ReplyTo)
	}
	if cmd.Payload != "amount=50" {
		t.Fatalf("expected payload 'amount=50', got %s", cmd.Payload)
	}
}

func TestProtocolSerializers(t *testing.T) {
	if relaymq.FormatPing() != "PING\r\n" {
		t.Fatalf("unexpected format")
	}
	if relaymq.FormatPong() != "PONG\r\n" {
		t.Fatalf("unexpected format")
	}
	if relaymq.FormatOk() != "OK\r\n" {
		t.Fatalf("unexpected format")
	}
	if relaymq.FormatErr("unknown topic") != "ERR unknown topic\r\n" {
		t.Fatalf("unexpected format")
	}
	if relaymq.FormatSub("service.counts.*", "1") != "SUB service.counts.* 1\r\n" {
		t.Fatalf("unexpected format")
	}
	if relaymq.FormatUnsub("1") != "UNSUB 1\r\n" {
		t.Fatalf("unexpected format")
	}
	if relaymq.FormatPub("service.message", "abc") != "PUB service.message 3\r\nabc\r\n" {
		t.Fatalf("unexpected format")
	}
	if relaymq.FormatMsg("service.message", "1", "abc") != "MSG service.message 1 3\r\nabc\r\n" {
		t.Fatalf("unexpected format")
	}
	if relaymq.FormatAuth("token_123") != "AUTH token_123\r\n" {
		t.Fatalf("unexpected format")
	}
}

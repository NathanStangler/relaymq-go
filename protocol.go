package relaymq

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const MaxPayloadSize = 64 * 1024 * 1024

type CommandType int

const (
	CmdUnknown CommandType = iota
	CmdPub
	CmdSub
	CmdUnsub
	CmdMsg
	CmdPing
	CmdPong
	CmdOk
	CmdErr
	CmdAuth
)

func (c CommandType) String() string {
	switch c {
	case CmdPub:
		return "PUB"
	case CmdSub:
		return "SUB"
	case CmdUnsub:
		return "UNSUB"
	case CmdMsg:
		return "MSG"
	case CmdPing:
		return "PING"
	case CmdPong:
		return "PONG"
	case CmdOk:
		return "OK"
	case CmdErr:
		return "ERR"
	case CmdAuth:
		return "AUTH"
	default:
		return "UNKNOWN"
	}
}

type Command struct {
	Type         CommandType
	Subject      string
	Sid          string
	ReplyTo      string
	Payload      string
	ErrorMessage string
	AuthToken    string
}

func FormatPub(subject, payload string, replyTo ...string) string {
	reply := ""
	if len(replyTo) > 0 {
		reply = replyTo[0]
	}
	lenStr := strconv.Itoa(len(payload))
	if reply == "" {
		return fmt.Sprintf("PUB %s %s\r\n%s\r\n", subject, lenStr, payload)
	}
	return fmt.Sprintf("PUB %s %s %s\r\n%s\r\n", subject, reply, lenStr, payload)
}

func FormatSub(subject, sid string) string {
	return fmt.Sprintf("SUB %s %s\r\n", subject, sid)
}

func FormatUnsub(sid string) string {
	return fmt.Sprintf("UNSUB %s\r\n", sid)
}

func FormatMsg(subject, sid, payload string, replyTo ...string) string {
	reply := ""
	if len(replyTo) > 0 {
		reply = replyTo[0]
	}
	lenStr := strconv.Itoa(len(payload))
	if reply == "" {
		return fmt.Sprintf("MSG %s %s %s\r\n%s\r\n", subject, sid, lenStr, payload)
	}
	return fmt.Sprintf("MSG %s %s %s %s\r\n%s\r\n", subject, sid, reply, lenStr, payload)
}

func FormatPing() string {
	return "PING\r\n"
}

func FormatPong() string {
	return "PONG\r\n"
}

func FormatOk() string {
	return "OK\r\n"
}

func FormatErr(reason string) string {
	return fmt.Sprintf("ERR %s\r\n", reason)
}

func FormatAuth(token string) string {
	return fmt.Sprintf("AUTH %s\r\n", token)
}

func ReadCommand(r *bufio.Reader) (*Command, error) {
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}

		line = strings.TrimRight(line, "\r\n")
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}

		action := strings.ToUpper(fields[0])

		switch action {
		case "PONG":
			return &Command{Type: CmdPong}, nil

		case "PING":
			return &Command{Type: CmdPing}, nil

		case "OK":
			return &Command{Type: CmdOk}, nil

		case "ERR":
			reason := ""
			if len(line) > 4 {
				reason = strings.TrimPrefix(line, "ERR ")
				if reason == line {
					reason = strings.TrimPrefix(line, "err ")
				}
			}
			return &Command{Type: CmdErr, ErrorMessage: reason}, nil

		case "AUTH":
			if len(fields) < 2 {
				return &Command{Type: CmdErr, ErrorMessage: "Invalid AUTH syntax: expected AUTH <token>"}, nil
			}
			return &Command{Type: CmdAuth, AuthToken: fields[1]}, nil

		case "SUB":
			if len(fields) < 3 {
				return &Command{Type: CmdErr, ErrorMessage: "Invalid SUB syntax: expected SUB <subject> <sid>"}, nil
			}
			return &Command{Type: CmdSub, Subject: fields[1], Sid: fields[2]}, nil

		case "UNSUB":
			if len(fields) < 2 {
				return &Command{Type: CmdErr, ErrorMessage: "Invalid UNSUB syntax: expected UNSUB <sid>"}, nil
			}
			return &Command{Type: CmdUnsub, Sid: fields[1]}, nil

		case "PUB":
			if len(fields) < 3 || len(fields) > 4 {
				return &Command{Type: CmdErr, ErrorMessage: "Invalid PUB syntax: expected PUB <subject> [reply-to] <bytes>"}, nil
			}
			subject := fields[1]
			var replyTo string
			var bytesStr string
			if len(fields) == 3 {
				bytesStr = fields[2]
			} else {
				replyTo = fields[2]
				bytesStr = fields[3]
			}
			payloadLen, err := strconv.Atoi(bytesStr)
			if err != nil || payloadLen < 0 {
				return &Command{Type: CmdErr, ErrorMessage: "Invalid byte length in PUB command"}, nil
			}
			if payloadLen > MaxPayloadSize {
				return &Command{Type: CmdErr, ErrorMessage: "Payload size exceeds maximum allowed size"}, nil
			}
			buf := make([]byte, payloadLen)
			if _, err := io.ReadFull(r, buf); err != nil {
				return nil, err
			}
			var crlf [2]byte
			if _, err := io.ReadFull(r, crlf[:]); err != nil {
				return nil, err
			}
			return &Command{
				Type:    CmdPub,
				Subject: subject,
				ReplyTo: replyTo,
				Payload: string(buf),
			}, nil

		case "MSG":
			if len(fields) < 4 || len(fields) > 5 {
				return &Command{Type: CmdErr, ErrorMessage: "Invalid MSG syntax: expected MSG <subject> <sid> [reply-to] <bytes>"}, nil
			}
			subject := fields[1]
			sid := fields[2]
			var replyTo string
			var bytesStr string
			if len(fields) == 4 {
				bytesStr = fields[3]
			} else {
				replyTo = fields[3]
				bytesStr = fields[4]
			}
			payloadLen, err := strconv.Atoi(bytesStr)
			if err != nil || payloadLen < 0 {
				return &Command{Type: CmdErr, ErrorMessage: "Invalid byte length in MSG command"}, nil
			}
			if payloadLen > MaxPayloadSize {
				return &Command{Type: CmdErr, ErrorMessage: "Payload size exceeds maximum allowed size"}, nil
			}
			buf := make([]byte, payloadLen)
			if _, err := io.ReadFull(r, buf); err != nil {
				return nil, err
			}
			var crlf [2]byte
			if _, err := io.ReadFull(r, crlf[:]); err != nil {
				return nil, err
			}
			return &Command{
				Type:    CmdMsg,
				Subject: subject,
				Sid:     sid,
				ReplyTo: replyTo,
				Payload: string(buf),
			}, nil

		default:
			return &Command{Type: CmdUnknown}, nil
		}
	}
}

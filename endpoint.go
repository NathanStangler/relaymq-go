package relaymq

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

var ErrInvalidEndpoint = errors.New("relaymq: invalid endpoint format")

type Endpoint struct {
	Host string
	Port uint16
}

func (e Endpoint) String() string {
	return fmt.Sprintf("%s:%d", e.Host, e.Port)
}

func (e Endpoint) ToString() string {
	return fmt.Sprintf("%s:%d", e.Host, e.Port)
}

func ParseEndpoint(addr string) (Endpoint, error) {
	if addr == "" {
		return Endpoint{}, ErrInvalidEndpoint
	}

	if before, after, ok := strings.Cut(addr, ":"); ok {
		hostPart := before
		portPart := after
		if portPart == "" {
			return Endpoint{}, ErrInvalidEndpoint
		}
		p, err := strconv.ParseUint(portPart, 10, 16)
		if err != nil || p == 0 {
			return Endpoint{}, ErrInvalidEndpoint
		}
		host := hostPart
		if host == "" {
			host = "127.0.0.1"
		}
		return Endpoint{Host: host, Port: uint16(p)}, nil
	}

	allDigits := true
	for _, r := range addr {
		if !unicode.IsDigit(r) {
			allDigits = false
			break
		}
	}

	if allDigits {
		p, err := strconv.ParseUint(addr, 10, 16)
		if err != nil || p == 0 {
			return Endpoint{}, ErrInvalidEndpoint
		}
		return Endpoint{Host: "127.0.0.1", Port: uint16(p)}, nil
	}

	return Endpoint{Host: addr, Port: 9000}, nil
}

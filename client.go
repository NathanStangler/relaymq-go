package relaymq

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrNotConnected = errors.New("relaymq: client not connected")
	ErrTimeout      = errors.New("relaymq: operation timed out")
	ErrAuthFailed   = errors.New("relaymq: authentication failed")
	ErrEmptySubject = errors.New("relaymq: subject cannot be empty")
)

type Message struct {
	Subject string
	Sid     string
	ReplyTo string
	Payload string
}

func (m Message) String() string {
	return m.Payload
}

type MessageCallback func(msg Message)
type DisconnectCallback func()
type ErrorCallback func(err string)

type Subscription struct {
	client  *Client
	subject string
	sid     string
}

func (s *Subscription) Subject() string {
	return s.subject
}

func (s *Subscription) Sid() string {
	return s.sid
}

func (s *Subscription) Unsubscribe() error {
	return s.client.Unsubscribe(s.sid)
}

type ClientOptions struct {
	Host               string
	Port               uint16
	AuthToken          string
	Timeout            time.Duration
	DisconnectCallback DisconnectCallback
	ErrorCallback      ErrorCallback
}

type Option func(*ClientOptions)

func WithAuthToken(token string) Option {
	return func(o *ClientOptions) {
		o.AuthToken = token
	}
}

func WithTimeout(d time.Duration) Option {
	return func(o *ClientOptions) {
		o.Timeout = d
	}
}

func WithDisconnectCallback(cb DisconnectCallback) Option {
	return func(o *ClientOptions) {
		o.DisconnectCallback = cb
	}
}

func WithErrorCallback(cb ErrorCallback) Option {
	return func(o *ClientOptions) {
		o.ErrorCallback = cb
	}
}

type Client struct {
	conn               net.Conn
	reader             *bufio.Reader
	writeMu            sync.Mutex
	subMu              sync.RWMutex
	subscriptions      map[string]MessageCallback
	nextSid            atomic.Uint64
	nextReqID          atomic.Uint64
	isConnected        atomic.Bool
	lastErrorMu        sync.RWMutex
	lastError          string
	options            ClientOptions
	disconnectCallback DisconnectCallback
	errorCallback      ErrorCallback
	closedOnce         sync.Once
	closeCh            chan struct{}
	pongMu             sync.Mutex
	pongCh             chan struct{}
	authMu             sync.Mutex
	authCh             chan error
}

func NewClient() *Client {
	return &Client{
		subscriptions: make(map[string]MessageCallback),
		options: ClientOptions{
			Host:    "127.0.0.1",
			Port:    9000,
			Timeout: 5 * time.Second,
		},
	}
}

func Connect(addr string, args ...any) (*Client, error) {
	endpoint, err := ParseEndpoint(addr)
	if err != nil {
		return nil, err
	}

	opts := ClientOptions{
		Host:    endpoint.Host,
		Port:    endpoint.Port,
		Timeout: 5 * time.Second,
	}

	for _, arg := range args {
		switch v := arg.(type) {
		case string:
			opts.AuthToken = v
		case Option:
			v(&opts)
		case time.Duration:
			opts.Timeout = v
		case ClientOptions:
			opts = v
		}
	}

	client := NewClient()
	if err := client.Connect(opts); err != nil {
		return nil, err
	}
	return client, nil
}

func (c *Client) Connect(options ClientOptions) error {
	if c.isConnected.Load() {
		return nil
	}

	c.options = options
	if c.options.Host == "" {
		c.options.Host = "127.0.0.1"
	}
	if c.options.Port == 0 {
		c.options.Port = 9000
	}
	if c.options.Timeout == 0 {
		c.options.Timeout = 5 * time.Second
	}
	if c.options.DisconnectCallback != nil {
		c.disconnectCallback = c.options.DisconnectCallback
	}
	if c.options.ErrorCallback != nil {
		c.errorCallback = c.options.ErrorCallback
	}

	c.closedOnce = sync.Once{}
	c.closeCh = make(chan struct{})

	dialer := net.Dialer{Timeout: c.options.Timeout}
	addr := net.JoinHostPort(c.options.Host, strconv.Itoa(int(c.options.Port)))
	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		c.setLastError("Failed to establish TCP connection")
		return fmt.Errorf("relaymq: failed to connect to %s: %w", addr, err)
	}

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetNoDelay(true)
	}

	c.conn = conn
	c.reader = bufio.NewReaderSize(conn, 32768)
	c.isConnected.Store(true)

	if c.options.AuthToken != "" {
		c.authCh = make(chan error, 1)
	}

	go c.receiveLoop()

	if c.options.AuthToken != "" {
		authMsg := FormatAuth(c.options.AuthToken)
		if err := c.sendRaw(authMsg); err != nil {
			_ = c.Close()
			c.setLastError("Failed to send AUTH frame")
			return err
		}

		select {
		case err := <-c.authCh:
			if err != nil {
				_ = c.Close()
				return err
			}
		case <-time.After(c.options.Timeout):
			_ = c.Close()
			c.setLastError("Authentication timed out or rejected")
			return ErrTimeout
		}
	}

	return nil
}

func (c *Client) ConnectHostPort(host string, port uint16, authToken ...string) error {
	token := ""
	if len(authToken) > 0 {
		token = authToken[0]
	}
	return c.Connect(ClientOptions{
		Host:      host,
		Port:      port,
		AuthToken: token,
		Timeout:   5 * time.Second,
	})
}

func (c *Client) ConnectAddr(addr string, authToken ...string) error {
	token := ""
	if len(authToken) > 0 {
		token = authToken[0]
	}
	endpoint, err := ParseEndpoint(addr)
	if err != nil {
		return err
	}
	return c.Connect(ClientOptions{
		Host:      endpoint.Host,
		Port:      endpoint.Port,
		AuthToken: token,
		Timeout:   5 * time.Second,
	})
}

func (c *Client) Disconnect() {
	_ = c.Close()
}

func (c *Client) Close() error {
	var closeErr error
	c.closedOnce.Do(func() {
		c.isConnected.Store(false)
		if c.closeCh != nil {
			select {
			case <-c.closeCh:
			default:
				close(c.closeCh)
			}
		}
		if c.conn != nil {
			closeErr = c.conn.Close()
		}

		c.pongMu.Lock()
		if c.pongCh != nil {
			close(c.pongCh)
			c.pongCh = nil
		}
		c.pongMu.Unlock()

		c.authMu.Lock()
		if c.authCh != nil {
			select {
			case c.authCh <- ErrNotConnected:
			default:
			}
		}
		c.authMu.Unlock()
	})
	return closeErr
}

func (c *Client) IsConnected() bool {
	return c.isConnected.Load()
}

func (c *Client) LastError() string {
	c.lastErrorMu.RLock()
	defer c.lastErrorMu.RUnlock()
	return c.lastError
}

func (c *Client) SetDisconnectCallback(callback DisconnectCallback) {
	c.subMu.Lock()
	c.disconnectCallback = callback
	c.subMu.Unlock()
}

func (c *Client) SetErrorCallback(callback ErrorCallback) {
	c.errorCallback = callback
}

func (c *Client) setLastError(err string) {
	c.lastErrorMu.Lock()
	c.lastError = err
	c.lastErrorMu.Unlock()
}

func (c *Client) sendRaw(data string) error {
	if !c.isConnected.Load() {
		return ErrNotConnected
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	_, err := io.WriteString(c.conn, data)
	if err != nil {
		c.isConnected.Store(false)
		return err
	}
	return nil
}

func (c *Client) Publish(subject, payload string, replyTo ...string) error {
	if subject == "" {
		return ErrEmptySubject
	}
	reply := ""
	if len(replyTo) > 0 {
		reply = replyTo[0]
	}
	msg := FormatPub(subject, payload, reply)
	return c.sendRaw(msg)
}

func (c *Client) PublishBytes(subject string, payload []byte, replyTo ...string) error {
	return c.Publish(subject, string(payload), replyTo...)
}

func (c *Client) PublishString(subject, payload string, replyTo ...string) error {
	return c.Publish(subject, payload, replyTo...)
}

func (c *Client) Subscribe(subject string, callback MessageCallback) (*Subscription, error) {
	if subject == "" {
		return nil, ErrEmptySubject
	}
	if callback == nil {
		return nil, errors.New("relaymq: callback cannot be nil")
	}

	sidNum := c.nextSid.Add(1)
	sid := strconv.FormatUint(sidNum, 10)

	c.subMu.Lock()
	c.subscriptions[sid] = callback
	c.subMu.Unlock()

	msg := FormatSub(subject, sid)
	if err := c.sendRaw(msg); err != nil {
		c.subMu.Lock()
		delete(c.subscriptions, sid)
		c.subMu.Unlock()
		return nil, err
	}

	return &Subscription{
		client:  c,
		subject: subject,
		sid:     sid,
	}, nil
}

func (c *Client) Unsubscribe(sid string) error {
	c.subMu.Lock()
	delete(c.subscriptions, sid)
	c.subMu.Unlock()

	msg := FormatUnsub(sid)
	return c.sendRaw(msg)
}

func (c *Client) Request(subject, payload string, timeout ...time.Duration) (*Message, error) {
	t := 2 * time.Second
	if len(timeout) > 0 {
		t = timeout[0]
	}
	ctx, cancel := context.WithTimeout(context.Background(), t)
	defer cancel()
	return c.RequestWithContext(ctx, subject, payload)
}

func (c *Client) RequestBytes(subject string, payload []byte, timeout ...time.Duration) (*Message, error) {
	return c.Request(subject, string(payload), timeout...)
}

func (c *Client) RequestString(subject, payload string, timeout ...time.Duration) (*Message, error) {
	return c.Request(subject, payload, timeout...)
}

func (c *Client) RequestWithContext(ctx context.Context, subject, payload string) (*Message, error) {
	if subject == "" {
		return nil, ErrEmptySubject
	}
	if !c.isConnected.Load() {
		return nil, ErrNotConnected
	}

	reqID := c.nextReqID.Add(1)
	replySubject := "reply." + strconv.FormatUint(reqID, 10)

	respCh := make(chan *Message, 1)
	var delivered atomic.Bool

	sub, err := c.Subscribe(replySubject, func(msg Message) {
		if delivered.CompareAndSwap(false, true) {
			respCh <- &msg
		}
	})
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = sub.Unsubscribe()
	}()

	if err := c.Publish(subject, payload, replySubject); err != nil {
		return nil, err
	}

	select {
	case msg := <-respCh:
		return msg, nil
	case <-c.closeCh:
		return nil, ErrNotConnected
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, ErrTimeout
		}
		return nil, ctx.Err()
	}
}

func (c *Client) Ping(timeout ...time.Duration) (int64, error) {
	t := 2 * time.Second
	if len(timeout) > 0 {
		t = timeout[0]
	}
	if !c.isConnected.Load() {
		return -1, ErrNotConnected
	}

	c.pongMu.Lock()
	c.pongCh = make(chan struct{}, 1)
	ch := c.pongCh
	c.pongMu.Unlock()

	startTime := time.Now()
	if err := c.sendRaw(FormatPing()); err != nil {
		c.pongMu.Lock()
		c.pongCh = nil
		c.pongMu.Unlock()
		return -1, err
	}

	select {
	case _, ok := <-ch:
		if !ok || !c.isConnected.Load() {
			return -1, ErrNotConnected
		}
		return time.Since(startTime).Microseconds(), nil
	case <-c.closeCh:
		return -1, ErrNotConnected
	case <-time.After(t):
		c.pongMu.Lock()
		c.pongCh = nil
		c.pongMu.Unlock()
		return -1, ErrTimeout
	}
}

func (c *Client) PingDuration(timeout ...time.Duration) (time.Duration, error) {
	us, err := c.Ping(timeout...)
	if err != nil {
		return 0, err
	}
	return time.Duration(us) * time.Microsecond, nil
}

func (c *Client) receiveLoop() {
	for c.isConnected.Load() {
		cmd, err := ReadCommand(c.reader)
		if err != nil {
			_ = c.Close()
			c.subMu.RLock()
			cb := c.disconnectCallback
			c.subMu.RUnlock()
			if cb != nil {
				cb()
			}
			break
		}

		switch cmd.Type {
		case CmdMsg:
			c.subMu.RLock()
			handler, exists := c.subscriptions[cmd.Sid]
			c.subMu.RUnlock()

			if exists && handler != nil {
				msg := Message{
					Subject: cmd.Subject,
					Sid:     cmd.Sid,
					ReplyTo: cmd.ReplyTo,
					Payload: cmd.Payload,
				}
				go handler(msg)
			}

		case CmdPong:
			c.pongMu.Lock()
			if c.pongCh != nil {
				select {
				case c.pongCh <- struct{}{}:
				default:
				}
			}
			c.pongMu.Unlock()

		case CmdOk:
			c.authMu.Lock()
			if c.authCh != nil {
				select {
				case c.authCh <- nil:
				default:
				}
			}
			c.authMu.Unlock()

		case CmdErr:
			c.setLastError(cmd.ErrorMessage)

			c.authMu.Lock()
			if c.authCh != nil {
				select {
				case c.authCh <- fmt.Errorf("%w: %s", ErrAuthFailed, cmd.ErrorMessage):
				default:
				}
			}
			c.authMu.Unlock()

			if c.errorCallback != nil {
				c.errorCallback(cmd.ErrorMessage)
			}

		case CmdPing:
			_ = c.sendRaw(FormatPong())
		}
	}

	c.pongMu.Lock()
	if c.pongCh != nil {
		close(c.pongCh)
		c.pongCh = nil
	}
	c.pongMu.Unlock()
}

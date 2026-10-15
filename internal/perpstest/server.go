package perpstest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// canonicalUUID is the dashed 36-character form the server requires; the unhyphenated spelling
// is rejected.
var canonicalUUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// Handshake records how one connection attempt was made.
type Handshake struct {
	Product       string
	Market        string
	Subscriptions string
	SessionID     string
	Subprotocols  []string
	UserAgent     string
	Token         string
	// Query is the whole query string, for the parameters only one product sends.
	Query url.Values
}

// Product is the product parameter and stream names the fake accepts at the handshake.
type Product struct {
	Name    string
	Streams []string
}

// Message is one frame a client sent.
type Message struct {
	Message string          `json:"message"`
	Content json.RawMessage `json:"content"`
}

// ClientRequestID reads the correlation id out of a message's content.
func (m Message) ClientRequestID() string {
	var content struct {
		ClientRequestID string `json:"clientRequestId"`
	}
	_ = json.Unmarshal(m.Content, &content)
	return content.ClientRequestID
}

// Server is a fake exchange. Create one with [New]; it shuts down with the test.
type Server struct {
	URL string

	t       *testing.T
	http    *httptest.Server
	product Product

	// Everything below is read from the server's own goroutines while a test writes it, so it
	// is reached only through the setters.
	mu                 sync.Mutex
	reject             func(w http.ResponseWriter, r *http.Request) bool
	autoPong           bool
	controlPing        time.Duration
	missedControlPongs int
	handshakes         []Handshake
	received           []Message
	conns              []*websocket.Conn
	inbox              chan Message
}

// SetReject installs a handler that answers the upgrade instead of accepting it. Use it to
// script a 429, a 503 DRAINING, or a soft ban. Returning false falls through to a normal
// upgrade.
func (s *Server) SetReject(reject func(w http.ResponseWriter, r *http.Request) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reject = reject
}

// SetAutoPong turns the pong reply to an application-level ping on or off. On by default; turn
// it off to make a client miss a pong.
func (s *Server) SetAutoPong(on bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.autoPong = on
}

// New starts a fake perpetuals exchange on a random port.
func New(t *testing.T) *Server {
	t.Helper()
	return NewProduct(t, Product{
		Name:    "perpetuals",
		Streams: []string{"orders", "trades", "publicTrades", "publicOrderBook"},
	})
}

// NewProduct starts a fake exchange for one product on a random port.
func NewProduct(t *testing.T, product Product) *Server {
	t.Helper()
	s := &Server{t: t, product: product, autoPong: true, inbox: make(chan Message, 256)}
	s.http = httptest.NewServer(http.HandlerFunc(s.handle))
	s.URL = "ws" + strings.TrimPrefix(s.http.URL, "http")
	t.Cleanup(s.http.Close)
	return s
}

// validSubscriptions mirrors the server: an empty parameter defaults to orders, and one
// unrecognised name rejects the whole request.
func (s *Server) validSubscriptions(param string) bool {
	if param == "" {
		return true
	}
	for _, name := range strings.Split(param, ",") {
		if !slices.Contains(s.product.Streams, strings.TrimSpace(name)) {
			return false
		}
	}
	return true
}

// offeredSubprotocols splits the Sec-WebSocket-Protocol header into the values it offers.
func offeredSubprotocols(r *http.Request) []string {
	var offered []string
	for _, header := range r.Header.Values("Sec-WebSocket-Protocol") {
		for _, value := range strings.Split(header, ",") {
			if value = strings.TrimSpace(value); value != "" {
				offered = append(offered, value)
			}
		}
	}
	return offered
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	reject := s.reject
	s.mu.Unlock()
	if reject != nil && reject(w, r) {
		return
	}

	query := r.URL.Query()
	hs := Handshake{
		Product:       query.Get("product"),
		Market:        query.Get("market"),
		Subscriptions: query.Get("subscriptions"),
		SessionID:     query.Get("sessionId"),
		Subprotocols:  offeredSubprotocols(r),
		UserAgent:     r.Header.Get("User-Agent"),
		Query:         query,
	}

	// The exchange validates these before the upgrade, and answers in plain text.
	switch {
	case hs.Product != s.product.Name:
		http.Error(w, "Bad Request: Missing or invalid product parameter", http.StatusBadRequest)
		return
	case hs.Market == "":
		http.Error(w, "Bad Request: Missing required market parameter", http.StatusBadRequest)
		return
	case hs.SessionID != "" && !canonicalUUID.MatchString(strings.ToLower(hs.SessionID)):
		http.Error(w, "Bad Request: Invalid sessionId parameter provided", http.StatusBadRequest)
		return
	case !s.validSubscriptions(hs.Subscriptions):
		// One unrecognised stream name rejects the whole request; it is not dropped.
		http.Error(w, "Bad Request: Invalid subscription parameter provided", http.StatusBadRequest)
		return
	case len(hs.Subprotocols) != 1 || !strings.HasPrefix(hs.Subprotocols[0], "authorization#"):
		// Exactly one value: an offer list is rejected, not negotiated.
		http.Error(w, "Unauthorized: Authentication required", http.StatusUnauthorized)
		return
	}
	hs.Token = strings.TrimPrefix(hs.Subprotocols[0], "authorization#")
	// Recorded before the 101 goes out, so a test reading Handshakes as soon as its Connect
	// returns sees this one.
	s.mu.Lock()
	s.handshakes = append(s.handshakes, hs)
	s.mu.Unlock()

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: hs.Subprotocols})
	if err != nil {
		return
	}
	conn.SetReadLimit(256_000)

	s.mu.Lock()
	s.conns = append(s.conns, conn)
	s.mu.Unlock()

	go s.controlPings(conn)
	s.readLoop(conn)
}

// controlPings mirrors the exchange's keepalive. A client that stops answering them is silently
// dropped from its drain notifications, so the fake has to send them to catch a client that
// has stopped reading.
func (s *Server) controlPings(conn *websocket.Conn) {
	interval := s.controlPingInterval()
	if interval <= 0 {
		return
	}
	for {
		time.Sleep(interval)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := conn.Ping(ctx)
		cancel()
		if err != nil {
			s.mu.Lock()
			s.missedControlPongs++
			s.mu.Unlock()
			return
		}
	}
}

// SetControlPingInterval sets how often the fake sends a WebSocket control ping. Zero, the
// default, sends none; the exchange sends one every 15 seconds.
func (s *Server) SetControlPingInterval(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.controlPing = d
}

func (s *Server) controlPingInterval() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.controlPing
}

// MissedControlPongs counts connections that stopped answering control pings, which is how the
// exchange decides a client has gone quiet.
func (s *Server) MissedControlPongs() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.missedControlPongs
}

func (s *Server) readLoop(conn *websocket.Conn) {
	for {
		_, data, err := conn.Read(context.Background())
		if err != nil {
			return
		}
		var msg Message
		if json.Unmarshal(data, &msg) != nil {
			continue
		}

		s.mu.Lock()
		s.received = append(s.received, msg)
		s.mu.Unlock()
		select {
		case s.inbox <- msg:
		default:
		}

		s.mu.Lock()
		autoPong := s.autoPong
		s.mu.Unlock()
		if msg.Message == "ping" && autoPong {
			s.pong(conn, msg.ClientRequestID())
		}
	}
}

func (s *Server) pong(conn *websocket.Conn, clientRequestID string) {
	body, _ := json.Marshal(map[string]any{
		"resultType": "pong",
		"data":       map[string]any{"statusCode": 200, "clientRequestId": clientRequestID},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = conn.Write(ctx, websocket.MessageText, body)
}

// Push sends a frame to the newest connection, which is the one a client is using after a
// reconnect or a handover.
func (s *Server) Push(frame any) {
	s.t.Helper()
	s.pushTo(s.latest(), frame)
}

// PushTo sends a frame to one specific connection, so a test can drive both halves of a
// handover.
func (s *Server) PushTo(index int, frame any) {
	s.t.Helper()
	s.pushTo(s.conn(index), frame)
}

func (s *Server) pushTo(conn *websocket.Conn, frame any) {
	s.t.Helper()
	body, err := json.Marshal(frame)
	if err != nil {
		s.t.Fatalf("perpstest: encoding frame: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageText, body); err != nil {
		s.t.Fatalf("perpstest: writing frame: %v", err)
	}
}

// CloseConn closes one connection with a close code and reason, for scripting a 1012 drain.
func (s *Server) CloseConn(index int, code int, reason string) {
	s.t.Helper()
	_ = s.conn(index).Close(websocket.StatusCode(code), reason)
}

// conn returns the index-th accepted connection, waiting for it: a client's Connect returns on
// the 101, before the goroutine that accepted the socket has recorded it.
func (s *Server) conn(index int) *websocket.Conn {
	s.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.mu.Lock()
		if index < len(s.conns) {
			conn := s.conns[index]
			s.mu.Unlock()
			return conn
		}
		s.mu.Unlock()
		if time.Now().After(deadline) {
			s.t.Fatalf("perpstest: connection %d never arrived", index)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// latest is the most recent connection, once there is one.
func (s *Server) latest() *websocket.Conn {
	s.t.Helper()
	s.conn(0)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conns[len(s.conns)-1]
}

// Handshakes returns every connection attempt that got as far as the upgrade, oldest first.
func (s *Server) Handshakes() []Handshake {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Handshake(nil), s.handshakes...)
}

// Connections is how many sockets have been accepted.
func (s *Server) Connections() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

// Received returns every frame clients have sent, oldest first.
func (s *Server) Received() []Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Message(nil), s.received...)
}

// NextMessage waits for the next frame a client sends, and fails the test if none arrives.
func (s *Server) NextMessage(timeout time.Duration) Message {
	s.t.Helper()
	select {
	case msg := <-s.inbox:
		return msg
	case <-time.After(timeout):
		s.t.Fatalf("perpstest: no message within %v", timeout)
		return Message{}
	}
}

// WaitConnections blocks until at least n sockets have been accepted.
func (s *Server) WaitConnections(n int, timeout time.Duration) {
	s.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if s.Connections() >= n {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	s.t.Fatalf("perpstest: only %d connections after %v, want %d", s.Connections(), timeout, n)
}

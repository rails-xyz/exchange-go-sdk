// Package perpstest is an in-process exchange speaking the real WebSocket protocol. [New] serves
// perpetuals; [NewProduct] any other product.
//
// It enforces what the exchange enforces at the handshake — product, market, sessionId, and
// the single Sec-WebSocket-Protocol value — and answers ping with pong. Everything above that
// is the test's to script: [Server.Push] sends a frame, [Server.Received] reads what the client
// sent, and [Server.Handshakes] shows how each connection attempt was made.
package perpstest

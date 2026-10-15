package rails

import "context"

// TokenSource supplies the bearer token that authenticates HTTP calls and WebSocket upgrades.
// Implementations must be safe for concurrent use and should cache: the token endpoint allows
// only 20 mints per minute per API key, which a reconnect storm reaches easily.
type TokenSource interface {
	Token(ctx context.Context) (string, error)
}

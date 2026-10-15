// Package auth mints and caches the bearer token every HTTP call and WebSocket upgrade needs.
//
// The token endpoint allows 20 mints per minute per API key, so minting per request — or per
// dial during a reconnect storm — is a self-inflicted outage. One token is kept until shortly
// before it expires, and concurrent callers that arrive during a refresh share its result.
package auth

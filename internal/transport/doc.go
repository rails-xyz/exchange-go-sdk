// Package transport is the HTTP half of the SDK: it authenticates a request, identifies the
// SDK on it, retries the reads that are safe to retry, and turns any of the exchange's
// incompatible error envelopes into a single [rails.Error].
package transport

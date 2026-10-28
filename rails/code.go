package rails

// Code identifies an error condition: a three-letter scope and four digits, such as "SYS-0003".
// The message next to it may be reworded; the code never changes meaning, so branch on the
// code. SYS codes are shared by every product and route and live here; a product's own route
// codes are constants in its package, such as perps.Code*.
type Code string

const (
	// CodeUnauthorized is a missing, invalid or expired credential.
	CodeUnauthorized Code = "SYS-0001"
	// CodeServerError is an internal failure. Retrying is not expected to help, and the
	// request's effect is unknown, so look the order up.
	CodeServerError Code = "SYS-0002"
	// CodeTooManyRequests is a rate limit; Error.RetryAfter says how long to wait.
	CodeTooManyRequests Code = "SYS-0003"
	// CodeServiceUnavailable is a dependency down or a gateway draining. Try again shortly.
	CodeServiceUnavailable Code = "SYS-0004"
	// CodeRetryableError is a transient internal failure before the request took effect, such
	// as a command that could not be queued. It may be sent again.
	CodeRetryableError Code = "SYS-0005"
	// CodeBadRequest is a malformed or invalid request field; the message names it.
	CodeBadRequest Code = "SYS-0006"
	// CodeNotFound is an order, market or resource that does not exist for this account.
	CodeNotFound Code = "SYS-0007"
	// CodeForbidden is an action this account may not perform.
	CodeForbidden Code = "SYS-0008"
	// CodeRequestTimeout is the exchange giving up on the request internally. Its effect is
	// unknown, so look the order up.
	CodeRequestTimeout Code = "SYS-0009"
	// CodeDuplicateRequest is a clientRequestId already accepted within the last minute. The
	// original request stands and is not executed a second time.
	CodeDuplicateRequest Code = "SYS-0010"
	// CodeSoftBanned is a connection refused until the moment named in the message.
	CodeSoftBanned Code = "SYS-0011"
	// CodeSubscriptionLimit is a connection that would exceed its stream subscriptions.
	CodeSubscriptionLimit Code = "SYS-0012"
	// CodeConnectionLimit is an account at its maximum connections to a market.
	CodeConnectionLimit Code = "SYS-0013"
	// CodeTradingDisabled is an account that may not trade: disabled, or its wallet unverified.
	CodeTradingDisabled Code = "SYS-0014"
	// CodeMarketDisabled is a Perpetuals market that is disabled or does not exist, returned to a
	// WebSocket connection, subscription or command that names it.
	CodeMarketDisabled Code = "SYS-0015"
)

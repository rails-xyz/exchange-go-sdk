package options

// OrderType is the side of an order. Requests call it orderType; order reads call it side.
type OrderType string

const (
	OrderTypeBuy  OrderType = "buy"
	OrderTypeSell OrderType = "sell"
)

// TradeType is how an order is priced. Requests call it tradeType; order reads call it type.
type TradeType string

const (
	TradeTypeLimit  TradeType = "limit"
	TradeTypeMarket TradeType = "market"
)

// OptionType is whether a contract is a call or a put.
type OptionType string

const (
	OptionTypeCall OptionType = "C"
	OptionTypePut  OptionType = "P"
)

// ExpiryType is the cycle an expiry belongs to.
type ExpiryType string

const (
	ExpiryTypeWeekly  ExpiryType = "WEEKLY"
	ExpiryTypeMonthly ExpiryType = "MONTHLY"
)

// PositionSide is the direction of a position.
type PositionSide string

const (
	PositionSideLong  PositionSide = "long"
	PositionSideShort PositionSide = "short"
	// PositionSideFlat is only in a [PositionsDelta]: the position closed, so drop it.
	PositionSideFlat PositionSide = "flat"
)

// ExecutionType is which side of a match a public trade entry describes.
type ExecutionType string

const (
	ExecutionTypeTaker ExecutionType = "taker"
	ExecutionTypeMaker ExecutionType = "maker"
)

// OpenOrderKind is how an open-orders delta changes the set of open orders.
type OpenOrderKind string

const (
	OpenOrderAdded   OpenOrderKind = "add"
	OpenOrderDropped OpenOrderKind = "drop" // filled or cancelled
)

// FillType says whether an order filled in full.
type FillType string

const (
	FillTypeComplete FillType = "complete"
	FillTypePartial  FillType = "partial"
)

// Status is where an order is, as [Client.OrderStatus] and the WebSocket's [Order] report it.
type Status string

const (
	StatusPending   Status = "pending"
	StatusAccepted  Status = "accepted"
	StatusOpen      Status = "open"
	StatusCompleted Status = "completed"
	StatusCancelled Status = "cancelled"
	StatusRejected  Status = "rejected"
	StatusNotFound  Status = "not_found"
)

// Action is the last request recorded against an order.
type Action string

const (
	ActionCreate Action = "create"
	ActionModify Action = "modify"
	ActionCancel Action = "cancel"
)

// Page is one page of a paginated read. Pass Next back as the PageToken of the following
// request; it is empty on the last page.
type Page[T any] struct {
	_ struct{}

	Items []T
	Next  string
}

// Expiration is one expiry date of a market and what trades on it.
type Expiration struct {
	_ struct{}

	ExpiryDate        string // YYYY-MM-DD
	ExpiryType        ExpiryType
	ContractCount     int
	AtmIV             string
	ExpectedPriceMove string // the expected move of IndexPrice by this expiry
	IndexPrice        string
}

// Contract is one tradable option: its terms, its prices and its greeks. ContractName is what
// every order names as its Contract.
type Contract struct {
	_ struct{}

	ContractName string
	Underlying   string
	Strike       string
	OptionType   OptionType
	ExpiryDate   string // YYYY-MM-DD
	ExpiryType   ExpiryType
	MarkPrice    string
	BidPrice     string
	AskPrice     string
	MarkIV       string
	BidIV        string
	AskIV        string
	Forward      string
	TickSize     string
	ContractSize string
	// LotSize is the quantity step: an order quantity must be a positive multiple of it. "1" means
	// whole contracts only.
	LotSize        string
	SettlementType string
	Delta          string
	Gamma          string
	Vega           string
	Theta          string
	Rho            string
}

// AccountSummary is the account's balance sheet, in USDT.
type AccountSummary struct {
	_ struct{}

	AccountBalance      string
	AvailableBalance    string // available for new orders
	WithdrawableBalance string
	// TotalEquity is AccountBalance plus the signed mark value of the open positions.
	TotalEquity string
	// MarginUsed is the initial margin locked by open positions and resting orders.
	MarginUsed        string
	MaintenanceMargin string
	// InitialMarginRatio and MaintenanceMarginRatio are fractions of TotalEquity, 1 when it is
	// not positive.
	InitialMarginRatio     string
	MaintenanceMarginRatio string
	PnL                    string // realized, cumulative
	// UnrealizedPnL is reported only; it is not a term of TotalEquity.
	UnrealizedPnL string
	Fees          string
	Volume        string
}

// Position is an open position on one contract.
//
// Qty, Value and NotionalValue are signed: negative for a short, positive for a long. Side says
// the same thing as the sign of Qty. Value is the cost at entry and NotionalValue the mark
// value; NotionalValue and UnrealizedPnL are "0" while the mark price is unavailable. LiqPrice
// comes only over HTTP, Fees and UpdatedAt only over the WebSocket.
type Position struct {
	_ struct{}

	Contract        string
	Side            PositionSide
	OptionType      OptionType
	Qty             string
	Value           string
	EntryPrice      string
	MarkPrice       string
	MarkIV          string
	UnderlyingPrice string
	// LiqPrice is not a number: the exchange sends "--".
	LiqPrice string
	// BreakEvenPrice is the underlying price at expiry where the position's PnL is zero, net
	// of the premium and opening fees.
	BreakEvenPrice string
	// InitialMargin is "--" over HTTP and "0" over the WebSocket for a long.
	InitialMargin string
	RealizedPnL   string
	UnrealizedPnL string
	NotionalValue string
	// Fees is what opening the contracts still held cost.
	Fees      string
	UpdatedAt int64
}

// RecentOrder is one order from the account's history, as [Client.RecentOrders] pages
// through it.
type RecentOrder struct {
	_ struct{}

	OrderID        string
	Contract       string
	Side           OrderType
	Type           TradeType
	FillType       FillType // empty for a cancelled order
	Price          string   // the price requested
	Quantity       string   // the full order quantity
	FilledQuantity string
	// AvgFillPrice is "0" while nothing has filled.
	AvgFillPrice string
	Fees         string // USDT; empty for a cancelled order, and over the WebSocket
	CreatedAt    int64
	UpdatedAt    int64
}

// OpenOrder is an order resting on the book. Quantity is the full order quantity; what is left
// to fill is Quantity minus FilledQuantity.
type OpenOrder struct {
	_ struct{}

	OrderID        string
	Contract       string
	Side           OrderType
	Type           TradeType
	Price          string
	Quantity       string
	FilledQuantity string
	// AvgFillPrice is "0" while nothing has filled.
	AvgFillPrice string
	ReduceOnly   bool
	PostOnly     bool
	CreatedAt    int64
	UpdatedAt    int64
}

// Order is an order as the WebSocket describes it, in a command's answer or an event.
type Order struct {
	_ struct{}

	Contract       string
	OrderID        string
	OrderType      OrderType
	TradeType      TradeType
	Price          string // the limit price, or for a market order the price expected
	Quantity       string
	FilledQuantity string
	// AvgFillPrice is "0" while nothing has filled.
	AvgFillPrice    string
	Status          Status
	PostOnly        bool
	CreatedAt       int64
	UpdatedAt       int64
	ClientRequestID string
}

// OrderStatus is what the exchange has recorded about an order. Fields it has not recorded yet
// are empty.
type OrderStatus struct {
	_ struct{}

	OrderID           string
	ClientRequestID   string
	Contract          string
	Status            Status
	Action            Action
	Body              string
	Reason            string // why the order was rejected
	Quantity          string
	FilledQuantity    string
	RemainingQuantity string
	UpdatedAt         int64
}

// Deposit is one deposit into the account. Market is the asset deposited, such as USDT.
type Deposit struct {
	_ struct{}

	Market       string
	Value        string
	RefChainTxID string
	CreatedAt    int64
}

// Withdrawal is one withdrawal from the account. Market is the asset withdrawn, such as USDT;
// Value is net of Fees. TxHash is empty until the withdrawal has been processed.
type Withdrawal struct {
	_ struct{}

	Market    string
	Value     string
	Fees      string
	TxHash    string
	CreatedAt int64
}

// Queued is an order request the exchange has queued. Queued is not accepted: the outcome
// arrives on the order stream of the options WebSocket, and [Client.OrderStatus] looks it up.
type Queued struct {
	_ struct{}

	OrderID         string
	ClientRequestID string
	Contract        string
	MessageID       string // the id the queue assigned the request
}

// QueuedCancelAll is the cancels a [Client.CancelAllOrders] queued, one per open order it
// found. Unqueued lists the open orders it found but could not queue a cancel for, when the
// queue refused part-way; cancel those again.
type QueuedCancelAll struct {
	_ struct{}

	ClientRequestID string
	Contract        string // empty when every contract was swept
	Count           int
	Cancels         []Queued
	Unqueued        []UnqueuedCancel
}

// UnqueuedCancel is an open order a cancel-all found but could not queue a cancel for.
type UnqueuedCancel struct {
	_ struct{}

	OrderID  string
	Contract string
}

// ContractState is a contract as the contracts stream reports it: its terms, the top of its
// book, its IVs and its greeks. BidIV and AskIV may be empty when that side has no firm quote.
type ContractState struct {
	_ struct{}

	ContractName string
	Underlying   string // such as BTC-USDT
	Strike       string
	OptionType   OptionType
	ExpiryDate   string // YYYY-MM-DD
	MarkPrice    string
	BidPrice     string
	AskPrice     string
	BidSize      string
	AskSize      string
	MarkIV       string
	BidIV        string
	AskIV        string
	Forward      string
	Delta        string
	Gamma        string
	Vega         string
	Theta        string
	Rho          string
	OpenInterest string
	Volume24h    string
	LotSize      string // order quantities must be positive multiples of it
}

// ExpectedMove is the move of the index an expiry's options imply by that expiry.
type ExpectedMove struct {
	_ struct{}

	ExpiryDate        string // YYYY-MM-DD
	Underlying        string
	AtmIV             string
	ExpectedPriceMove string
	IndexPrice        string // what ExpectedPriceMove is measured from
}

// ContractsSnapshot is one page of an expiry's contracts, with its expected move.
type ContractsSnapshot struct {
	_ struct{}

	Contracts []ContractState
	Expiries  []ExpectedMove
	Next      string
}

// Quote is the top of one contract's book. A side with no orders has its fields empty.
type Quote struct {
	_ struct{}

	Contract   string
	BestBid    string
	BestBidQty string
	BestBidIV  string
	BestAsk    string
	BestAskQty string
	BestAskIV  string
	Ts         int64
}

// OrderBookLevel is the resting quantity at one price.
type OrderBookLevel struct {
	_ struct{}

	OrderType OrderType
	Price     string
	Quantity  string
	IV        string
}

// BestQuote is the best price on one side of a book, zero when that side is empty.
type BestQuote struct {
	_ struct{}

	Price string
	Size  string
	IV    string
}

// OrderBook is one contract's whole book.
type OrderBook struct {
	_ struct{}

	Contract string
	Orders   []OrderBookLevel
	BestBid  BestQuote
	BestAsk  BestQuote
	Ts       int64
}

// PublicTrade is one side of a match on a contract; the taker and maker entries of a match share
// its MatchID.
type PublicTrade struct {
	_ struct{}

	ExecutionType ExecutionType
	MatchID       string
	Price         string
	Quantity      string
	Side          OrderType
	UpdatedAt     int64
}

// Settlement is how a position was settled at its contract's expiry.
type Settlement struct {
	_ struct{}

	Contract        string
	Side            PositionSide
	OptionType      OptionType
	Qty             string
	Value           string
	EntryPrice      string
	MarkPrice       string
	UnderlyingPrice string
	BreakEvenPrice  string
	SettlementPrice string // of the underlying
	Payoff          string
	DeliveryFee     string
	RealizedPnL     string
	SettledAt       int64
}

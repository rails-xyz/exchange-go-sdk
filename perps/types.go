package perps

// MarketAll names every market at once, wherever a market is asked for. A connection to it
// receives every market's events, and each request on it has to name its own market.
const MarketAll = "ALL"

// OrderType is the side of an order. The API's own name for it is orderType.
type OrderType string

const (
	OrderTypeBuy  OrderType = "buy"
	OrderTypeSell OrderType = "sell"
)

// TradeType is how an order is priced. The API's own name for it is tradeType.
type TradeType string

const (
	TradeTypeLimit  TradeType = "limit"
	TradeTypeMarket TradeType = "market"
)

// OrderStatus is where an order is in its life. The four terminal states are Filled, Rejected,
// Cancelled and Modified; an order in one of them has left the book.
type OrderStatus string

const (
	OrderStatusPending    OrderStatus = "pending"
	OrderStatusActive     OrderStatus = "active"
	OrderStatusFilled     OrderStatus = "filled"
	OrderStatusRejected   OrderStatus = "rejected"
	OrderStatusCancelling OrderStatus = "cancelling"
	OrderStatusCancelled  OrderStatus = "cancelled"
	OrderStatusModifying  OrderStatus = "modifying"
	OrderStatusModified   OrderStatus = "modified"
)

// Order is a perpetuals order as the exchange reports it, over HTTP and over the WebSocket
// alike.
//
// Quantity is what is left to fill, not the original size: an order for 10 that has filled 4
// reports Quantity "6" and FilledQuantity "4".
type Order struct {
	_ struct{}

	Market          string
	OrderID         string
	OrderType       OrderType
	TradeType       TradeType
	Price           string
	Quantity        string
	FilledQuantity  string
	Leverage        string
	CreatedAt       int64
	UpdatedAt       int64
	OrderStatus     OrderStatus
	ClientRequestID string
	ReduceOnly      bool
	PostOnly        bool
	AvgFillPrice    string // empty until at least one fill has happened
}

// PositionType is the direction of a position. The API's own name for it is type.
type PositionType string

const (
	PositionTypeLong  PositionType = "LONG"
	PositionTypeShort PositionType = "SHORT"
)

// MarginMode is how a position is collateralised: from the whole account's balance, or from
// the margin set aside for it alone.
type MarginMode string

const (
	MarginModeCross    MarginMode = "C"
	MarginModeIsolated MarginMode = "I"
)

// Position is an open perpetuals position as the exchange reports it. The exchange returns only
// positions with a non-zero quantity, so a market with no position is simply absent.
//
// Quantity, Value and NotionalValue are signed: negative for a short, positive for a long. Type
// says the same thing as the sign of Quantity. Value is the entry cost, so Value / Quantity is
// the average entry price; NotionalValue is mark price × Quantity.
type Position struct {
	_ struct{}

	Market           string
	Value            string
	Quantity         string
	Funding          string // signed: funding is both paid and received
	RealizedPnL      string
	UnrealizedPnL    string
	LiquidationPrice string
	Type             PositionType
	NotionalValue    string
	MarginMode       MarginMode
}

// Page is one page of a paginated read. Pass Next back as the cursor for the following page;
// it is empty on the last one.
type Page[T any] struct {
	_ struct{}

	Items []T
	Next  string
}

// Balances is the account's balance sheet, overall and per market. Amounts are USDT; quantities
// are in the market's base asset. TransferIn, TransferOut, PromoCredit and LiqFees are empty when
// zero.
type Balances struct {
	_ struct{}

	TotalBalance        string
	CrossMarginBalance  string
	AvailableBalance    string
	WithdrawableBalance string
	Deposit             string
	Withdraw            string
	WithdrawFees        string
	TransferIn          string
	TransferOut         string
	PromoCredit         string
	Leverage            string
	PnL                 string
	Funding             string
	Fees                string
	LiqFees             string
	Volume              string
	// Markets is keyed by market name.
	Markets map[string]MarketBalance
}

// MarketBalance is the account's standing in one market. Funding and PositionFunding are signed:
// funding is both paid and received.
type MarketBalance struct {
	_ struct{}

	Position          string
	PositionQuantity  string
	PositionPnL       string
	PositionFunding   string
	PositionFees      string
	UnrealizedPnL     string
	OpenBuy           string
	OpenBuyQty        string
	OpenSell          string
	OpenSellQty       string
	Margin            string
	MaintenanceMargin string
	Leverage          string
	MarginMode        MarginMode
	LiquidationPrice  string
	LiquidationFees   string
	MarkPrice         string
	PnL               string
	Funding           string
	Fees              string
	Volume            string
}

// Deposit is one deposit into the account. Market is the asset deposited, such as USDT.
type Deposit struct {
	_ struct{}

	Market       string
	Value        string
	TxCreated    int64
	RefChainTxID string
}

// Withdrawal is one withdrawal from the account. Market is the asset withdrawn, such as USDT.
type Withdrawal struct {
	_ struct{}

	Market    string
	Value     string
	Fees      string
	TxCreated int64
	TxHash    string
}

// Funding is one funding payment on a position. Value is signed: positive when received,
// negative when paid. PositionSize is signed too, negative for a short. Rate is a decimal rate,
// so 0.0001125 is 0.01125%.
type Funding struct {
	_ struct{}

	Market       string
	Value        string
	Rate         string
	IndexPrice   string
	PositionSize string
	TxCreated    int64
}

// ExecutionType says which side of a fill an order was on. The API's own name for it is
// executionType.
type ExecutionType string

const (
	ExecutionTypeMaker ExecutionType = "maker"
	ExecutionTypeTaker ExecutionType = "taker"
)

// FillType says whether a fill completed its order. The API's own name for it is fillType.
type FillType string

const (
	FillTypeComplete FillType = "complete"
	FillTypePartial  FillType = "partial"
)

// TriggerType says what placed an order, when it was not the account. The API's own name for
// it is triggerType.
type TriggerType string

const (
	TriggerTypeNone        TriggerType = "none"
	TriggerTypeLiquidation TriggerType = "liquidation"
	TriggerTypeStopLoss    TriggerType = "stop_loss"
	TriggerTypeTakeProfit  TriggerType = "take_profit"
)

// CompletedOrder is one fill, the unit [Client.CompletedOrders] pages through: an order filled
// in three parts appears three times, once per MatchID.
type CompletedOrder struct {
	_ struct{}

	Market          string
	MatchID         string
	OrderID         string
	OrderType       OrderType
	TradeType       TradeType
	Price           string
	Quantity        string
	ExecutionType   ExecutionType
	FillType        FillType
	Fees            string
	FilledAt        int64
	TransactionHash string
	TriggerType     TriggerType
	TriggerPrice    string
}

// Trade is one fill on one of the account's orders. Quantity is the fill's and
// OriginalQuantity the order's; CreatedAt is the order's and UpdatedAt the fill's.
// TriggerPrice is "0" when TriggerType is TriggerTypeNone.
type Trade struct {
	_ struct{}

	Market           string
	OrderID          string
	MatchID          string
	OrderType        OrderType
	TradeType        TradeType
	ExecutionType    ExecutionType
	FillType         FillType
	Price            string
	Quantity         string
	OriginalQuantity string
	Leverage         string
	Fees             string
	CreatedAt        int64
	UpdatedAt        int64
	TriggerType      TriggerType
	TriggerPrice     string
	TriggerCreatedAt int64
}

// PublicTrade is one side of a fill on the market: each match appears twice, as its maker
// and as its taker.
type PublicTrade struct {
	_ struct{}

	ExecutionType ExecutionType
	MatchID       string
	OrderType     OrderType
	Price         string
	Quantity      string
	UpdatedAt     int64
}

// MarketDataType is which figure a [MarketDataPoint] carries. The API's own name for it is
// type.
type MarketDataType string

const (
	MarketDataTypeIndex        MarketDataType = "INDEX"
	MarketDataTypeMark         MarketDataType = "MARK"
	MarketDataTypeBid          MarketDataType = "BID"
	MarketDataTypeAsk          MarketDataType = "ASK"
	MarketDataTypeFunding      MarketDataType = "FUNDING"
	MarketDataTypeNextFunding  MarketDataType = "NEXT_FUNDING"
	MarketDataTypeOpenInterest MarketDataType = "OPEN_INTEREST"
	MarketDataTypeVolume24H    MarketDataType = "24H_VOLUME"
	MarketDataTypeCount24H     MarketDataType = "24H_COUNT"
)

// MarketDataPoint is one figure about a market, from the index that produced it. Funding
// values are decimal rates, not percentages.
type MarketDataPoint struct {
	_ struct{}

	Market    string
	Type      MarketDataType
	Index     string
	Value     string
	UpdatedAt int64
}

// OrderBookLevel is one price level of the book.
type OrderBookLevel struct {
	_ struct{}

	OrderType OrderType
	Price     string
	Quantity  string
}

// OrderBookSnapshot is the whole book at one moment, over HTTP or from [Conn.EmitPublicOrderBook].
// Bids are OrderTypeBuy, asks OrderTypeSell.
type OrderBookSnapshot struct {
	_ struct{}

	Orders []OrderBookLevel
	Ts     int64
}

// ProductSpecification is a market's trading rules: price and quantity steps, order size
// bounds in USD (MaxOrderSizeUSD empty means none), and the funding interval.
type ProductSpecification struct {
	_ struct{}

	Market               string
	TickSize             string
	LotSize              string
	FundingIntervalHours int
	MinOrderSizeUSD      string
	MaxOrderSizeUSD      string
	ContractType         string
}

// ContractSpecification describes a perpetual contract. UnderlyingPrice, TickSize and LotSize
// are numbers; the other fields are the exchange's own wording.
type ContractSpecification struct {
	_ struct{}

	ContractName         string
	UnderlyingPrice      string
	PriceIndex           string
	IndexUpdateFrequency string
	ContractType         string
	MarginType           string
	TradingHours         string
	FundingTimes         string
	FundingRateCap       string
	TickSize             string
	LotSize              string
	MinOrderNotional     string
	PriceUnit            string
	QuantityUnit         string
}

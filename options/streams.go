package options

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/rails-xyz/exchange-go-sdk/internal/ws"
)

const (
	opEmitContracts      = "emitOptionsContracts"
	opEmitQuotes         = "emitOptionsQuotes"
	opEmitOrderBook      = "emitPublicOrderBook"
	opEmitPublicTrades   = "emitPublicCompletedOrders"
	opEmitPositions      = "emitOptionsPositions"
	opEmitOpenOrders     = "emitOptionsOpenOrders"
	opEmitRecentOrders   = "emitOptionsRecentOrders"
	opEmitSettlements    = "emitOptionsSettlements"
	opEmitAccountSummary = "emitOptionsAccountSummary"

	resultContracts      = "optionsContracts"
	resultQuotes         = "optionsQuotes"
	resultOrderBook      = "publicOrderBook"
	resultPublicTrades   = "publicCompletedOrders"
	resultPositions      = "optionsPositions"
	resultOpenOrders     = "optionsOpenOrders"
	resultRecentOrders   = "optionsRecentOrders"
	resultSettlements    = "optionsSettlements"
	resultAccountSummary = "optionsAccountSummary"
)

// ExpiryQuery names one expiry of a market and a page of what trades on it.
type ExpiryQuery struct {
	_ struct{}

	Market     string // such as BTC-USDT
	ExpiryDate string // YYYY-MM-DD, as [Expiration] spells it
	PageSize   int    // 1 to 200; zero leaves the exchange's default
	PageToken  string
}

// ContractQuery names a contract and a page of what is recorded for it.
type ContractQuery struct {
	_ struct{}

	Contract  string
	PageSize  int // 1 to 200; zero leaves the exchange's default
	PageToken string
}

// EmitContracts asks for a page of an expiry's contracts, the state a stream of
// [ContractUpdate] continues from. It needs [SubscriptionContracts] for [ScopeAll] or any scope
// of q.Market.
func (c *Conn) EmitContracts(ctx context.Context, q ExpiryQuery) (*ContractsSnapshot, error) {
	underlying, err := c.guardExpiry(opEmitContracts, SubscriptionContracts, q)
	if err != nil {
		return nil, err
	}
	var data contractsData
	crid := uuid.NewString()
	if err := c.emit(ctx, opEmitContracts, resultContracts, underlying, newExpiryContent(crid, underlying, q), crid, &data); err != nil {
		return nil, err
	}
	snapshot := &ContractsSnapshot{
		Contracts: convert(data.Contracts, wireContractState.toContractState),
		Expiries:  convert(data.Expiries, wireExpectedMove.toExpectedMove),
		Next:      data.NextPageToken,
	}
	return snapshot, repeatedToken(opEmitContracts, q.PageToken, snapshot.Next)
}

// EmitQuotes asks for a page of the tops of an expiry's books, the state a stream of
// [QuoteUpdate] continues from. It needs [SubscriptionQuotes] for [ScopeAll] or any scope of
// q.Market.
func (c *Conn) EmitQuotes(ctx context.Context, q ExpiryQuery) (*Page[Quote], error) {
	underlying, err := c.guardExpiry(opEmitQuotes, SubscriptionQuotes, q)
	if err != nil {
		return nil, err
	}
	var data quotesData
	crid := uuid.NewString()
	if err := c.emit(ctx, opEmitQuotes, resultQuotes, underlying, newExpiryContent(crid, underlying, q), crid, &data); err != nil {
		return nil, err
	}
	page := &Page[Quote]{Items: convert(data.Quotes, wireQuote.toQuote), Next: data.NextPageToken}
	return page, repeatedToken(opEmitQuotes, q.PageToken, page.Next)
}

// EmitOrderBook asks for a contract's whole book, the state a stream of [OrderBookDelta]
// continues from. It needs [SubscriptionPublicOrderBook] for a scope covering the contract.
func (c *Conn) EmitOrderBook(ctx context.Context, contract string) (*OrderBook, error) {
	if err := c.guardEmitContract(opEmitOrderBook, SubscriptionPublicOrderBook, contract); err != nil {
		return nil, err
	}
	var data orderBookData
	crid := uuid.NewString()
	if err := c.emit(ctx, opEmitOrderBook, resultOrderBook, contract, newContractContent(crid, contract, ContractQuery{}), crid, &data); err != nil {
		return nil, err
	}
	book := data.OrderBook.toOrderBook()
	return &book, nil
}

// EmitPublicTrades asks for a page of a contract's recent matches, the state a stream of
// [PublicTrades] continues from. It needs [SubscriptionPublicTrades] for a scope covering
// q.Contract.
func (c *Conn) EmitPublicTrades(ctx context.Context, q ContractQuery) (*Page[PublicTrade], error) {
	if err := c.guardEmitContract(opEmitPublicTrades, SubscriptionPublicTrades, q.Contract); err != nil {
		return nil, err
	}
	var data publicTradesData
	crid := uuid.NewString()
	if err := c.emit(ctx, opEmitPublicTrades, resultPublicTrades, q.Contract, newContractContent(crid, q.Contract, q), crid, &data); err != nil {
		return nil, err
	}
	page := &Page[PublicTrade]{Items: convert(data.Trades, wirePublicTrade.toPublicTrade), Next: data.NextPageToken}
	return page, repeatedToken(opEmitPublicTrades, q.PageToken, page.Next)
}

// EmitPositions asks for a page of your open positions, the state a stream of [PositionsDelta]
// continues from; a zero PageSize asks for every one. It needs [SubscriptionPositions] for a
// scope covering q.Contract, or for [ScopeAll] when q.Contract is empty, which asks for every
// contract.
func (c *Conn) EmitPositions(ctx context.Context, q ContractQuery) (*Page[Position], error) {
	var data positionsData
	if err := c.emitForContract(ctx, opEmitPositions, resultPositions, SubscriptionPositions, q, &data); err != nil {
		return nil, err
	}
	page := &Page[Position]{Items: convert(data.Positions, wirePosition.toPosition), Next: data.NextPageToken}
	return page, repeatedToken(opEmitPositions, q.PageToken, page.Next)
}

// EmitOpenOrders asks for a page of your open orders, the state a stream of [OpenOrdersDelta]
// continues from. It needs [SubscriptionOpenOrders] for a scope covering q.Contract, or for
// [ScopeAll] when q.Contract is empty, which asks for every contract.
func (c *Conn) EmitOpenOrders(ctx context.Context, q ContractQuery) (*Page[OpenOrder], error) {
	var data openOrdersData
	if err := c.emitForContract(ctx, opEmitOpenOrders, resultOpenOrders, SubscriptionOpenOrders, q, &data); err != nil {
		return nil, err
	}
	page := &Page[OpenOrder]{Items: convert(data.Orders, wireOpenOrder.toOpenOrder), Next: data.NextPageToken}
	return page, repeatedToken(opEmitOpenOrders, q.PageToken, page.Next)
}

// EmitRecentOrders asks for a page of your order history, the state a stream of
// [RecentOrdersDelta] continues from. It needs [SubscriptionRecentOrders] for a scope covering
// q.Contract, or for [ScopeAll] when q.Contract is empty, which asks for every contract.
func (c *Conn) EmitRecentOrders(ctx context.Context, q ContractQuery) (*Page[RecentOrder], error) {
	var data recentOrdersData
	if err := c.emitForContract(ctx, opEmitRecentOrders, resultRecentOrders, SubscriptionRecentOrders, q, &data); err != nil {
		return nil, err
	}
	page := &Page[RecentOrder]{Items: convert(data.Orders, wireRecentOrder.toRecentOrder), Next: data.NextPageToken}
	return page, repeatedToken(opEmitRecentOrders, q.PageToken, page.Next)
}

// EmitSettlements asks for a page of your settlements on every contract; a PageSize above 50 is
// taken as 50. q.Contract only picks where the answer is delivered: it needs
// [SubscriptionSettlements] for a scope covering it, or for [ScopeAll] when it is empty.
func (c *Conn) EmitSettlements(ctx context.Context, q ContractQuery) (*Page[Settlement], error) {
	var data settlementsData
	if err := c.emitForContract(ctx, opEmitSettlements, resultSettlements, SubscriptionSettlements, q, &data); err != nil {
		return nil, err
	}
	page := &Page[Settlement]{Items: convert(data.Settlements, wireSettlement.toSettlement), Next: data.NextPageToken}
	return page, repeatedToken(opEmitSettlements, q.PageToken, page.Next)
}

// EmitAccountSummary asks for the account's balance sheet. Nothing pushes it, so this is the
// only way to read it over the WebSocket. It needs [SubscriptionAccountSummary] for any scope.
func (c *Conn) EmitAccountSummary(ctx context.Context) (*AccountSummary, error) {
	if _, ok := (*c.subscribed.Load())[SubscriptionAccountSummary]; !ok {
		return nil, fmt.Errorf("%s: %w: %s", opEmitAccountSummary, ErrNotSubscribed, SubscriptionAccountSummary)
	}
	var data accountSummaryData
	crid := uuid.NewString()
	if err := c.emit(ctx, opEmitAccountSummary, resultAccountSummary, "", accountSummaryContent{ClientRequestID: crid}, crid, &data); err != nil {
		return nil, err
	}
	return data.AccountSummary.toAccountSummary(), nil
}

// guardExpiry checks an expiry emit and returns the underlying it names, such as BTC.
func (c *Conn) guardExpiry(op string, stream Subscription, q ExpiryQuery) (string, error) {
	if q.Market == "" || q.ExpiryDate == "" {
		return "", fmt.Errorf("%s: market and expiry date are required", op)
	}
	underlying, _, _ := strings.Cut(q.Market, "-")
	if !c.subscribed.Load().coversUnderlying(stream, underlying) {
		return "", fmt.Errorf("%s: %w: %s for %s", op, ErrNotSubscribed, stream, q.Market)
	}
	return underlying, nil
}

func (c *Conn) guardEmitContract(op string, stream Subscription, contract string) error {
	if contract == "" {
		return fmt.Errorf("%s: contract is required", op)
	}
	return c.guardTarget(op, stream, contract)
}

func (c *Conn) guardTarget(op string, stream Subscription, target string) error {
	if !c.subscribed.Load().covers(stream, target) {
		return fmt.Errorf("%s: %w: %s for %s", op, ErrNotSubscribed, stream, target)
	}
	return nil
}

// emitForContract sends an emit of one contract, or of every contract when q names none.
func (c *Conn) emitForContract(ctx context.Context, op, resultType string, stream Subscription, q ContractQuery, out any) error {
	// Omitted, the exchange would use the connection's scope instead, and refuse it.
	market := q.Contract
	if market == "" {
		market = marketAll
	}
	if err := c.guardTarget(op, stream, market); err != nil {
		return err
	}
	crid := uuid.NewString()
	return c.emit(ctx, op, resultType, market, newContractContent(crid, market, q), crid, out)
}

func (c *Conn) emit(ctx context.Context, op, resultType, market string, content any, crid string, out any) error {
	reply, err := c.ws.Send(ctx, ws.Send{
		Message:         op,
		ClientRequestID: crid,
		// An error is answered under the command's own name, or the snapshot's.
		ReplyTypes: []string{resultType, op},
		Market:     market,
		Content:    content,
	})
	if err != nil {
		return err
	}
	if err := json.Unmarshal(reply.Frame.Data, out); err != nil {
		return fmt.Errorf("%s: decoding the answer: %w", op, err)
	}
	return nil
}

func convert[W, T any](wire []W, to func(W) T) []T {
	out := make([]T, 0, len(wire))
	for _, w := range wire {
		out = append(out, to(w))
	}
	return out
}

func newExpiryContent(crid, underlying string, q ExpiryQuery) expiryContent {
	return expiryContent{
		ClientRequestID: crid, Underlying: underlying, ExpirationDate: q.ExpiryDate,
		PageSize: q.PageSize, PageToken: q.PageToken,
	}
}

func newContractContent(crid, market string, q ContractQuery) contractContent {
	return contractContent{ClientRequestID: crid, Market: market, PageSize: q.PageSize, PageToken: q.PageToken}
}

type expiryContent struct {
	ClientRequestID string `json:"clientRequestId"`
	Underlying      string `json:"underlying"`
	ExpirationDate  string `json:"expirationDate"`
	PageSize        int    `json:"pageSize,omitempty"`
	PageToken       string `json:"pageToken,omitempty"`
}

type contractContent struct {
	ClientRequestID string `json:"clientRequestId"`
	Market          string `json:"market"`
	PageSize        int    `json:"pageSize,omitempty"`
	PageToken       string `json:"pageToken,omitempty"`
}

type accountSummaryContent struct {
	ClientRequestID string `json:"clientRequestId"`
}

// The snapshots and deltas in their wire spelling; each *Data type decodes a stream's snapshot
// and its delta alike. Leverage (always "0") and initiatorType are decoded but not passed on.

type contractsData struct {
	StatusCode      int                 `json:"statusCode"`
	ClientRequestID string              `json:"clientRequestId"`
	Contracts       []wireContractState `json:"contracts"`
	Expiries        []wireExpectedMove  `json:"expiries"`
	NextPageToken   string              `json:"nextPageToken"`
}

type quotesData struct {
	StatusCode      int         `json:"statusCode"`
	ClientRequestID string      `json:"clientRequestId"`
	Quotes          []wireQuote `json:"quotes"`
	NextPageToken   string      `json:"nextPageToken"`
}

type orderBookData struct {
	StatusCode      int           `json:"statusCode"`
	ClientRequestID string        `json:"clientRequestId"`
	OrderBook       wireOrderBook `json:"orderBook"`
}

type publicTradesData struct {
	StatusCode      int               `json:"statusCode"`
	ClientRequestID string            `json:"clientRequestId"`
	Trades          []wirePublicTrade `json:"trades"`
	NextPageToken   string            `json:"nextPageToken"`
}

type positionsData struct {
	StatusCode      int            `json:"statusCode"`
	ClientRequestID string         `json:"clientRequestId"`
	Positions       []wirePosition `json:"positions"`
	NextPageToken   string         `json:"nextPageToken"`
}

type openOrdersData struct {
	StatusCode      int             `json:"statusCode"`
	ClientRequestID string          `json:"clientRequestId"`
	Orders          []wireOpenOrder `json:"orders"`
	NextPageToken   string          `json:"nextPageToken"`
}

type recentOrdersData struct {
	StatusCode      int               `json:"statusCode"`
	ClientRequestID string            `json:"clientRequestId"`
	Orders          []wireRecentOrder `json:"orders"`
	NextPageToken   string            `json:"nextPageToken"`
}

type settlementsData struct {
	StatusCode      int              `json:"statusCode"`
	ClientRequestID string           `json:"clientRequestId"`
	Settlements     []wireSettlement `json:"settlements"`
	NextPageToken   string           `json:"nextPageToken"`
}

type accountSummaryData struct {
	StatusCode      int                `json:"statusCode"`
	ClientRequestID string             `json:"clientRequestId"`
	AccountSummary  wireAccountSummary `json:"accountSummary"`
}

type wireContractState struct {
	ContractName string `json:"contractName"`
	Underlying   string `json:"underlying"`
	Strike       string `json:"strike"`
	OptionType   string `json:"optionType"`
	ExpiryDate   string `json:"expiryDate"`
	MarkPrice    string `json:"markPrice"`
	BidPrice     string `json:"bidPrice"`
	AskPrice     string `json:"askPrice"`
	BidSize      string `json:"bidSize"`
	AskSize      string `json:"askSize"`
	MarkIV       string `json:"markIV"`
	BidIV        string `json:"bidIV"`
	AskIV        string `json:"askIV"`
	Forward      string `json:"forward"`
	Delta        string `json:"delta"`
	Gamma        string `json:"gamma"`
	Vega         string `json:"vega"`
	Theta        string `json:"theta"`
	Rho          string `json:"rho"`
	OpenInterest string `json:"openInterest"`
	Volume24h    string `json:"volume24h"`
	LotSize      string `json:"lotSize"`
}

func (w wireContractState) toContractState() ContractState {
	return ContractState{
		ContractName: w.ContractName,
		Underlying:   w.Underlying,
		Strike:       w.Strike,
		OptionType:   OptionType(w.OptionType),
		ExpiryDate:   w.ExpiryDate,
		MarkPrice:    w.MarkPrice,
		BidPrice:     w.BidPrice,
		AskPrice:     w.AskPrice,
		BidSize:      w.BidSize,
		AskSize:      w.AskSize,
		MarkIV:       w.MarkIV,
		BidIV:        w.BidIV,
		AskIV:        w.AskIV,
		Forward:      w.Forward,
		Delta:        w.Delta,
		Gamma:        w.Gamma,
		Vega:         w.Vega,
		Theta:        w.Theta,
		Rho:          w.Rho,
		OpenInterest: w.OpenInterest,
		Volume24h:    w.Volume24h,
		LotSize:      w.LotSize,
	}
}

type wireExpectedMove struct {
	ExpiryDate        string `json:"expiryDate"`
	Underlying        string `json:"underlying"`
	AtmIV             string `json:"atmIV"`
	ExpectedPriceMove string `json:"expectedPriceMove"`
	IndexPrice        string `json:"indexPrice"`
}

func (w wireExpectedMove) toExpectedMove() ExpectedMove {
	return ExpectedMove{
		ExpiryDate:        w.ExpiryDate,
		Underlying:        w.Underlying,
		AtmIV:             w.AtmIV,
		ExpectedPriceMove: w.ExpectedPriceMove,
		IndexPrice:        w.IndexPrice,
	}
}

type wireContractUpdate struct {
	ContractName string `json:"contractName"`
	Underlying   string `json:"underlying"`
	Strike       string `json:"strike"`
	OptionType   string `json:"optionType"`
	ExpiryDate   string `json:"expiryDate"`
	MarkPrice    string `json:"markPrice"`
	MarkIV       string `json:"markIV"`
	Forward      string `json:"forward"`
	Delta        string `json:"delta"`
	Gamma        string `json:"gamma"`
	Vega         string `json:"vega"`
	Theta        string `json:"theta"`
	Rho          string `json:"rho"`
	LotSize      string `json:"lotSize"`
}

func (w wireContractUpdate) toContractUpdate() ContractUpdate {
	return ContractUpdate{
		ContractName: w.ContractName,
		Underlying:   w.Underlying,
		Strike:       w.Strike,
		OptionType:   OptionType(w.OptionType),
		ExpiryDate:   w.ExpiryDate,
		MarkPrice:    w.MarkPrice,
		MarkIV:       w.MarkIV,
		Forward:      w.Forward,
		Delta:        w.Delta,
		Gamma:        w.Gamma,
		Vega:         w.Vega,
		Theta:        w.Theta,
		Rho:          w.Rho,
		LotSize:      w.LotSize,
	}
}

type wireQuote struct {
	Contract   string `json:"contract"`
	BestBid    string `json:"bestBid"`
	BestBidQty string `json:"bestBidQty"`
	BestBidIV  string `json:"bestBidIV"`
	BestAsk    string `json:"bestAsk"`
	BestAskQty string `json:"bestAskQty"`
	BestAskIV  string `json:"bestAskIV"`
	Ts         int64  `json:"ts"`
}

func (w wireQuote) toQuote() Quote {
	return Quote{
		Contract:   w.Contract,
		BestBid:    w.BestBid,
		BestBidQty: w.BestBidQty,
		BestBidIV:  w.BestBidIV,
		BestAsk:    w.BestAsk,
		BestAskQty: w.BestAskQty,
		BestAskIV:  w.BestAskIV,
		Ts:         w.Ts,
	}
}

type wireOrderBook struct {
	Contract string          `json:"contract"`
	Orders   []wireBookLevel `json:"orders"`
	BestBid  *wireBestQuote  `json:"bestBid"`
	BestAsk  *wireBestQuote  `json:"bestAsk"`
	Ts       int64           `json:"ts"`
}

func (w wireOrderBook) toOrderBook() OrderBook {
	return OrderBook{
		Contract: w.Contract,
		Orders:   convert(w.Orders, wireBookLevel.toLevel),
		BestBid:  w.BestBid.toBestQuote(),
		BestAsk:  w.BestAsk.toBestQuote(),
		Ts:       w.Ts,
	}
}

type wireBookLevel struct {
	OrderType string `json:"orderType"`
	Price     string `json:"price"`
	Quantity  string `json:"quantity"`
	IV        string `json:"iv"`
}

func (w wireBookLevel) toLevel() OrderBookLevel {
	return OrderBookLevel{OrderType: OrderType(w.OrderType), Price: w.Price, Quantity: w.Quantity, IV: w.IV}
}

type wireBestQuote struct {
	Price string `json:"price"`
	Size  string `json:"size"`
	IV    string `json:"iv"`
}

func (w *wireBestQuote) toBestQuote() BestQuote {
	if w == nil {
		return BestQuote{}
	}
	return BestQuote{Price: w.Price, Size: w.Size, IV: w.IV}
}

type wirePublicTrade struct {
	ExecutionType string `json:"executionType"`
	MatchID       string `json:"matchId"`
	Price         string `json:"price"`
	Quantity      string `json:"quantity"`
	Side          string `json:"side"`
	UpdatedAt     int64  `json:"updatedAt"`
}

func (w wirePublicTrade) toPublicTrade() PublicTrade {
	return PublicTrade{
		ExecutionType: ExecutionType(w.ExecutionType),
		MatchID:       w.MatchID,
		Price:         w.Price,
		Quantity:      w.Quantity,
		Side:          OrderType(w.Side),
		UpdatedAt:     w.UpdatedAt,
	}
}

type wirePosition struct {
	Contract        string `json:"contract"`
	Side            string `json:"side"`
	OptionType      string `json:"optionType"`
	Qty             string `json:"qty"`
	Value           string `json:"value"`
	Fees            string `json:"fees"`
	EntryPrice      string `json:"entryPrice"`
	MarkPrice       string `json:"markPrice"`
	MarkIV          string `json:"markIV"`
	UnderlyingPrice string `json:"underlyingPrice"`
	BreakEvenPrice  string `json:"breakEvenPrice"`
	InitialMargin   string `json:"initialMargin"`
	RealizedPnl     string `json:"realizedPnl"`
	UnrealizedPnl   string `json:"unrealizedPnl"`
	NotionalValue   string `json:"notionalValue"`
	UpdatedAt       int64  `json:"updatedAt"`
}

func (w wirePosition) toPosition() Position {
	return Position{
		Contract:        w.Contract,
		Side:            PositionSide(w.Side),
		OptionType:      OptionType(w.OptionType),
		Qty:             w.Qty,
		Value:           w.Value,
		EntryPrice:      w.EntryPrice,
		MarkPrice:       w.MarkPrice,
		MarkIV:          w.MarkIV,
		UnderlyingPrice: w.UnderlyingPrice,
		BreakEvenPrice:  w.BreakEvenPrice,
		InitialMargin:   w.InitialMargin,
		RealizedPnL:     w.RealizedPnl,
		UnrealizedPnL:   w.UnrealizedPnl,
		NotionalValue:   w.NotionalValue,
		Fees:            w.Fees,
		UpdatedAt:       w.UpdatedAt,
	}
}

// wireOpenOrder is an open order; Kind is set only in a delta.
type wireOpenOrder struct {
	OrderID        string `json:"orderId"`
	Contract       string `json:"contract"`
	Side           string `json:"side"`
	Type           string `json:"type"`
	InitiatorType  string `json:"initiatorType"`
	Price          string `json:"price"`
	Quantity       string `json:"quantity"`
	FilledQuantity string `json:"filledQuantity"`
	AvgFillPrice   string `json:"avgFillPrice"`
	Leverage       string `json:"leverage"`
	ReduceOnly     bool   `json:"reduceOnly"`
	PostOnly       bool   `json:"postOnly"`
	CreatedAt      int64  `json:"createdAt"`
	UpdatedAt      int64  `json:"updatedAt"`
	Kind           string `json:"kind"`
}

func (w wireOpenOrder) toOpenOrder() OpenOrder {
	return OpenOrder{
		OrderID:        w.OrderID,
		Contract:       w.Contract,
		Side:           OrderType(w.Side),
		Type:           TradeType(w.Type),
		Price:          w.Price,
		Quantity:       w.Quantity,
		FilledQuantity: w.FilledQuantity,
		AvgFillPrice:   w.AvgFillPrice,
		ReduceOnly:     w.ReduceOnly,
		PostOnly:       w.PostOnly,
		CreatedAt:      w.CreatedAt,
		UpdatedAt:      w.UpdatedAt,
	}
}

func (w wireOpenOrder) toChange() OpenOrderChange {
	return OpenOrderChange{Kind: OpenOrderKind(w.Kind), Order: w.toOpenOrder()}
}

type wireRecentOrder struct {
	OrderID        string `json:"orderId"`
	Contract       string `json:"contract"`
	Side           string `json:"side"`
	Type           string `json:"type"`
	FillType       string `json:"fillType"`
	Price          string `json:"price"`
	Quantity       string `json:"quantity"`
	FilledQuantity string `json:"filledQuantity"`
	AvgFillPrice   string `json:"avgFillPrice"`
	Leverage       string `json:"leverage"`
	CreatedAt      int64  `json:"createdAt"`
	UpdatedAt      int64  `json:"updatedAt"`
}

func (w wireRecentOrder) toRecentOrder() RecentOrder {
	return RecentOrder{
		OrderID:        w.OrderID,
		Contract:       w.Contract,
		Side:           OrderType(w.Side),
		Type:           TradeType(w.Type),
		FillType:       FillType(w.FillType),
		Price:          w.Price,
		Quantity:       w.Quantity,
		FilledQuantity: w.FilledQuantity,
		AvgFillPrice:   w.AvgFillPrice,
		CreatedAt:      w.CreatedAt,
		UpdatedAt:      w.UpdatedAt,
	}
}

type wireSettlement struct {
	Contract        string `json:"contract"`
	Side            string `json:"side"`
	OptionType      string `json:"optionType"`
	Qty             string `json:"qty"`
	Value           string `json:"value"`
	EntryPrice      string `json:"entryPrice"`
	MarkPrice       string `json:"markPrice"`
	UnderlyingPrice string `json:"underlyingPrice"`
	BreakEvenPrice  string `json:"breakEvenPrice"`
	SettlementPrice string `json:"settlementPrice"`
	Payoff          string `json:"payoff"`
	DeliveryFee     string `json:"deliveryFee"`
	RealizedPnl     string `json:"realizedPnl"`
	SettledAt       int64  `json:"settledAt"`
}

func (w wireSettlement) toSettlement() Settlement {
	return Settlement{
		Contract:        w.Contract,
		Side:            PositionSide(w.Side),
		OptionType:      OptionType(w.OptionType),
		Qty:             w.Qty,
		Value:           w.Value,
		EntryPrice:      w.EntryPrice,
		MarkPrice:       w.MarkPrice,
		UnderlyingPrice: w.UnderlyingPrice,
		BreakEvenPrice:  w.BreakEvenPrice,
		SettlementPrice: w.SettlementPrice,
		Payoff:          w.Payoff,
		DeliveryFee:     w.DeliveryFee,
		RealizedPnL:     w.RealizedPnl,
		SettledAt:       w.SettledAt,
	}
}

type wireAccountSummary struct {
	AccountBalance         string `json:"accountBalance"`
	AvailableBalance       string `json:"availableBalance"`
	TotalEquity            string `json:"totalEquity"`
	MarginUsed             string `json:"marginUsed"`
	InitialMarginRatio     string `json:"initialMarginRatio"`
	MaintenanceMargin      string `json:"maintenanceMargin"`
	MaintenanceMarginRatio string `json:"maintenanceMarginRatio"`
	WithdrawableBalance    string `json:"withdrawableBalance"`
	UnrealizedPnl          string `json:"unrealizedPnl"`
	Pnl                    string `json:"pnl"`
	Fees                   string `json:"fees"`
	Volume                 string `json:"volume"`
}

func (w wireAccountSummary) toAccountSummary() *AccountSummary {
	return &AccountSummary{
		AccountBalance:         w.AccountBalance,
		AvailableBalance:       w.AvailableBalance,
		WithdrawableBalance:    w.WithdrawableBalance,
		TotalEquity:            w.TotalEquity,
		MarginUsed:             w.MarginUsed,
		MaintenanceMargin:      w.MaintenanceMargin,
		InitialMarginRatio:     w.InitialMarginRatio,
		MaintenanceMarginRatio: w.MaintenanceMarginRatio,
		PnL:                    w.Pnl,
		UnrealizedPnL:          w.UnrealizedPnl,
		Fees:                   w.Fees,
		Volume:                 w.Volume,
	}
}

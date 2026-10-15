package perps

import (
	"github.com/rails-xyz/exchange-go-sdk/internal/wire/account"
	"github.com/rails-xyz/exchange-go-sdk/internal/wire/market"
	"github.com/rails-xyz/exchange-go-sdk/internal/wire/order"
)

// The generated order maps here by plain assignment — never a numeric conversion, which is what
// would hide a spec regression from int64 back to float32.
func restOrderToOrder(w order.Order) Order {
	return Order{
		Market:          w.Market,
		OrderID:         w.OrderId,
		OrderType:       OrderType(w.OrderType),
		TradeType:       TradeType(w.TradeType),
		Price:           w.Price,
		Quantity:        w.Quantity,
		FilledQuantity:  w.FilledQuantity,
		Leverage:        w.Leverage,
		CreatedAt:       w.CreatedAt,
		UpdatedAt:       w.UpdatedAt,
		OrderStatus:     OrderStatus(w.OrderStatus),
		ClientRequestID: deref(w.ClientRequestId),
		ReduceOnly:      w.ReduceOnly,
		PostOnly:        w.PostOnly,
		AvgFillPrice:    deref(w.AvgFillPrice),
	}
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// The generated response embeds the position as an anonymous type, so the whole response is
// mapped rather than one element.
func positionsResponseToPositions(w account.PositionsResponse) []Position {
	positions := make([]Position, 0, len(w.Positions))
	for _, p := range w.Positions {
		positions = append(positions, Position{
			Market:           p.Market,
			Value:            p.Value,
			Quantity:         p.Quantity,
			Funding:          p.Funding,
			RealizedPnL:      p.RealizedPnl,
			UnrealizedPnL:    p.UnrealizedPnl,
			LiquidationPrice: p.LiquidationPrice,
			Type:             PositionType(p.Type),
			NotionalValue:    p.NotionalValue,
			MarginMode:       MarginMode(p.MarginMode),
		})
	}
	return positions
}

func balancesResponseToBalances(w account.BalancesResponse) *Balances {
	b := w.Balances
	markets := make(map[string]MarketBalance, len(b.MarketBalancesMap))
	for name, m := range b.MarketBalancesMap {
		markets[name] = MarketBalance{
			Position:          m.Position,
			PositionQuantity:  m.PositionQuantity,
			PositionPnL:       m.PositionPnl,
			PositionFunding:   m.PositionFunding,
			PositionFees:      m.PositionFees,
			UnrealizedPnL:     m.UnrealizedPnl,
			OpenBuy:           m.OpenBuy,
			OpenBuyQty:        m.OpenBuyQty,
			OpenSell:          m.OpenSell,
			OpenSellQty:       m.OpenSellQty,
			Margin:            m.Margin,
			MaintenanceMargin: m.MaintenanceMargin,
			Leverage:          m.Leverage,
			MarginMode:        MarginMode(m.MarginMode),
			LiquidationPrice:  m.LiquidationPrice,
			LiquidationFees:   m.LiquidationFees,
			MarkPrice:         m.MarkPrice,
			PnL:               m.Pnl,
			Funding:           m.Funding,
			Fees:              m.Fees,
			Volume:            m.Volume,
		}
	}
	return &Balances{
		TotalBalance:        b.TotalBalance,
		CrossMarginBalance:  b.CrossMarginBalance,
		AvailableBalance:    b.AvailableBalance,
		WithdrawableBalance: b.WithdrawableBalance,
		Deposit:             b.Deposit,
		Withdraw:            b.Withdraw,
		WithdrawFees:        b.WithdrawFees,
		TransferIn:          deref(b.TransferIn),
		TransferOut:         deref(b.TransferOut),
		PromoCredit:         deref(b.PromoCredit),
		Leverage:            b.Leverage,
		PnL:                 b.Pnl,
		Funding:             b.Funding,
		Fees:                b.Fees,
		LiqFees:             deref(b.LiqFees),
		Volume:              b.Volume,
		Markets:             markets,
	}
}

func depositsResponseToPage(w account.DepositsResponse) *Page[Deposit] {
	page := &Page[Deposit]{Items: make([]Deposit, 0, len(w.Deposits)), Next: deref(w.Last)}
	for _, d := range w.Deposits {
		page.Items = append(page.Items, Deposit{
			Market:       d.Market,
			Value:        d.Value,
			TxCreated:    d.TxCreated,
			RefChainTxID: d.RefChainTxId,
		})
	}
	return page
}

func withdrawalsResponseToPage(w account.WithdrawalsResponse) *Page[Withdrawal] {
	page := &Page[Withdrawal]{Items: make([]Withdrawal, 0, len(w.Withdrawals)), Next: deref(w.Last)}
	for _, d := range w.Withdrawals {
		page.Items = append(page.Items, Withdrawal{
			Market:    d.Market,
			Value:     d.Value,
			Fees:      d.Fees,
			TxCreated: d.TxCreated,
			TxHash:    d.TxHash,
		})
	}
	return page
}

func fundingsResponseToPage(w account.FundingsResponse) *Page[Funding] {
	page := &Page[Funding]{Items: make([]Funding, 0, len(w.Fundings)), Next: deref(w.Last)}
	for _, f := range w.Fundings {
		page.Items = append(page.Items, Funding{
			Market:       f.Market,
			Value:        f.Value,
			Rate:         f.Rate,
			IndexPrice:   f.IndexPrice,
			PositionSize: f.PositionSize,
			TxCreated:    f.TxCreated,
		})
	}
	return page
}

func completedOrdersResponseToPage(w order.CompletedOrdersResponse) *Page[CompletedOrder] {
	page := &Page[CompletedOrder]{Items: make([]CompletedOrder, 0, len(w.CompletedOrders)), Next: deref(w.Last)}
	for _, o := range w.CompletedOrders {
		page.Items = append(page.Items, CompletedOrder{
			Market:          o.Market,
			MatchID:         o.MatchId,
			OrderID:         o.OrderId,
			OrderType:       OrderType(o.OrderType),
			TradeType:       TradeType(o.TradeType),
			Price:           o.Price,
			Quantity:        o.Quantity,
			ExecutionType:   ExecutionType(o.ExecutionType),
			FillType:        FillType(o.FillType),
			Fees:            o.Fees,
			FilledAt:        o.FilledAt,
			TransactionHash: o.TransactionHash,
			TriggerType:     TriggerType(o.TriggerType),
			TriggerPrice:    o.TriggerPrice,
		})
	}
	return page
}

func restMarketDataPointToPoint(w market.MarketDataPoint) MarketDataPoint {
	return MarketDataPoint{
		Market:    w.Market,
		Type:      MarketDataType(w.Type),
		Index:     string(w.Index),
		Value:     w.Value,
		UpdatedAt: w.UpdatedAt,
	}
}

func restOrderBookToSnapshot(w market.OrderBookSnapshot) *OrderBookSnapshot {
	book := &OrderBookSnapshot{Orders: make([]OrderBookLevel, 0, len(w.Orders)), Ts: w.Ts}
	for _, level := range w.Orders {
		book.Orders = append(book.Orders, OrderBookLevel{
			OrderType: OrderType(level.OrderType),
			Price:     level.Price,
			Quantity:  level.Quantity,
		})
	}
	return book
}

func restProductSpecificationToSpecification(w market.ProductSpecification) ProductSpecification {
	return ProductSpecification{
		Market:               w.Market,
		TickSize:             w.TickSize,
		LotSize:              w.LotSize,
		FundingIntervalHours: w.FundingIntervalHours,
		MinOrderSizeUSD:      w.MinOrderSizeUSD,
		MaxOrderSizeUSD:      w.MaxOrderSizeUSD,
		ContractType:         w.ContractType,
	}
}

func restContractSpecificationToSpecification(w market.ContractSpecification) ContractSpecification {
	return ContractSpecification{
		ContractName:         w.ContractName,
		UnderlyingPrice:      w.UnderlyingPrice,
		PriceIndex:           w.PriceIndex,
		IndexUpdateFrequency: w.IndexUpdateFrequency,
		ContractType:         w.ContractType,
		MarginType:           w.MarginType,
		TradingHours:         w.TradingHours,
		FundingTimes:         w.FundingTimes,
		FundingRateCap:       w.FundingRateCap,
		TickSize:             w.TickSize,
		LotSize:              w.LotSize,
		MinOrderNotional:     w.MinOrderNotional,
		PriceUnit:            w.PriceUnit,
		QuantityUnit:         w.QuantityUnit,
	}
}

package options

import (
	"github.com/rails-xyz/exchange-go-sdk/internal/wire/optionsaccount"
	"github.com/rails-xyz/exchange-go-sdk/internal/wire/optionsmatching"
)

// Generated models map by plain assignment, never a numeric conversion, which would hide a spec
// regression to float32. Optional spec fields arrive as pointers; absent reads as zero.

func deref[T any](p *T) T {
	if p == nil {
		var zero T
		return zero
	}
	return *p
}

func derefSlice[T any](p *[]T) []T {
	if p == nil {
		return nil
	}
	return *p
}

func expirationsResponseToExpirations(w optionsaccount.Expirations) []Expiration {
	wire := derefSlice(w.Expirations)
	out := make([]Expiration, 0, len(wire))
	for _, e := range wire {
		out = append(out, Expiration{
			ExpiryDate:        deref(e.ExpiryDate),
			ExpiryType:        ExpiryType(deref(e.ExpiryType)),
			ContractCount:     deref(e.ContractCount),
			AtmIV:             deref(e.AtmIV),
			ExpectedPriceMove: deref(e.ExpectedPriceMove),
			IndexPrice:        deref(e.IndexPrice),
		})
	}
	return out
}

func contractsResponseToContracts(w optionsaccount.Contracts) []Contract {
	wire := derefSlice(w.Contracts)
	out := make([]Contract, 0, len(wire))
	for _, c := range wire {
		out = append(out, Contract{
			ContractName:   deref(c.ContractName),
			Underlying:     deref(c.Underlying),
			Strike:         deref(c.Strike),
			OptionType:     OptionType(deref(c.OptionType)),
			ExpiryDate:     deref(c.ExpiryDate),
			ExpiryType:     ExpiryType(deref(c.ExpiryType)),
			MarkPrice:      deref(c.MarkPrice),
			BidPrice:       deref(c.BidPrice),
			AskPrice:       deref(c.AskPrice),
			MarkIV:         deref(c.MarkIV),
			BidIV:          deref(c.BidIV),
			AskIV:          deref(c.AskIV),
			Forward:        deref(c.Forward),
			TickSize:       deref(c.TickSize),
			ContractSize:   deref(c.ContractSize),
			LotSize:        deref(c.LotSize),
			SettlementType: deref(c.SettlementType),
			Delta:          deref(c.Delta),
			Gamma:          deref(c.Gamma),
			Vega:           deref(c.Vega),
			Theta:          deref(c.Theta),
			Rho:            deref(c.Rho),
		})
	}
	return out
}

func accountSummaryResponseToSummary(w optionsaccount.AccountSummary) *AccountSummary {
	return &AccountSummary{
		AccountBalance:         deref(w.AccountBalance),
		AvailableBalance:       deref(w.AvailableBalance),
		WithdrawableBalance:    deref(w.WithdrawableBalance),
		TotalEquity:            deref(w.TotalEquity),
		MarginUsed:             deref(w.MarginUsed),
		MaintenanceMargin:      deref(w.MaintenanceMargin),
		InitialMarginRatio:     deref(w.InitialMarginRatio),
		MaintenanceMarginRatio: deref(w.MaintenanceMarginRatio),
		PnL:                    deref(w.Pnl),
		UnrealizedPnL:          deref(w.UnrealizedPnl),
		Fees:                   deref(w.Fees),
		Volume:                 deref(w.Volume),
	}
}

func positionsResponseToPositions(w optionsaccount.Positions) []Position {
	wire := derefSlice(w.Positions)
	out := make([]Position, 0, len(wire))
	for _, p := range wire {
		out = append(out, Position{
			Contract:        deref(p.Contract),
			Side:            PositionSide(deref(p.Side)),
			OptionType:      OptionType(deref(p.OptionType)),
			Qty:             deref(p.Qty),
			Value:           deref(p.Value),
			EntryPrice:      deref(p.EntryPrice),
			MarkPrice:       deref(p.MarkPrice),
			MarkIV:          deref(p.MarkIV),
			UnderlyingPrice: deref(p.UnderlyingPrice),
			LiqPrice:        deref(p.LiqPrice),
			BreakEvenPrice:  deref(p.BreakEvenPrice),
			InitialMargin:   deref(p.InitialMargin),
			RealizedPnL:     deref(p.RealizedPnl),
			UnrealizedPnL:   deref(p.UnrealizedPnl),
			NotionalValue:   deref(p.NotionalValue),
		})
	}
	return out
}

func recentOrdersResponseToPage(w optionsaccount.RecentOrders) *Page[RecentOrder] {
	wire := derefSlice(w.Orders)
	page := &Page[RecentOrder]{Items: make([]RecentOrder, 0, len(wire)), Next: deref(w.NextPageToken)}
	for _, o := range wire {
		page.Items = append(page.Items, RecentOrder{
			OrderID:        deref(o.OrderId),
			Contract:       deref(o.Contract),
			Side:           OrderType(deref(o.Side)),
			Type:           TradeType(deref(o.Type)),
			FillType:       FillType(deref(o.FillType)),
			Price:          deref(o.Price),
			Quantity:       deref(o.Quantity),
			FilledQuantity: deref(o.FilledQuantity),
			AvgFillPrice:   deref(o.AvgFillPrice),
			Fees:           deref(o.Fees),
			CreatedAt:      deref(o.CreatedAt),
			UpdatedAt:      deref(o.UpdatedAt),
		})
	}
	return page
}

func depositsResponseToPage(w optionsaccount.Deposits) *Page[Deposit] {
	wire := derefSlice(w.Deposits)
	page := &Page[Deposit]{Items: make([]Deposit, 0, len(wire)), Next: deref(w.NextPageToken)}
	for _, d := range wire {
		page.Items = append(page.Items, Deposit{
			Market:       deref(d.Market),
			Value:        deref(d.Value),
			RefChainTxID: deref(d.RefChainTxId),
			CreatedAt:    deref(d.CreatedAt),
		})
	}
	return page
}

func withdrawalsResponseToPage(w optionsaccount.Withdrawals) *Page[Withdrawal] {
	wire := derefSlice(w.Withdrawals)
	page := &Page[Withdrawal]{Items: make([]Withdrawal, 0, len(wire)), Next: deref(w.NextPageToken)}
	for _, d := range wire {
		page.Items = append(page.Items, Withdrawal{
			Market:    deref(d.Market),
			Value:     deref(d.Value),
			Fees:      deref(d.Fees),
			TxHash:    deref(d.TxHash),
			CreatedAt: deref(d.CreatedAt),
		})
	}
	return page
}

func openOrdersResponseToPage(w optionsmatching.OpenOrders) *Page[OpenOrder] {
	page := &Page[OpenOrder]{Items: make([]OpenOrder, 0, len(w.Orders)), Next: deref(w.NextPageToken)}
	for _, o := range w.Orders {
		page.Items = append(page.Items, OpenOrder{
			OrderID:        o.OrderId,
			Contract:       o.Contract,
			Side:           OrderType(o.Side),
			Type:           TradeType(o.Type),
			Price:          o.Price,
			Quantity:       o.Quantity,
			FilledQuantity: o.FilledQuantity,
			AvgFillPrice:   o.AvgFillPrice,
			ReduceOnly:     o.ReduceOnly,
			PostOnly:       o.PostOnly,
			CreatedAt:      o.CreatedAt,
			UpdatedAt:      o.UpdatedAt,
		})
	}
	return page
}

func orderStatusResponseToStatus(w optionsmatching.OrderStatus) *OrderStatus {
	return &OrderStatus{
		OrderID:           deref(w.OrderId),
		ClientRequestID:   deref(w.ClientRequestId),
		Contract:          deref(w.Contract),
		Status:            Status(w.Status),
		Action:            Action(deref(w.Action)),
		Body:              deref(w.Body),
		Reason:            deref(w.Reason),
		Quantity:          deref(w.Quantity),
		FilledQuantity:    deref(w.FilledQuantity),
		RemainingQuantity: deref(w.RemainingQuantity),
		UpdatedAt:         w.UpdatedAt,
	}
}

// The create and modify responses have the cancel response's shape, and convert to it.
func queuedResponseToQueued(w optionsmatching.CancelOrderQueued) Queued {
	return Queued{
		OrderID:         w.OrderId,
		ClientRequestID: deref(w.ClientRequestId),
		Contract:        w.Contract,
		MessageID:       deref(w.MessageId),
	}
}

func cancelAllResponseToQueued(w optionsmatching.CancelAllOrdersQueued) *QueuedCancelAll {
	out := &QueuedCancelAll{
		ClientRequestID: w.ClientRequestId,
		Contract:        deref(w.Contract),
		Count:           w.Count,
		Cancels:         make([]Queued, 0, len(w.Cancels)),
	}
	for _, c := range w.Cancels {
		out.Cancels = append(out.Cancels, queuedResponseToQueued(c))
	}
	for _, u := range derefSlice(w.Unqueued) {
		out.Unqueued = append(out.Unqueued, UnqueuedCancel{OrderID: u.OrderId, Contract: u.Contract})
	}
	return out
}

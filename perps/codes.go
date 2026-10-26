package perps

import "github.com/rails-xyz/exchange-go-sdk/rails"

// The perpetuals route codes, PRP, one block per route: 01xx createOrder, 02xx cancelOrder,
// 03xx modifyOrder, 06xx the pushed rejectOrder, 07xx updateLeverage, 08xx updateMargin, 09xx
// updateMarginMode. Every route can also answer with a shared rails.Code*.
const (
	// CodeCreateOrderError is a create failure none of the other 01xx codes names.
	CodeCreateOrderError rails.Code = "PRP-0100"
	// CodeCreateOrderInsufficientBalance is available balance not covering the order's margin.
	CodeCreateOrderInsufficientBalance rails.Code = "PRP-0101"
	// CodeCreateOrderInvalidTrade is a trading rule none of the other 01xx codes names.
	CodeCreateOrderInvalidTrade rails.Code = "PRP-0102"
	// CodeCreateOrderReceiveWindow is an order that reached the engine after the receive window.
	CodeCreateOrderReceiveWindow rails.Code = "PRP-0103"
	// CodeCreateOrderMaxOpenOrders is the per-market open-orders cap.
	CodeCreateOrderMaxOpenOrders rails.Code = "PRP-0104"
	// CodeCreateOrderBanned is an order creation ban in force; RetryAfter says for how long.
	// A modifyOrder is refused with it too.
	CodeCreateOrderBanned rails.Code = "PRP-0105"
	// CodeCreateOrderReduceOnly is a reduce-only rule: no open position, or the quantity
	// exceeds it.
	CodeCreateOrderReduceOnly rails.Code = "PRP-0106"
	// CodeCreateOrderSelfTrade is an order that would trade against one of your own.
	CodeCreateOrderSelfTrade rails.Code = "PRP-0107"
	// CodeCreateOrderPriceBand is a price too far from the mark or index price.
	CodeCreateOrderPriceBand rails.Code = "PRP-0108"
	// CodeCreateOrderConflict is an order whose state moved on while the command was in
	// flight. Look the order up before deciding what to do.
	CodeCreateOrderConflict rails.Code = "PRP-0109"
	// CodeCreateOrderMarketHalted is the market's trading state not accepting the order, for
	// example while the market is reduce-only or disabled.
	CodeCreateOrderMarketHalted rails.Code = "PRP-0110"

	// CodeCancelOrderError is a cancel failure none of the other 02xx codes names.
	CodeCancelOrderError rails.Code = "PRP-0200"
	// CodeCancelOrderConflictError is a cancel that lost a race and was not applied. Send it
	// again as is.
	CodeCancelOrderConflictError rails.Code = "PRP-0201"
	// CodeCancelOrderAlreadyCancelling is another cancel of the same order already in flight;
	// its answer follows. Do not resend.
	CodeCancelOrderAlreadyCancelling rails.Code = "PRP-0202"
	// CodeCancelOrderNotCancellable is an order already filled, cancelled or rejected, or a
	// liquidation order.
	CodeCancelOrderNotCancellable rails.Code = "PRP-0203"
	// CodeCancelOrderMarketHalted is a cancel in a disabled market.
	CodeCancelOrderMarketHalted rails.Code = "PRP-0204"

	// CodeModifyOrderError is a modify failure none of the other 03xx codes names.
	CodeModifyOrderError rails.Code = "PRP-0300"
	// CodeModifyOrderInsufficientBalance is available balance not covering the modified order.
	CodeModifyOrderInsufficientBalance rails.Code = "PRP-0301"
	// CodeModifyOrderInvalidTrade is a trading rule none of the other 03xx codes names.
	CodeModifyOrderInvalidTrade rails.Code = "PRP-0302"
	// CodeModifyOrderNotFoundError is no order with that id for this account.
	CodeModifyOrderNotFoundError rails.Code = "PRP-0303"
	// CodeModifyOrderConflictError is a modify that lost a race and was not applied.
	CodeModifyOrderConflictError rails.Code = "PRP-0304"
	// CodeModifyOrderNotActive is a modify of an order that is not active.
	CodeModifyOrderNotActive rails.Code = "PRP-0305"
	// CodeModifyOrderReceiveWindow is a replacement order that reached the engine after the
	// receive window.
	CodeModifyOrderReceiveWindow rails.Code = "PRP-0306"
	// CodeModifyOrderMaxOpenOrders is a replacement order over the per-market open-orders cap.
	CodeModifyOrderMaxOpenOrders rails.Code = "PRP-0307"
	// CodeModifyOrderReduceOnly is a reduce-only rule on the replacement order.
	CodeModifyOrderReduceOnly rails.Code = "PRP-0308"
	// CodeModifyOrderWouldMatch is a modified post-only price that would take liquidity.
	CodeModifyOrderWouldMatch rails.Code = "PRP-0309"
	// CodePartialFillOrderError is an internal failure recording a partial fill.
	CodePartialFillOrderError rails.Code = "PRP-0310"
	// CodeCompleteFillOrderError is an internal failure recording a complete fill.
	CodeCompleteFillOrderError rails.Code = "PRP-0311"
	// CodeModifyOrderStateChanged is the original order filled or cancelled during the modify.
	CodeModifyOrderStateChanged rails.Code = "PRP-0312"
	// CodeModifyOrderSelfTrade is a replacement order that would trade against one of your own.
	CodeModifyOrderSelfTrade rails.Code = "PRP-0313"
	// CodeModifyOrderPriceBand is a modified price too far from the mark or index price.
	CodeModifyOrderPriceBand rails.Code = "PRP-0314"
	// CodeModifyOrderMarketHalted is the market's trading state not accepting modifies from
	// this account.
	CodeModifyOrderMarketHalted rails.Code = "PRP-0315"

	// CodeOrderRejected is a pushed rejection none of the other 06xx codes names.
	CodeOrderRejected rails.Code = "PRP-0600"
	// CodeOrderRejectedPostOnly is a post-only order that would have matched immediately.
	CodeOrderRejectedPostOnly rails.Code = "PRP-0601"

	// CodeUpdateLeverageError is a leverage update failure none of the other 07xx codes names.
	CodeUpdateLeverageError rails.Code = "PRP-0700"
	// CodeUpdateLeverageAlreadySet is the market already using the requested leverage.
	CodeUpdateLeverageAlreadySet rails.Code = "PRP-0701"
	// CodeUpdateLeverageNotAllowed is open positions or orders forbidding the change.
	CodeUpdateLeverageNotAllowed rails.Code = "PRP-0702"

	// CodeUpdateMarginError is a margin update failure none of the other 08xx codes names.
	CodeUpdateMarginError rails.Code = "PRP-0800"
	// CodeUpdateMarginNoPosition is no position in the market.
	CodeUpdateMarginNoPosition rails.Code = "PRP-0801"
	// CodeUpdateMarginNotIsolated is a position not in isolated margin mode.
	CodeUpdateMarginNotIsolated rails.Code = "PRP-0802"
	// CodeUpdateMarginExceedsAvailable is an increase over available balance.
	CodeUpdateMarginExceedsAvailable rails.Code = "PRP-0803"
	// CodeUpdateMarginBelowMinimum is a decrease below the minimum required margin.
	CodeUpdateMarginBelowMinimum rails.Code = "PRP-0804"
	// CodeUpdateMarginConflict is the position's margin updated by another request meanwhile.
	// Send it again.
	CodeUpdateMarginConflict rails.Code = "PRP-0805"
	// CodeUpdateMarginMarketHalted is a margin update in a disabled market.
	CodeUpdateMarginMarketHalted rails.Code = "PRP-0806"

	// CodeUpdateMarginModeMarketInUse is open positions or orders forbidding the change.
	CodeUpdateMarginModeMarketInUse rails.Code = "PRP-0901"
)

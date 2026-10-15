package options

import "github.com/rails-xyz/exchange-go-sdk/rails"

// The options order codes, OPT: 00xx any order command, 01xx createOrder, 02xx cancelOrder, 03xx
// modifyOrder. A condition createOrder and modifyOrder share has the same last two digits in
// both blocks. Every command can also answer with a shared rails.Code*.
const (
	// CodeContractNotReady is a contract the exchange cannot trade yet. Try again shortly.
	CodeContractNotReady rails.Code = "OPT-0001"
	// CodeInactiveContract is a contract that is unknown, expired or not tradable.
	CodeInactiveContract rails.Code = "OPT-0002"
	// CodeOrderRejected is a rejection none of the other codes names.
	CodeOrderRejected rails.Code = "OPT-0003"
	// CodeOrderNotFound is an order that is gone: never accepted, filled or cancelled.
	CodeOrderNotFound rails.Code = "OPT-0004"
	// CodeReceiveWindowExceeded is a request that reached the exchange too late. Nothing was
	// applied; send it again under a new clientRequestId.
	CodeReceiveWindowExceeded rails.Code = "OPT-0005"
	// CodeRateLimited is an operation class whose budget is spent. The rejection carries no retry
	// hint: back off that class.
	CodeRateLimited rails.Code = "OPT-0006"
	// CodeTooManyInflightOrders is too many requests waiting to be applied. Wait for their
	// outcomes before sending more.
	CodeTooManyInflightOrders rails.Code = "OPT-0007"
	// CodeEngineOverloaded is the exchange shedding load. Back off.
	CodeEngineOverloaded rails.Code = "OPT-0008"
	// CodeEngineTimeout is a request that timed out before it was applied. Nothing was applied.
	CodeEngineTimeout rails.Code = "OPT-0009"
	// CodeCommitFailed is a change that could not be saved and was rolled back. Nothing was
	// applied.
	CodeCommitFailed rails.Code = "OPT-0010"
	// CodeInternalError is an internal failure whose effect is unknown. Look the order up before
	// sending it again.
	CodeInternalError rails.Code = "OPT-0011"
	// CodeMarkPriceUnavailable is margin that cannot be computed yet. Try again shortly.
	CodeMarkPriceUnavailable rails.Code = "OPT-0012"

	// CodeCreateOrderInsufficientBalance is available balance not covering the order's margin.
	CodeCreateOrderInsufficientBalance rails.Code = "OPT-0101"
	// CodeCreateOrderMarginRatioExceeded is an order that would take short exposure past the
	// account's margin ratio.
	CodeCreateOrderMarginRatioExceeded rails.Code = "OPT-0102"
	// CodeCreateOrderMaxOpenOrders is the per-contract open-orders cap.
	CodeCreateOrderMaxOpenOrders rails.Code = "OPT-0103"
	// CodeCreateOrderSelfTrade is an order that would trade against one of your own.
	CodeCreateOrderSelfTrade rails.Code = "OPT-0104"
	// CodeCreateOrderWouldMatch is a post-only order that would take liquidity.
	CodeCreateOrderWouldMatch rails.Code = "OPT-0105"
	// CodeCreateOrderReduceOnly is a reduce-only order that would increase the position.
	CodeCreateOrderReduceOnly rails.Code = "OPT-0106"
	// CodeCreateOrderMinValue is an order below the minimum contract value.
	CodeCreateOrderMinValue rails.Code = "OPT-0107"
	// CodeCreateOrderDuplicateOrderID is an orderId already resting on the contract.
	CodeCreateOrderDuplicateOrderID rails.Code = "OPT-0108"

	// CodeCancelOrderNotOwned is an order that belongs to another account.
	CodeCancelOrderNotOwned rails.Code = "OPT-0201"

	// CodeModifyOrderInsufficientBalance is available balance not covering the new margin.
	CodeModifyOrderInsufficientBalance rails.Code = "OPT-0301"
	// CodeModifyOrderMarginRatioExceeded is a change that would take short exposure past the
	// account's margin ratio.
	CodeModifyOrderMarginRatioExceeded rails.Code = "OPT-0302"
	// CodeModifyOrderMaxOpenOrders is the per-contract open-orders cap.
	CodeModifyOrderMaxOpenOrders rails.Code = "OPT-0303"
	// CodeModifyOrderSelfTrade is a new price that would trade against one of your own orders.
	CodeModifyOrderSelfTrade rails.Code = "OPT-0304"
	// CodeModifyOrderWouldMatch is a post-only order whose new price would take liquidity.
	CodeModifyOrderWouldMatch rails.Code = "OPT-0305"
	// CodeModifyOrderReduceOnly is a reduce-only order that would increase the position.
	CodeModifyOrderReduceOnly rails.Code = "OPT-0306"
	// CodeModifyOrderMinValue is a new size below the minimum contract value.
	CodeModifyOrderMinValue rails.Code = "OPT-0307"
	// CodeModifyOrderNotOwned is an order that belongs to another account.
	CodeModifyOrderNotOwned rails.Code = "OPT-0308"
	// CodeModifyOrderQuantityBelowFilled is a new quantity below what is already filled.
	CodeModifyOrderQuantityBelowFilled rails.Code = "OPT-0309"
)

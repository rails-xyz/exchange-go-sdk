// Package ws owns a WebSocket connection's lifetime: dialling, reconnecting, keeping the
// socket alive, and correlating replies with the requests that caused them.
//
// It knows nothing about any product's messages. The frame envelope (`{"message","content"}`
// out, `{"resultType","market","data"}` back) and correlation by clientRequestId are all it
// reads; everything above that belongs to the product package.
//
// # Ownership
//
// One connection is one owner goroutine. It alone touches the pending-request map, the session
// state and the active socket; callers reach it only by sending on a channel and waiting for a
// reply. A reader goroutine per socket does nothing but read frames and forward them to the
// owner, which answers the requests they correlate with and hands the rest to [Options.OnFrame].
package ws

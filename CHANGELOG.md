# Changelog

Notable changes to this module. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and versions follow [semantic versioning](https://semver.org/).

## [Unreleased]

### Added

- `options.Contract.LotSize`, the quantity step of a contract. An order quantity must be a
  positive multiple of it; `"1"` means whole contracts only.

## [1.0.0]

The first release.

- `perps.NewClient`, the client for the perpetuals API: orders created, modified and cancelled
  over the WebSocket, isolated margin, and typed events for fills, rejections, trades and the
  order book; HTTP reads for orders, fills, balances, positions, the ledger and public market
  data, plus leverage, margin mode and cancel-all.
- `options.NewClient`, the client for the options API: contracts, account and order reads over
  HTTP with paging and cancel-all; order commands over the WebSocket for the contracts of an
  `options.Scope`, with cancels and reduce-only creates falling back to HTTP while the socket
  is down; streams and snapshots for contracts, quotes, the order book, trades, positions,
  orders, settlements and the account summary.
- Bearer tokens minted from the API key and refreshed before they expire.
- Reconnects with backoff, session resume, subscriptions restored after a reconnect, and
  application-level keepalive.
- Retries for idempotent HTTP reads, honouring `Retry-After`; nothing that changes state is
  sent again unasked.
- Opt-in client-side pacing per class of operation, from the documented budgets
  (`perps.NewPacer`, `options.NewPacer`, `rails.WithPacer`).
- `*rails.Error` for every failure the exchange describes, with `errors.Is` sentinels and one
  `Code` constant per documented condition (`rails.Code*`, `perps.Code*`, `options.Code*`).
- `rails.Hooks` for connect phases, pings, command acknowledgement and answer times, HTTP
  calls and pacing.
- Latency telemetry reported to Rails once a minute, on by default; `rails.WithTelemetry(false)`
  or `RAILS_SDK_TELEMETRY=off` turns it off.
- `rails.Production()` (the default) and `rails.Sandbox()` environments, or a custom one.
- Runnable programs under `examples/` for both products.

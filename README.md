# xmr.pub MCP — read-only Monero tools for AI agents

Hosted endpoint: `https://xmr.pub/mcp` (MCP Streamable HTTP)
Registry entry: `pub.xmr/xmr-pub-mcp` · Source: this repository

A seven-tool, **read-only** Monero toolset served over the Model Context Protocol. The server reads a
Monero mainnet node we operate ourselves (restricted RPC), plus a one-shot *read-only* wallet for the
payment-verification tools. It never takes custody of funds, never signs, never broadcasts a
transaction, and never receives a spend key.

What it deliberately does **not** do is part of the product: no wallet custody, no address generation
on your behalf, no transaction construction, no mining-pool or Stratum surface (xmr.pub operates no
mining pool).

## Tools

| Tool | Tier | Question it answers |
| --- | --- | --- |
| `xmr_node_status` | free | Height, sync state, difficulty and network hashrate, mempool size, database size — what our node sees right now. |
| `xmr_fee_oracle` | free | Fee tiers, quantization, and the dynamic block-weight limits that explain why Monero has no fixed block size. |
| `xmr_mempool_health` | free | Queued transactions, bytes, median fee, oldest wait, fee-rate distribution, key-image (double-spend) footprint. |
| `xmr_verify_tx_proof` | paid | "Did this `txid` really pay my address?" — txid + address + the payer's `tx_key` + your `view_key`, verified by a one-shot read-only wallet. |
| `xmr_verify_payment` | paid | Single or batch (≤100) settlement check with expected amount and tolerance: amount actually received by that address, confirmations, shortfall. |
| `xmr_merchant_report` | paid | 1–30 day inbound flow for one address: per-day counts and totals, unlocked/pending counts, most common amounts. |
| `xmr_mining_profitability` | paid | Given hashrate, power draw and $/kWh: gross and net XMR per day/month, break-even power price, payback days. |

Every tool returns JSON (a text block plus `structuredContent`), so results can be consumed directly
by an agent without screen-scraping.

## Pricing — x402 credit packs, no accounts

The three telemetry tools above are free (rate limited). The four tools marked *paid* are metered
with [x402](https://xmr.pub/mcp): the caller signs an EIP-3009 authorization with its **own** wallet
and attaches it to the tool call. On settlement the paying address receives a credit pack:

| Pack | Credit |
| --- | --- |
| 1 USDC | 100 paid calls |
| 0.1 USDC | 10 paid calls |

Accepted assets: USDC on Base (`eip155:8453`) and USDC on Solana. A successful settlement also
returns an API key bound to that address, so follow-up calls can be made with `X-API-Key` instead of
re-signing on every request. There is no subscription, no account, no email and no separately sold
API key: credits are bought by paying, not by registering.

An unpaid call to a metered tool returns a standard MCP *payment required* result carrying the
`accepts[]` list — a client can read the price list from the error itself.

## Privacy and data handling

These are guarantees implemented in `mcpserver.py`, not marketing copy:

- **Tool arguments are never logged.** Arguments can contain transaction keys, view keys and
  merchant addresses, so the audit table (`usage.db`) stores only: timestamp, tool name, success or
  failure, latency, a key fingerprint, and a truncated-IP hash. There is no column for parameters,
  amounts or addresses.
- **A `view_key` is used in-process for the immediate check only.** The three view-key tools create a
  one-shot read-only wallet that lives on the service container's tmpfs (memory), is closed and
  deleted at the end of the call, and is additionally swept by a host-side job after ten minutes.
  Nothing is written to persistent storage.
- **Read-only really means read-only.** The wallet RPC password used by the service belongs to a
  read-only wallet context; no spend key ever exists on the server, so a compromised service cannot
  move funds.
- **Response bodies echo addresses in truncated form** (`4abcd…Xy`, first five and last two
  characters) wherever an address is echoed
  back for readability.

## Trust model and limits

Stated plainly, because these are the things a merchant or an agent builder will hit:

- **Single data source.** Height, difficulty and mempool answers describe *what our node sees*, not
  a multi-node consensus view. That is normally fine for fee estimation and coarse reconciliation,
  and it is the wrong tool for a "prove the chain state to a third party" requirement.
- **Payment verification needs the payer's `tx_key`.** Monero hides the amount from everyone without
  the view key, and the *destination* amount from the recipient without the transaction key. If a
  payer does not hand over a `tx_key` (e.g. many exchange withdrawals), no service can decrypt that
  amount for you — that is the protocol, not a pricing decision.
- **Bounded windows.** `xmr_merchant_report` scans at most 30 days and returns at most 2000 detail
  rows; `xmr_verify_payment` accepts at most 100 payments per call.
- **Reads only.** Nothing here can construct, sign, relay or broadcast a transaction, and there is
  no endpoint that touches your wallet.
- **Rate limits.** 120 requests/minute per IP anonymously, 600 requests/minute with a key. Wallet
  RPC concurrency is 2, and report scans are serialised — a large report may queue behind another.
- **No pool data.** xmr.pub runs no mining pool, so no tool here returns pool or hashrate-share data.
  `xmr_mining_profitability` is a forward-looking calculator that uses network-wide chain figures
  only; it is not a settlement statement.

## Connecting an MCP client

```json
{
  "mcpServers": {
    "xmr-pub": {
      "type": "streamable-http",
      "url": "https://xmr.pub/mcp"
    }
  }
}
```

The endpoint speaks JSON-RPC over HTTP (`initialize`, `ping`, `tools/list`, `tools/call`) at the
single path `/mcp`. `GET /mcp` without a JSON-RPC body returns a 404 with a pointer to the endpoint.

## Self-hosting

`mcpserver.py` is dependency-free — standard library only, Python 3.9+. Copy it next to your nodes
and set the environment variables (see [`examples/env.example`](./examples/env.example)):

| Variable | Purpose |
| --- | --- |
| `MONEROD_RPC` | Restricted-RPC `monerod` (read-only). Required for all three free tools. |
| `XMR_HELPER_RPC` | Optional whitelisted full-RPC helper, used only for `get_transaction_pool`. Without it, `xmr_mempool_health` degrades to `get_transaction_pool_stats` and reports `pool_source: unavailable`. |
| `WALLET_RPC_HOST` / `WALLET_RPC_PORT` / `WALLET_RPC_PATH` / `WALLET_RPC_USER` / `WALLET_RPC_PASS` | Optional `monero-wallet-rpc`, needed only by the three view-key tools. |
| `USAGE_DB` | SQLite path for the parameter-free audit table. |
| `API_KEY_FILE` | Optional allowlist: one key per line, hot-reloaded by mtime. Keys raise the rate ceiling only. |
| `MCP_PORT` | Listening port (default 8080). |
| `ANON_RATE_PER_MIN` / `KEY_RATE_PER_MIN` | Rate ceilings. |
| `RB_X402_CONFIG` | Optional config for the `x402_pay` module. When that module is missing the paid tools return a payment-required result instead of running; the free tools are unaffected. |

```bash
MONEROD_RPC=http://127.0.0.1:18089 python3 mcpserver.py
```

Or build the reference container:

```bash
docker build -t xmr-pub-mcp .
docker run --rm -p 8080:8080 -v /srv/xmrpub/data:/data \
  -e MONEROD_RPC=http://10.0.0.5:18089 xmr-pub-mcp
```

### 中文速览

只读门罗工具集，7 个工具：节点状态 / 动态费率 / 内存池健康度 免费，交易与到账核验、收款流水、收益测算走 x402
信用包计费（1 USDC = 100 次）。服务端不托管资产、不广播交易、不记录任何工具参数；需要 view key 的工具用一次性只读钱包在
内存盘里完成校验并即时销毁。本站不运行矿池，因此没有任何池相关工具。

## License

MIT — see [LICENSE](./LICENSE).

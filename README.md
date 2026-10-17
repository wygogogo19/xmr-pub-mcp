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
re-signing on every request. That key is shown **once**, in the machine-readable
`structuredContent`; the human/LLM-facing text block only carries its prefix, and the key is never
logged. There is no subscription, no account, no email and no separately sold API key: credits are
bought by paying, not by registering.

Malformed arguments are rejected **before** any credit is spent: call the tool with the wrong shape
and you get `invalid_arguments` naming the missing or invalid field, with your balance untouched.

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
single path `/mcp`. There is no SSE stream, so `GET /mcp` answers **405 Method Not Allowed** with
`Allow: POST, OPTIONS` and a body that points at the endpoint, the site docs and this repository —
which is what the Streamable HTTP transport expects instead of a 404. Unknown paths still 404.

## Self-hosting

`mcpserver.py` is dependency-free — standard library only, Python 3.9+. It talks to nodes **you**
run; nothing in it is hard-wired to xmr.pub's hosts, and every endpoint is an environment variable
(see [`examples/env.example`](./examples/env.example)). The examples below use the port numbers of
our own deployment, so map them onto yours (table further down).

### 1. Point it at your own `monerod`

Only a restricted-RPC `monerod` is required, and it has to be reachable over plain HTTP **without
RPC login**: this client sends credentials to the wallet RPC (HTTP digest) but not to the node. Keep
the node bound to localhost, or firewall it.

```bash
# on the node host: restricted RPC on 18089, bound to loopback
# (--rpc-restricted-bind-ip already defaults to 127.0.0.1, so no extra flag is needed)
monerod --rpc-restricted-bind-port 18089
```

If the node and this server are not on the same host — that is our case, the MCP runs in a container
on another machine — the node must listen on an interface the MCP host can reach:

```bash
monerod --rpc-restricted-bind-ip 0.0.0.0 --rpc-restricted-bind-port 18089 --confirm-external-bind
# then firewall 18089 so only the MCP host can reach it, and set MONEROD_RPC to that address
```

A node bound that way is *not* reachable at `127.0.0.1` anymore, so `MONEROD_RPC` has to name the
address you actually bound. Also note `--rpc-restricted-bind-port` has no built-in default: leave it
out and monerod serves the restricted RPC on the main RPC port (`18081` mainnet) instead.

Preflight it before starting the server — this one line catches every port or bind mismatch:

```bash
curl -s -m 10 -X POST http://127.0.0.1:18089/json_rpc \
  -d '{"jsonrpc":"2.0","id":"0","method":"get_info"}' | head -c 200
# expect JSON with "height" / "synchronized"; "Connection refused" means MONEROD_RPC is wrong
```

| Value in our examples | Whose convention | What to use on your own box |
| --- | --- | --- |
| node RPC `127.0.0.1:18089` | community convention for the **restricted** RPC port | `monerod`'s built-in default is to serve the restricted RPC on the *main* RPC port (`18081` mainnet / `28081` testnet / `38081` stagenet). Set `MONEROD_RPC` to whatever you actually bound. |
| wallet RPC | **ours: `18084`** | `monero-wallet-rpc` defaults to `18083` on mainnet, which is what §3's example uses — set `WALLET_RPC_PORT` to yours. |
| helper `127.0.0.1:18085/rpc` | **ours** | You supply your own, or omit it entirely. |

### 2. Endpoints and knobs

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

### 3. Run it

Free tools only — that is all you need for node status, fees and mempool health:

```bash
MONEROD_RPC=http://127.0.0.1:18089 python3 mcpserver.py
```

Add the optional upstreams: the three verification tools need a read-only `monero-wallet-rpc`, and
`xmr_mempool_health` gives fuller output with a helper that exposes `get_transaction_pool`:

```bash
MONEROD_RPC=http://127.0.0.1:18089 \
XMR_HELPER_RPC=http://127.0.0.1:18085/rpc \
WALLET_RPC_HOST=127.0.0.1 WALLET_RPC_PORT=18083 \
WALLET_RPC_USER=myuser WALLET_RPC_PASS=mypass \
python3 mcpserver.py
```

Missing pieces degrade instead of crashing: without `WALLET_RPC_*` the startup log says
`wallet_rpc=off` and only the view-key tools fail; without `XMR_HELPER_RPC` only
`xmr_mempool_health` falls back and reports `pool_source: unavailable`.

### 4. Or use the reference container

```bash
docker build -t xmr-pub-mcp .

# A) node on the same host, bound to loopback (the §1 default): share the host network
docker run --rm --network host -v "$PWD/data:/data" \
  -e MONEROD_RPC=http://127.0.0.1:18089 xmr-pub-mcp

# B) node elsewhere on your LAN: publish the port and name the node's address
#    (replace 10.0.0.5 with your own node host — this is a placeholder)
docker run --rm -p 8080:8080 -v "$PWD/data:/data" \
  -e MONEROD_RPC=http://10.0.0.5:18089 xmr-pub-mcp
```

Three things bite people here. Inside the container `127.0.0.1` is *the container*, not your host —
which is exactly why variant A needs `--network host`. A loopback-bound node is **not** reachable
through the docker bridge (`host.docker.internal` included), so for variant B the node has to be
bound to a reachable interface as shown in §1. And the `-v` path must be a directory you own
(`$PWD/data`), because `/data` holds `usage.db`.

### 5. Before you expose it

The verification tools accept your **view key in the request body**, so put this behind TLS
(Cloudflare Tunnel, nginx, Caddy, …) and never expose plain HTTP to the internet. The free telemetry
tools are unauthenticated by design and rate limited per IP; there is no admin surface and no
endpoint that can move funds.

Metering is not part of this repository: `x402_pay.py` is intentionally omitted, so on a self-hosted
instance the four metered tools answer with a payment-required result instead of running. The hosted
instance at `https://xmr.pub/mcp` is the one that settles x402 payments.

### 中文速览

只读门罗工具集，7 个工具：节点状态 / 动态费率 / 内存池健康度 免费，交易与到账核验、收款流水、收益测算走 x402
信用包计费（1 USDC = 100 次）。服务端不托管资产、不广播交易、不记录任何工具参数；需要 view key 的工具用一次性只读钱包在
内存盘里完成校验并即时销毁。本站不运行矿池，因此没有任何池相关工具。自托管只需自备一个受限 RPC 的 `monerod`（可选
再加 `monero-wallet-rpc`），端口与地址全部通过环境变量指定，不写死；对外暴露前务必套 TLS，因为 view key 在请求体里传输。

## License

MIT — see [LICENSE](./LICENSE).

#!/usr/bin/env python3
"""
xmr.pub —— MCP（Model Context Protocol）服务端：给 AI Agent 用的门罗只读工具集

为什么这样做
  门罗的链上数据虽然对普通人不可见，但**节点共识层、动态区块/费率、P2Pool 侧链、
  以及「只读密钥对账」（tx-proof / view-key）** 都是可编程的。本服务把这些能力
  封装成 MCP 工具，让 Agent 在不接触私钥的前提下完成对账、报价、矿池查询。

传输
  采用 MCP Streamable HTTP：客户端 POST JSON-RPC 到 https://xmr.pub/mcp。
  支持 initialize / notifications/initialized / tools/list / tools/call / ping。

工具分层（v3.0.0，2026-10-14 —— 站内已彻底移除矿池，故工具里也不再有任何池相关能力）
  免费层（4 个，获客）：节点状态 / 动态费率 / 内存池健康度 / tx-proof 单笔对账。
  数据产品层（3 个，**先免费开放但记录用量**）：
    xmr_verify_payment       —— 单笔/批量到账核验（txid + tx_key，金额容差，商户对账）
    xmr_merchant_report      —— 指定地址的时间窗收款流水（一次性只读钱包扫描，最多 30 天）
    xmr_mining_profitability —— 给定算力与电价的日/月净收益、盈亏平衡电价（只用链上网络口径）

安全红线（务必遵守）
  * 只读：本服务不广播任何交易、不持有任何私钥；
  * **绝不记录**任何工具参数（可能含 tx_key / view_key），日志只写"工具名 + 结果状态"；
  * 需要 view_key 的对账工具，用**一次性只读钱包**在 tmpfs 中完成，用完立刻删除；
  * 用量审计（usage.db）只落「工具名 / 成败 / 耗时 / key 指纹 / 截断 IP 哈希」，不含参数与金额；
  * 只依赖标准库；无任何第三方 JS（站点 CSP 的 script-src 'self' 不受影响）。
"""

import base64
import hashlib
import http.client
import json
import os
import re
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# ---------- 上游 ----------
# 三个上游都可以用环境变量指向你自己的节点（见 README 的 Self-hosting 一节）。
MONEROD = os.environ.get("MONEROD_RPC", "http://127.0.0.1:18089")
# Monero v0.18.5.3 起受限 RPC 不再提供 /get_transaction_pool（上游 #10973），
# 内存池明细因此改走一个**白名单化的 full-RPC helper**（只放行只读方法）。
XMR_HELPER = os.environ.get("XMR_HELPER_RPC", "http://127.0.0.1:18085/rpc")
WALLET_HOST = os.environ.get("WALLET_RPC_HOST", "127.0.0.1")
WALLET_PORT = int(os.environ.get("WALLET_RPC_PORT", "18084"))
WALLET_PATH = os.environ.get("WALLET_RPC_PATH", "/json_rpc")
WALLET_USER = os.environ.get("WALLET_RPC_USER", "")
WALLET_PASS = os.environ.get("WALLET_RPC_PASS", "")
# 用量计量：先免费开放一周，靠这里的真实调用数据决定要不要上收费墙
USAGE_DB = os.environ.get("USAGE_DB", "/data/usage.db")
# 免费期限速：匿名按 IP、带 key 按 key 指纹（配额仅为防刷，不做计费）
ANON_RATE_PER_MIN = int(os.environ.get("ANON_RATE_PER_MIN", "120"))
KEY_RATE_PER_MIN = int(os.environ.get("KEY_RATE_PER_MIN", "600"))
# 可选的 key 允许清单（一行一个 key，按 mtime 热加载）。
# 清单里没有的 key 一律按匿名口径限速（不会误给提额，也不会把垃圾 key 计入 key 维度统计）。
API_KEY_FILE = os.environ.get("API_KEY_FILE", "/data/api_keys.txt")
PORT = int(os.environ.get("MCP_PORT", "8080"))

# ---- x402 付款面（可选的 x402_pay 模块；缺文件时整个服务仍可用，只是付费工具不启用）----
# 计费口径是**信用包**（一次链上结算买 N 次调用，摊薄链上 gas），
# 结算成功后签发绑定付款地址的 API key，后续调用走 X-API-Key。所有链上动作都在 facilitator 里，
# 本服务不碰私钥、不写链。
try:
    import x402_pay
except Exception:                      # 缺 x402_pay.py 时其余工具照常工作
    x402_pay = None

# 2026-10-17 修复：账本表此前从未创建（init() 没被调用），任何带付款的请求都在 _seen() 处
# 抛 sqlite3.OperationalError: no such table → 进程未回包 → Cloudflare 502。启动时幂等建表。
if x402_pay is not None:
    try:
        x402_pay.init()
    except Exception as _exc:          # noqa: BLE001
        print("x402 ledger init failed: %s" % _exc, flush=True)

# 付费工具：对账 / 报表 / 测算这一类「商户与矿工真正要买」的工具。
# 节点状态、费率、内存池三个遥测工具保持免费——它们是发现层（Registry / 目录里被搜到的入口）。
PAID_TOOLS = ("xmr_verify_tx_proof", "xmr_verify_payment",
              "xmr_merchant_report", "xmr_mining_profitability")

def _x402_enabled():
    return x402_pay is not None and bool(x402_pay.enabled())

def _payment_required(tool, reason, mid):
    """按 MCP 传输规范返回 PaymentRequired（structuredContent + 人类可读说明）。
    共享模块的文案是第三方品牌，这里整体重写成 xmr.pub 自己的措辞（含 content[0] 同步）。"""
    out = x402_pay.payment_required(tool, reason)
    obj = out.get("structuredContent") or {}
    res = obj.get("resource")
    if isinstance(res, dict):
        res["url"] = "https://xmr.pub/mcp#%s" % tool
        res["description"] = "xmr.pub Monero read-only tool: %s" % tool
    c = x402_pay.cfg()
    dec = 10 ** int(c.get("decimals", 6))
    packs = [{"amount": p["amount_atomic"], "usdc": int(p["amount_atomic"]) / dec,
              "calls": int(p["calls"])} for p in x402_pay.packs()]
    nets = [{"network": o["network"], "asset": o["asset"], "payTo": o["pay_to"],
             "label": o.get("label", "")} for o in x402_pay.options()]
    obj["extensions"] = {"xmr.pub": {
        "pricing": "credit packs: " + "; ".join("%g USDC = %d calls" % (p["usdc"], p["calls"]) for p in packs),
        "packs": packs, "networks": nets,
        "facilitator": {"mode": "self-hosted",
                        "note": "Settlement is performed by xmr.pub server-side via its own x402 facilitator "
                                "(verify then settle); payers only send the signed payload."},
        "paidTools": list(PAID_TOOLS),
        "discovery": "https://xmr.pub/.well-known/x402",
        "docs": "https://xmr.pub/llms.txt",
    }}
    human = (
        "%s\n\n%s is a paid xmr.pub tool. Buy an x402 credit pack (%s) in USDC on %s, then retry this "
        "same call with the payment payload in params._meta[\"x402/payment\"]. Full requirements are in "
        "structuredContent.accepts[]. A successful payment returns an API key (X-API-Key header) for the "
        "remaining credits. Free tools: xmr_node_status, xmr_fee_oracle, xmr_mempool_health."
        % (obj.get("error", "payment required"), tool,
           ", ".join("%g USDC = %d calls" % (p["usdc"], p["calls"]) for p in packs),
           " / ".join("%s (payTo %s)" % (n["label"] or n["network"], n["payTo"]) for n in nets)))
    out["content"] = [{"type": "text", "text": json.dumps(obj, ensure_ascii=False)},
                      {"type": "text", "text": human}]
    out["structuredContent"] = obj
    return {"jsonrpc": "2.0", "id": mid, "result": out}

SERVER_NAME = "xmr.pub"
SERVER_VERSION = "2.1.0"
PROTOCOL_VERSION = "2025-06-18"              # 最新支持版本；客户端请求受支持版本时原样回显
SUPPORTED_PROTOCOLS = ("2025-06-18", "2025-03-26", "2024-11-05")

XMR_ADDR_RE = re.compile(r"^[48][1-9A-HJ-NP-Za-km-z]{94}$")
HEX64_RE = re.compile(r"^[0-9a-fA-F]{64}$")

_wallet_lock = threading.BoundedSemaphore(2)   # 钱包 RPC 并发上限（服务端 max-concurrency=2）
_scan_lock = threading.BoundedSemaphore(1)     # 流水扫描同时只跑一个（避免占满钱包 RPC 的两条并发）
_cache = {}


# ================= 通用工具 =================
def http_json(url, payload=None, timeout=25, headers=None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method="POST" if data else "GET")
    req.add_header("User-Agent", "xmr.pub-mcp/1.0")
    if data:
        req.add_header("Content-Type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def monerod(path, payload=None, timeout=25):
    """调用受限 RPC；path 可以是 /json_rpc 或路径式端点"""
    return http_json(MONEROD + path, payload if payload is not None else {}, timeout)


def xmr_helper(method, params=None, timeout=25):
    """经白名单化的 full-RPC helper 通道调用 full RPC（仅限其 ALLOWED 方法）"""
    out = http_json(XMR_HELPER, {"method": method, "params": params or {}}, timeout)
    if not out.get("ok"):
        raise RuntimeError(out.get("error") or "xmr-helper error")
    return out.get("result") or {}


def cached(key, ttl, fn):
    hit = _cache.get(key)
    now = time.time()
    if hit and now - hit[0] < ttl:
        return hit[1]
    val = fn()
    _cache[key] = (now, val)
    return val


def atomics(amount):
    try:
        return round(int(amount) / 1e12, 12)
    except Exception:
        return None


# ================= 用量计量（免费观察期的事实依据） =================
# 设计取舍：这里**不含任何请求参数**（参数可能含 tx_key / view_key / 商户地址），
# 只保留可观测量——工具名、成败、耗时、key 指纹（前 16 位哈希）、截断 IP 哈希。
_usage_lock = threading.Lock()
_usage_conn = None


def _usage():
    global _usage_conn
    if _usage_conn is not None:
        return _usage_conn
    try:
        d = os.path.dirname(USAGE_DB)
        if d:
            os.makedirs(d, exist_ok=True)
        conn = sqlite3.connect(USAGE_DB, check_same_thread=False, timeout=5)
        conn.execute("CREATE TABLE IF NOT EXISTS calls ("
                     "ts INTEGER NOT NULL, tool TEXT NOT NULL, ok INTEGER NOT NULL, "
                     "ms INTEGER NOT NULL, key_id TEXT, ip_h TEXT)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_calls_ts ON calls(ts)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_calls_tool ON calls(tool, ts)")
        conn.commit()
        _usage_conn = conn
    except Exception as exc:                      # 计量失败绝不影响工具本身
        print("usage db unavailable: %s" % exc, flush=True)
        _usage_conn = None
    return _usage_conn


def record_call(tool, ok, ms, key_id=None, ip_h=None):
    with _usage_lock:
        conn = _usage()
        if conn is None:
            return
        try:
            conn.execute("INSERT INTO calls(ts,tool,ok,ms,key_id,ip_h) VALUES(?,?,?,?,?,?)",
                         (int(time.time()), tool, 1 if ok else 0, int(ms), key_id, ip_h))
            conn.commit()
        except Exception:
            pass


def usage_summary(hours=24):
    with _usage_lock:
        conn = _usage()
        if conn is None:
            return {"available": False}
        try:
            since = int(time.time()) - max(1, min(int(hours), 24 * 90)) * 3600
            rows = conn.execute(
                "SELECT tool, COUNT(*), SUM(1-ok), AVG(ms), COUNT(DISTINCT ip_h), "
                "COUNT(DISTINCT key_id) FROM calls WHERE ts>=? GROUP BY tool ORDER BY 2 DESC",
                (since,)).fetchall()
            tot = conn.execute("SELECT COUNT(*), COUNT(DISTINCT ip_h), COUNT(DISTINCT key_id) "
                               "FROM calls WHERE ts>=?", (since,)).fetchone()
            return {
                "available": True, "window_hours": int(hours),
                "total_calls": tot[0] or 0, "unique_ips": tot[1] or 0, "unique_keys": tot[2] or 0,
                "by_tool": [{"tool": r[0], "calls": r[1], "errors": r[2] or 0,
                             "avg_ms": round(r[3] or 0), "unique_ips": r[4],
                             "unique_keys": r[5]} for r in rows],
            }
        except Exception as exc:
            return {"available": False, "error": str(exc)[:120]}


# ================= 免费期限速（防刷，不做计费） =================
_rate_lock = threading.Lock()
_rate = {}


def rate_ok(bucket, limit):
    now = int(time.time())
    with _rate_lock:
        win, cnt = _rate.get(bucket, (now, 0))
        if now - win >= 60:
            win, cnt = now, 0
        cnt += 1
        _rate[bucket] = (win, cnt)
        if len(_rate) > 20000:                 # 防内存膨胀：落伍的桶定期清掉
            for k in [k for k, v in _rate.items() if now - v[0] >= 60]:
                _rate.pop(k, None)
        return cnt <= limit, max(0, 60 - (now - win))


_keys_lock = threading.Lock()
_keys_cache = (0.0, frozenset())


def valid_keys():
    """读取 key 允许清单（不存在 = 空清单）。"""
    global _keys_cache
    try:
        mtime = os.path.getmtime(API_KEY_FILE)
    except OSError:
        return frozenset()
    with _keys_lock:
        if _keys_cache[0] == mtime:
            return _keys_cache[1]
        keys = set()
        try:
            with open(API_KEY_FILE, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        keys.add(line)
        except OSError:
            pass
        _keys_cache = (mtime, frozenset(keys))
        return _keys_cache[1]


def mask_addr(value):
    """地址只回显截断形式（本集群对外展示地址的统一口径）。"""
    v = (value or "").strip()
    return (v[:5] + "..." + v[-2:]) if len(v) >= 30 else v


# ================= 只读钱包（tx-proof） =================
class WalletClient:
    """monero-wallet-rpc 需要 HTTP Digest，且 **nonce 绑定同一条 TCP 连接**：
    401 挑战与带认证的重试必须复用同一连接（否则永远 401）。"""

    def __init__(self):
        self.conn = None
        self.challenge = ""

    def _connect(self):
        if self.conn is None:
            self.conn = http.client.HTTPConnection(WALLET_HOST, WALLET_PORT, timeout=90)
        return self.conn

    @staticmethod
    def _digest(user, pw, challenge, method, uri):
        def f(name):
            m = re.search(name + r'="([^"]*)"', challenge)
            return m.group(1) if m else ""
        realm, nonce, qop = f("realm"), f("nonce"), f("qop") or "auth"
        nc, cnonce = "00000001", uuid.uuid4().hex
        md5 = lambda s: hashlib.md5(s.encode()).hexdigest()
        ha1 = md5("%s:%s:%s" % (user, realm, pw))
        ha2 = md5("%s:%s" % (method, uri))
        resp = md5("%s:%s:%s:%s:%s:%s" % (ha1, nonce, nc, cnonce, qop, ha2))
        return ('Digest username="%s", realm="%s", nonce="%s", uri="%s", cnonce="%s", '
                'nc=%s, qop=%s, response="%s", algorithm=MD5'
                % (user, realm, nonce, uri, cnonce, nc, qop, resp))

    def _raw(self, body, timeout, auth=None):
        conn = self._connect()
        conn.timeout = timeout
        headers = {"Content-Type": "application/json"}
        if auth:
            headers["Authorization"] = auth
        conn.request("POST", WALLET_PATH, body, headers)
        r = conn.getresponse()
        raw = r.read()
        if r.status == 401:
            self.challenge = r.getheader("WWW-Authenticate", "") or self.challenge
        return r.status, raw

    def call(self, method, params=None, timeout=90):
        """带重试的调用。钱包 RPC 重启 / 连接空闲超时都会让缓存 nonce 失效，
        这里在异常或 401 时清空 challenge 并整轮重来（最多 3 轮）。"""
        body = json.dumps({"jsonrpc": "2.0", "id": "0", "method": method,
                           "params": params or {}}).encode()
        last = "wallet_auth_failed"
        for _ in range(3):
            try:
                if not self.challenge:
                    self._raw(body, timeout)               # 先拿一次 401 挑战
                status, raw = self._raw(body, timeout,
                                        self._digest(WALLET_USER, WALLET_PASS,
                                                     self.challenge, "POST", WALLET_PATH))
                if status == 200:
                    return json.loads(raw.decode("utf-8"))
                if status == 401:
                    self.challenge = ""                    # nonce 过期 → 重新挑战
                    continue
                last = "wallet_http_%d" % status
                self._reset()
            except Exception as exc:
                last = str(exc)[:120]
                self._reset()
        return {"error": last}

    def _reset(self):
        try:
            if self.conn:
                self.conn.close()
        except Exception:
            pass
        self.conn = None
        self.challenge = ""


_wallet = WalletClient()


def verify_tx_proof(txid, address, tx_key, view_key):
    """用一次性只读钱包校验 tx-proof。

    返回 (ok, data)。任何情况下都**不回显** view_key / tx_key。
    """
    if not HEX64_RE.match(txid or ""):
        return False, {"error": "txid 必须是 64 位十六进制"}
    if not HEX64_RE.match(tx_key or ""):
        return False, {"error": "tx_key 必须是 64 位十六进制"}
    if not HEX64_RE.match(view_key or ""):
        return False, {"error": "view_key 必须是 64 位十六进制"}
    if not XMR_ADDR_RE.match(address or ""):
        return False, {"error": "address 不是合法的门罗主网地址"}
    if not WALLET_USER:
        return False, {"error": "只读钱包服务未配置"}

    height = None
    try:
        height = monerod("/json_rpc", {"jsonrpc": "2.0", "id": "0", "method": "get_info"},
                         timeout=15).get("result", {}).get("height")
    except Exception:
        pass

    name = "mcp_" + uuid.uuid4().hex
    achieved = _wallet_lock.acquire(timeout=30)
    if not achieved:
        return False, {"error": "busy：请稍后重试"}
    try:
        created = _wallet.call("generate_from_keys", {
            "filename": name, "address": address, "viewkey": view_key,
            "restore_height": int(height or 0), "password": "",
        }, timeout=90)
        if created.get("error"):
            return False, {"error": "wallet_create_failed", "detail": created["error"]}

        res = _wallet.call("check_tx_key",
                           {"txid": txid, "tx_key": tx_key, "address": address}, timeout=90)
        _wallet.call("close_wallet", {}, timeout=30)
        _wallet.call("delete_wallet", {"filename": name, "password": ""}, timeout=30)

        r = (res or {}).get("result")
        if r is None:
            detail = (res or {}).get("error", {})
            msg = detail.get("message", "") if isinstance(detail, dict) else str(detail)
            if "Failed to get transaction" in msg:
                # 工具正常执行，结论是「这 4 个参数不匹配任何已知交易」——不算工具错误
                return True, {"verified": False, "reason": "transaction_not_found",
                              "hint": "txid 不存在 / tx_key 与地址不匹配 / 该交易尚未被打包",
                              "detail": msg[:160]}
            if "connect to daemon" in msg:
                return False, {"error": "daemon_unreachable", "detail": msg[:160]}
            return False, {"error": "check_failed", "detail": msg[:160]}
        return True, {
            "verified": True,
            "received": atomics(r.get("received", 0)),
            "confirmations": r.get("confirmations"),
            "in_pool": r.get("in_pool"),
            "note": "received = 该地址在这笔交易中实际收到的 XMR",
        }
    finally:
        _wallet_lock.release()


# ================= 各工具实现 =================
def t_node_status(_args):
    d = monerod("/json_rpc", {"jsonrpc": "2.0", "id": "0", "method": "get_info"}).get("result", {})
    diff = d.get("difficulty") or 0
    return {
        "network": "monero mainnet",
        "height": d.get("height"),
        "target_height": d.get("target_height"),
        "synchronized": d.get("synchronized"),
        "status": d.get("status"),
        "difficulty": diff,
        "network_hashrate_hs": round(diff / 120.0, 2) if diff else None,
        "tx_pool_size": d.get("tx_pool_size"),
        "database_size_gb": round((d.get("database_size") or 0) / 1e9, 2),
        "tx_count": d.get("tx_count"),
        "top_block_hash": d.get("top_block_hash"),
        "block_weight_median": d.get("block_weight_median"),
        "block_weight_limit": d.get("block_weight_limit"),
        "node": "node.xmr.pub (restricted RPC, read-only)",
    }


def t_fee_oracle(_args):
    fe = monerod("/json_rpc", {"jsonrpc": "2.0", "id": "0",
                               "method": "get_fee_estimate"}).get("result", {})
    info = monerod("/json_rpc", {"jsonrpc": "2.0", "id": "0",
                                 "method": "get_info"}).get("result", {})
    fees = fe.get("fees") or []
    median = info.get("block_weight_median") or 0
    limit = info.get("block_weight_limit") or 0
    return {
        "fee_per_byte_atomics": fees,                 # [低, 中, 高, 最快]
        "fee_per_byte_xmr": [atomics(f) for f in fees],
        "quantization_mask": fe.get("quantization_mask"),
        "note": "门罗费率随区块权重浮动；v0.18 中 f[0] 通常即可打包",
        "dynamic_block_size": {
            "block_weight_median": median,
            "block_weight_limit": limit,
            "limit_to_median": round(limit / median, 3) if median else None,
            "explain": ("门罗没有固定区块上限：共识用「过去 100 块权重中位数」的 2 倍作为长/短期上限，"
                        "区块越大对矿工的出块奖励惩罚越重，因此费率会随之抬升"),
        },
        "mempool_txs": info.get("tx_pool_size"),
    }


def t_mempool_health(_args):
    raw = monerod("/get_transaction_pool_stats", {})
    st = raw.get("pool_stats", {})
    try:
        pool = xmr_helper("get_transaction_pool")
        pool_source = "full-rpc helper (whitelisted)"
    except Exception:  # noqa: BLE001 —— helper 不可用时退化为仅统计口径
        pool = {}
        pool_source = "unavailable"
    kas = pool.get("spent_key_images") or []
    info = monerod("/json_rpc", {"jsonrpc": "2.0", "id": "0",
                                 "method": "get_info"}).get("result", {})
    now = info.get("adjusted_time") or int(time.time())
    oldest = st.get("oldest")
    # histo 是费率分档直方图：累加各档交易数，便于 Agent 判断拥堵分布
    histo = st.get("histo") or []
    bands = [{"txs": h.get("txs"), "bytes": h.get("bytes")} for h in histo if h.get("txs")]
    return {
        "txs_in_pool": st.get("txs_total"),
        "bytes_total": st.get("bytes_total"),
        "tx_size_bytes": {"min": st.get("bytes_min"), "median": st.get("bytes_med"),
                          "max": st.get("bytes_max")},
        "fee_total_atomics": st.get("fee_total"),
        "oldest_tx_age_seconds": (now - oldest) if isinstance(oldest, int) and oldest else None,
        "fee_histogram_nonzero_bands": bands,
        "double_spend_window": {
            "spent_key_images_tracked": len(kas),
            "source": pool_source,
            "note": ("每个 key image 代表一个被花费的输出；门罗用 key image 唯一性防双花，"
                     "内存池中 key image 的数量是全网并发花费压力的直接指标"),
        },
    }


def t_verify_tx_proof(args):
    a = args or {}
    ok, data = verify_tx_proof(a.get("txid"), a.get("address"),
                               a.get("tx_key"), a.get("view_key"))
    return data


# ================= 数据产品层（本轮新增 4 个工具） =================
def _new_view_wallet(address, view_key, restore_height, prefix):
    """在 tmpfs 里建一次性只读钱包。返回 (name, error)。view_key 只在内存里传递。"""
    if not WALLET_USER:
        return None, "只读钱包服务未配置"
    name = "%s_%s" % (prefix, uuid.uuid4().hex)
    created = _wallet.call("generate_from_keys", {
        "filename": name, "address": address, "viewkey": view_key,
        "restore_height": int(restore_height or 0), "password": "",
    }, timeout=120)
    err = created.get("error")
    if err:
        msg = err.get("message") if isinstance(err, dict) else str(err)
        return name, "wallet_create_failed: %s" % str(msg)[:140]
    return name, None


def _drop_view_wallet(name):
    """关闭并删除一次性钱包；失败也无所谓（宿主上的定时任务是兜底清理）。"""
    try:
        _wallet.call("close_wallet", {}, timeout=30)
        _wallet.call("delete_wallet", {"filename": name, "password": ""}, timeout=30)
    except Exception:
        pass


def _wallet_msg(res):
    err = (res or {}).get("error")
    if isinstance(err, dict):
        return err.get("message", "")
    return "" if err is None else str(err)


def t_verify_payment(args):
    """单笔 / 批量到账核验：txid + tx_key 解密金额，view_key 用于一次性只读钱包。"""
    a = args or {}
    address = (a.get("address") or "").strip()
    view_key = (a.get("view_key") or "").strip()
    if not XMR_ADDR_RE.match(address):
        return {"error": "address 不是合法的门罗主网地址"}
    if not HEX64_RE.match(view_key):
        return {"error": "view_key 必须是 64 位十六进制（只读密钥，无法转账）"}

    raw_items = a.get("payments")
    if raw_items is None:                       # 单笔简写
        if not a.get("txid"):
            return {"error": "需要 payments 数组，或提供 txid/tx_key/amount 单笔参数"}
        raw_items = [{"txid": a.get("txid"), "tx_key": a.get("tx_key"),
                      "amount": a.get("amount"), "label": a.get("label")}]
    if not isinstance(raw_items, list) or not raw_items:
        return {"error": "payments 必须是非空数组"}
    if len(raw_items) > 100:
        return {"error": "单次最多核验 100 笔（更多请分批或用 xmr_merchant_report）"}

    try:
        tol = abs(float(a.get("amount_tolerance_percent", 1.0)))
    except (TypeError, ValueError):
        tol = 1.0
    tol = min(tol, 100.0)

    items = []
    for i, it in enumerate(raw_items):
        it = it or {}
        txid, tx_key = (it.get("txid") or "").strip(), (it.get("tx_key") or "").strip()
        if not HEX64_RE.match(txid):
            return {"error": "第 %d 笔的 txid 不是 64 位十六进制" % (i + 1)}
        if not HEX64_RE.match(tx_key):
            return {"error": "第 %d 笔的 tx_key 不是 64 位十六进制（付款方出示的 tx proof）" % (i + 1)}
        expected = it.get("amount")
        try:
            expected = float(expected) if expected is not None else None
        except (TypeError, ValueError):
            return {"error": "第 %d 笔的 amount 不是数字" % (i + 1)}
        if expected is not None and expected <= 0:
            return {"error": "第 %d 笔的 amount 必须大于 0" % (i + 1)}
        items.append({"txid": txid, "tx_key": tx_key, "amount": expected,
                      "label": (it.get("label") or None)})

    height = None
    try:
        height = monerod("/json_rpc", {"jsonrpc": "2.0", "id": "0", "method": "get_info"},
                         timeout=15).get("result", {}).get("height")
    except Exception:
        pass

    if not _wallet_lock.acquire(timeout=30):
        return {"error": "busy：钱包通道繁忙，请稍后重试"}
    name = None
    try:
        name, err = _new_view_wallet(address, view_key, height, "mcp_pay")
        if err:
            return {"error": err}
        results = []
        for it in items:
            res = _wallet.call("check_tx_key",
                               {"txid": it["txid"], "tx_key": it["tx_key"],
                                "address": address}, timeout=90)
            r = (res or {}).get("result")
            if r is None:
                msg = _wallet_msg(res)
                if "Failed to get transaction" in msg:
                    results.append({"txid": it["txid"], "label": it["label"], "paid": False,
                                    "status": "not_found", "expected": it["amount"],
                                    "received": None,
                                    "hint": "交易不存在 / tx_key 与地址不匹配 / 尚未上链"})
                else:
                    results.append({"txid": it["txid"], "label": it["label"], "paid": False,
                                    "status": "error", "expected": it["amount"],
                                    "received": None, "error": msg[:140] or "check_failed"})
                continue
            received = atomics(r.get("received", 0)) or 0.0
            conf = r.get("confirmations")
            in_pool = bool(r.get("in_pool"))
            if it["amount"] is None:
                paid = received > 0
            else:
                paid = received >= it["amount"] * (1 - tol / 100.0)
            results.append({
                "txid": it["txid"], "label": it["label"], "paid": paid,
                "status": "in_pool" if in_pool else ("confirmed" if (conf or 0) > 0 else "unknown"),
                "expected": it["amount"], "received": received,
                "difference": (round(received - it["amount"], 12)
                               if it["amount"] is not None else None),
                "confirmations": conf, "in_pool": in_pool,
            })
        paid_n = sum(1 for r in results if r.get("paid"))
        total = round(sum(r["received"] or 0 for r in results), 12)
        return {
            "address": mask_addr(address),
            "checked": len(results), "paid_count": paid_n,
            "all_paid": paid_n == len(results),
            "amount_tolerance_percent": tol,
            "received_total": total,
            "payments": results,
            "summary": "%d/%d 笔核验通过，合计收到 %s XMR%s" % (
                paid_n, len(results), total,
                "" if paid_n == len(results) else "（未全部到账，逐笔见 payments）"),
            "note": ("received = 该地址在这笔交易中实际收到的 XMR；只读校验，不接触私钥、不广播交易。"
                     "view_key 只在内存中用于即时校验；一次性钱包落在服务容器的 tmpfs（内存盘），"
                     "不写持久盘，并由宿主上的定时任务在 10 分钟内兜底清除"),
        }
    finally:
        if name:
            _drop_view_wallet(name)
        _wallet_lock.release()


def _bj_date(ts):
    try:
        return time.strftime("%Y-%m-%d", time.gmtime(int(ts) + 8 * 3600))   # 北京时间
    except Exception:
        return None


def t_merchant_report(args):
    """指定地址在时间窗内的收款流水（一次性只读钱包扫描，最多 30 天）。"""
    a = args or {}
    address = (a.get("address") or "").strip()
    view_key = (a.get("view_key") or "").strip()
    if not XMR_ADDR_RE.match(address):
        return {"error": "address 不是合法的门罗主网地址"}
    if not HEX64_RE.match(view_key):
        return {"error": "view_key 必须是 64 位十六进制（只读密钥，无法转账）"}
    try:
        days = int(a.get("period_days", 7))
    except (TypeError, ValueError):
        days = 7
    days = max(1, min(days, 30))
    include_pending = bool(a.get("include_pending", True))
    try:
        limit = max(1, min(int(a.get("limit", 500)), 2000))
    except (TypeError, ValueError):
        limit = 500

    try:
        info = monerod("/json_rpc", {"jsonrpc": "2.0", "id": "0", "method": "get_info"},
                       timeout=15).get("result", {})
    except Exception as exc:
        return {"error": "daemon_unreachable", "detail": str(exc)[:140]}
    top = int(info.get("height") or 0)
    if top <= 0:
        return {"error": "daemon_unreachable", "detail": "无法取得链高度"}
    restore = max(0, top - days * 720)        # 门罗 2 分钟一块 → 每天 720 块

    if not _scan_lock.acquire(timeout=20):
        return {"error": "busy：另一个流水扫描正在进行，请 30 秒后重试"}
    name = None
    try:
        name, err = _new_view_wallet(address, view_key, restore, "mcp_rpt")
        if err:
            return {"error": err}
        # 扫描预算随窗口增长（实测约 240 块/秒）；超预算就返回已完成部分并标注 complete=false
        budget = int(min(170, 25 + days * 5))
        _wallet.call("refresh", {}, timeout=budget)
        cur = _wallet.call("get_height", {}, timeout=20)
        scanned_to = ((cur or {}).get("result") or {}).get("height")
        params = {"in": True, "out": False, "pending": include_pending,
                  "failed": False, "pool": include_pending}
        res = _wallet.call("get_transfers", params, timeout=60)
        r = (res or {}).get("result")
        # 注意：无流水时 monero-wallet-rpc 返回的是**空对象** {}，不是 {"in":[],...}，
        # 所以这里必须按「是不是 dict」判断，不能按真假判断。
        if not isinstance(r, dict):
            return {"error": "get_transfers_failed", "detail": _wallet_msg(res)[:140]}
        raw = (r.get("in") or []) + (r.get("pool") or [])
        raw.sort(key=lambda x: (x.get("height") or 0, x.get("timestamp") or 0))
        rows, daily, per_day = [], {}, {}
        total = 0.0
        for t in raw:
            amt = atomics(t.get("amount", 0)) or 0.0
            ts = t.get("timestamp") or 0
            d = _bj_date(ts)
            h = t.get("height") or 0
            conf = max(0, top - h + 1) if h else None
            total += amt
            per_day[d] = per_day.get(d, 0.0) + amt
            daily[d] = daily.get(d, 0) + 1
            if len(rows) < limit:
                rows.append({"txid": t.get("txid"), "amount": amt, "height": h,
                             "date": d, "timestamp": ts, "confirmations": conf,
                             "unlocked": not bool(t.get("locked")),
                             "pending": h == 0})
        counts = {}
        for t in raw:
            v = atomics(t.get("amount", 0)) or 0.0
            counts[v] = counts.get(v, 0) + 1
        top_amounts = [{"amount": k, "count": v}
                       for k, v in sorted(counts.items(), key=lambda kv: -kv[1])[:5]]
        complete = bool(scanned_to and scanned_to >= top)
        return {
            "address": mask_addr(address),
            "period_days": days,
            "window": {"from_height": restore, "to_height": top,
                       "scanned_to_height": scanned_to, "complete": complete},
            "tx_count": len(raw), "received_total": round(total, 12),
            "daily": [{"date": d, "tx_count": daily[d], "amount": round(per_day[d], 12)}
                      for d in sorted(daily)],
            "top_amounts": top_amounts,
            "transfers": rows,
            "truncated": len(raw) > len(rows),
            "summary": "近 %d 天共 %d 笔收款、合计 %s XMR（%s）" % (
                days, len(raw), round(total, 12),
                "完整扫描" if complete else "扫描未完成，请稍后重试以补齐"),
            "note": ("只读钱包在服务容器的 tmpfs（内存盘）中一次性创建，不写持久盘，"
                     "session 结束即关闭、文件由宿主上的定时任务在 10 分钟内兜底清除；"
                     "扫描预算内未完成时会返回已完成部分（complete=false）"),
        }
    finally:
        if name:
            _drop_view_wallet(name)
        _scan_lock.release()


def t_mining_profitability(args):
    """矿工收益测算：给定算力/功耗/电价，算日/月净收益、盈亏平衡电价与 ROI。"""
    a = args or {}
    hs = a.get("hashrate_hs")
    if hs is None and a.get("hashrate_khs") is not None:
        hs = a.get("hashrate_khs")
        try:
            hs = float(hs) * 1000.0
        except (TypeError, ValueError):
            hs = None
    if hs is None and a.get("hashrate_mhs") is not None:
        try:
            hs = float(a.get("hashrate_mhs")) * 1e6
        except (TypeError, ValueError):
            hs = None
    try:
        hs = float(hs)
    except (TypeError, ValueError):
        return {"error": "需要 hashrate_hs（或 hashrate_khs / hashrate_mhs），单位 H/s"}
    if hs <= 0:
        return {"error": "hashrate 必须大于 0"}

    def _f(key):
        v = a.get(key)
        try:
            return float(v) if v is not None and v != "" else None
        except (TypeError, ValueError):
            return None

    watts = _f("power_watts")
    price_kwh = _f("power_cost_kwh")
    xmr_price = _f("xmr_price_usd")
    hw_cost = _f("hardware_cost_usd")
    fee_pct = _f("service_fee_percent") or 0.0
    fee_pct = max(0.0, min(fee_pct, 100.0))

    # 网络与奖励：只依赖本节点的链上口径（2026-10-14 起 xmr.pub 不再运行任何矿池，故不引用池数据）
    net_hash = reward = height = None
    try:
        info = monerod("/json_rpc", {"jsonrpc": "2.0", "id": "0", "method": "get_info"},
                       timeout=15).get("result", {})
        diff = info.get("difficulty") or 0
        net_hash = (diff / 120.0) if diff else None
        height = info.get("height")
    except Exception as exc:
        return {"error": "network_data_unavailable", "detail": str(exc)[:140]}
    if reward is None:
        reward = 0.6022     # 门罗尾部通胀奖励约 0.6 XMR/块，仅作兜底
    if not net_hash or not reward:
        return {"error": "network_data_unavailable", "detail": "无法取得全网算力/区块奖励"}

    share = hs / float(net_hash)
    daily_blocks = 720.0
    gross_day = daily_blocks * float(reward) * share * (1 - fee_pct / 100.0)
    gross_month = gross_day * 30

    out = {
        "hashrate_hs": hs,
        "service_fee_percent": fee_pct,
        "network": {"height": height, "network_hashrate_hs": round(float(net_hash), 2),
                    "block_reward_xmr": float(reward), "blocks_per_day": 720,
                    "your_share_of_network": share},
        "gross_xmr": {"per_day": round(gross_day, 8), "per_month": round(gross_month, 6),
                      "per_year": round(gross_day * 365, 6)},
    }
    if watts is not None:
        kwh_day = watts * 24.0 / 1000.0
        out["power"] = {"watts": watts, "kwh_per_day": round(kwh_day, 4),
                        "kwh_per_month": round(kwh_day * 30, 3)}
        if price_kwh is not None:
            cost_day = kwh_day * price_kwh
            out["power"]["cost_per_day"] = round(cost_day, 4)
            out["power"]["cost_per_month"] = round(cost_day * 30, 2)
            out["breakeven_xmr_price_usd"] = round(cost_day / gross_day, 4) if gross_day > 0 else None
            if xmr_price is not None:
                rev_day = gross_day * xmr_price
                net_day = rev_day - cost_day
                out["usd"] = {"xmr_price_usd": xmr_price, "revenue_per_day": round(rev_day, 4),
                              "net_per_day": round(net_day, 4),
                              "net_per_month": round(net_day * 30, 2)}
                out["profitable"] = net_day > 0
                if hw_cost and net_day > 0:
                    out["roi_days"] = round(hw_cost / net_day, 1)
                if gross_day > 0:
                    out["breakeven_electricity_usd_per_kwh"] = round(
                        gross_day * xmr_price / kwh_day, 4)
    if xmr_price is None:
        out["note"] = ("未提供 xmr_price_usd，因此只给出 XMR 口径；补上 xmr_price_usd / power_watts / "
                       "power_cost_kwh 可得到净收益与盈亏平衡电价")
    out["summary"] = ("当前全网算力 %s H/s、区块奖励 %s XMR：你的 %s H/s 约占全网 %s，"
                      "毛收益约 %s XMR/天（%s XMR/月）" % (
                          round(float(net_hash), 2), float(reward), hs, "%.3e" % share,
                          round(gross_day, 8), round(gross_month, 6)))
    return out
TOOLS = [
    {
        "name": "xmr_node_status",
        "description": "门罗主网实时状态：高度、同步状态、难度与全网算力、内存池、链库体积。数据来自 xmr.pub 自建全节点。",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        "handler": t_node_status,
    },
    {
        "name": "xmr_fee_oracle",
        "description": ("门罗动态区块大小与费率预言机：返回当前费率档位，以及区块权重中位数/上限，"
                        "解释门罗「自适应区块」共识（无固定区块上限，按权重惩罚调节）。"),
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        "handler": t_fee_oracle,
    },
    {
        "name": "xmr_mempool_health",
        "description": "内存池健康度：排队交易数、体积、费率中位数、最老交易等待时间，以及 key image（防双花）跟踪规模。",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        "handler": t_mempool_health,
    },
    {
        "name": "xmr_verify_tx_proof",
        "description": ("【零私钥对账】用 tx-proof 校验某笔交易是否真的支付给你的地址："
                        "调用方提供 txid、收款地址、对方给出的 tx_key、以及你自己的 view_key，"
                        "服务端用一次性只读钱包校验（不落盘、不记录、无法转账）。"
                        "Paid: x402 credit pack or API key."),
        "inputSchema": {
            "type": "object",
            "properties": {
                "txid": {"type": "string", "description": "64 位十六进制交易哈希"},
                "address": {"type": "string", "description": "你的门罗收款地址（4/8 开头，95 字符）"},
                "tx_key": {"type": "string", "description": "付款方提供的交易密钥（tx proof）"},
                "view_key": {"type": "string", "description": "你的只读私钥 view key（无法用于转账）"},
            },
            "required": ["txid", "address", "tx_key", "view_key"],
            "additionalProperties": False,
        },
        "handler": t_verify_tx_proof,
    },
    {
        "name": "xmr_verify_payment",
        "description": ("【商户对账·可批量】核验一笔或多笔门罗到账：调用方给出收款地址、只读 view_key，"
                        "以及每笔的 txid + 付款方的 tx_key（可选 amount 期望金额），服务端用一次性只读钱包"
                        "解密出「该地址在这笔交易里实际收到多少 XMR」，并给出 paid / 确认数 / 差额。"
                        "When to use：订单到账确认、OTC 收款核验、批量对账。"
                        "Do not use：查交易是否上链（用 xmr_verify_tx_proof 或区块浏览器）。"
                        "Paid: x402 credit pack or API key."),
        "inputSchema": {
            "type": "object",
            "properties": {
                "address": {"type": "string", "description": "你的门罗收款地址（4/8 开头，95 字符）"},
                "view_key": {"type": "string", "description": "该地址的只读私钥 view key（无法转账）"},
                "amount_tolerance_percent": {"type": "number",
                                             "description": "允许的金额容差百分比，默认 1.0"},
                "payments": {
                    "type": "array",
                    "description": "批量核验列表（≤100 笔）",
                    "items": {
                        "type": "object",
                        "properties": {
                            "txid": {"type": "string"}, "tx_key": {"type": "string"},
                            "amount": {"type": "number", "description": "期望金额（XMR），可省略"},
                            "label": {"type": "string", "description": "你自己的订单号/备注"}},
                        "required": ["txid", "tx_key"], "additionalProperties": False,
                    },
                },
                "txid": {"type": "string", "description": "单笔简写：txid"},
                "tx_key": {"type": "string", "description": "单笔简写：tx_key"},
                "amount": {"type": "number", "description": "单笔简写：期望金额"},
                "label": {"type": "string", "description": "单笔简写：备注"},
            },
            "required": ["address", "view_key"],
            "additionalProperties": False,
        },
        "handler": t_verify_payment,
    },
    {
        "name": "xmr_merchant_report",
        "description": ("【商户对账·历史】按时间窗汇总某地址的收款流水：逐日笔数与金额、总额、"
                        "确认数、未解锁/待确认笔数、最常见金额分布。"
                        "输入 address + view_key + period_days（1–30）。"
                        "When to use：财务对账、月度流水、税务留痕、异常金额排查。"
                        "Do not use：单笔到账确认（用 xmr_verify_payment，秒级返回）。"
                        "Paid: x402 credit pack or API key."),
        "inputSchema": {
            "type": "object",
            "properties": {
                "address": {"type": "string", "description": "门罗收款地址"},
                "view_key": {"type": "string", "description": "只读私钥 view key（无法转账）"},
                "period_days": {"type": "integer", "description": "回溯天数，1–30，默认 7"},
                "include_pending": {"type": "boolean", "description": "是否含内存池未确认笔，默认 true"},
                "limit": {"type": "integer", "description": "返回明细条数上限，默认 500，上限 2000"},
            },
            "required": ["address", "view_key"],
            "additionalProperties": False,
        },
        "handler": t_merchant_report,
    },
    {
        "name": "xmr_mining_profitability",
        "description": ("【矿工决策】给定算力（H/s）与功耗/电价，算出日/月毛收益、净收益、"
                        "盈亏平衡电价与回本天数（数据来自本节点的链上网络口径）。"
                        "When to use：判断要不要开挖、买什么机器、电价谈到多少才划算。"
                        "Do not use：查询收益历史（本工具是前瞻测算，不是结算单）。"
                        "Paid: x402 credit pack or API key."),
        "inputSchema": {
            "type": "object",
            "properties": {
                "hashrate_hs": {"type": "number", "description": "算力，单位 H/s（例：100000 = 100 kH/s）"},
                "hashrate_khs": {"type": "number", "description": "算力的 kH/s 简写（与 hashrate_hs 二选一）"},
                "hashrate_mhs": {"type": "number", "description": "算力的 MH/s 简写"},
                "power_watts": {"type": "number", "description": "整机功耗（瓦），用于算电费"},
                "power_cost_kwh": {"type": "number", "description": "电价（美元/kWh）"},
                "service_fee_percent": {"type": "number",
                                        "description": "你所用挖矿服务的费率（%），默认 0；xmr.pub 不收取任何费率"},
                "xmr_price_usd": {"type": "number", "description": "XMR 价格（美元），填了才给美元净收益"},
                "hardware_cost_usd": {"type": "number", "description": "机器成本（美元），用于算回本天数"},
            },
            "additionalProperties": False,
        },
        "handler": t_mining_profitability,
    },
]
TOOL_BY_NAME = {t["name"]: t for t in TOOLS}


# ================= 参数预检（在扣费之前） =================
def _p_addr(v):
    return isinstance(v, str) and bool(XMR_ADDR_RE.match(v.strip()))


def _p_hex64(v):
    return isinstance(v, str) and bool(HEX64_RE.match(v.strip()))


def precheck(name, args):
    """扣信用之前做纯参数校验：客户端参数错误不该扣额度，也不该占用钱包通道。

    只校验「必填 + 格式」，与各工具自身的同名校验保持一致；返回错误串 = 应拒付执行，
    None = 放行（链上/钱包层面的校验仍在工具内部做）。
    """
    a = args if isinstance(args, dict) else {}
    if name == "xmr_verify_tx_proof":
        for k in ("txid", "address", "tx_key", "view_key"):
            if not str(a.get(k) or "").strip():
                return "缺少参数 %s" % k
        if not _p_hex64(a.get("txid")):
            return "txid 必须是 64 位十六进制"
        if not _p_hex64(a.get("tx_key")):
            return "tx_key 必须是 64 位十六进制"
        if not _p_hex64(a.get("view_key")):
            return "view_key 必须是 64 位十六进制"
        if not _p_addr(a.get("address")):
            return "address 不是合法的门罗主网地址"
    elif name == "xmr_verify_payment":
        if not _p_addr(a.get("address")):
            return "address 不是合法的门罗主网地址"
        if not _p_hex64(a.get("view_key")):
            return "view_key 必须是 64 位十六进制（只读密钥，无法转账）"
        pays = a.get("payments")
        if pays is None:
            if not _p_hex64(a.get("txid")):
                return "需要 payments 数组，或提供 txid/tx_key 单笔参数"
            if not _p_hex64(a.get("tx_key")):
                return "tx_key 必须是 64 位十六进制（付款方出示的 tx proof）"
        else:
            if not isinstance(pays, list) or not pays:
                return "payments 必须是非空数组"
            if len(pays) > 100:
                return "单次最多核验 100 笔（更多请分批或用 xmr_merchant_report）"
            for i, p in enumerate(pays, 1):
                if not isinstance(p, dict):
                    return "第 %d 笔格式不对" % i
                if not _p_hex64(p.get("txid")):
                    return "第 %d 笔的 txid 不是 64 位十六进制" % i
                if not _p_hex64(p.get("tx_key")):
                    return "第 %d 笔的 tx_key 不是 64 位十六进制（付款方出示的 tx proof）" % i
    elif name == "xmr_merchant_report":
        if not _p_addr(a.get("address")):
            return "address 不是合法的门罗主网地址"
        if not _p_hex64(a.get("view_key")):
            return "view_key 必须是 64 位十六进制（只读密钥，无法转账）"
        try:
            days = int(a.get("period_days", 7))
        except (TypeError, ValueError):
            return "period_days 必须是整数"
        if not 1 <= days <= 30:
            return "period_days 必须在 1–30 之间"
    elif name == "xmr_mining_profitability":
        raw = a.get("hashrate_hs")
        try:
            if raw in (None, ""):
                if a.get("hashrate_khs") not in (None, ""):
                    raw = float(a["hashrate_khs"]) * 1000.0
                elif a.get("hashrate_mhs") not in (None, ""):
                    raw = float(a["hashrate_mhs"]) * 1e6
            hs = float(raw) if raw not in (None, "") else 0.0
        except (TypeError, ValueError):
            return "hashrate 必须是数字"
        if hs <= 0:
            return "需要 hashrate_hs（或 hashrate_khs / hashrate_mhs），单位 H/s，且必须大于 0"
    return None


# ================= MCP / HTTP 层 =================
def _x402_networks_text():
    try:
        if _x402_enabled():
            return ", ".join(o.get("label") or o["network"] for o in x402_pay.options())
    except Exception:  # noqa: BLE001
        pass
    return "Base USDC"


def mcp_handle(msg, ctx=None):
    """处理一条 JSON-RPC 消息；通知类返回 None。ctx 携带计量上下文（不含任何工具参数）。"""
    method = msg.get("method")
    mid = msg.get("id")
    if mid is None:            # 通知，不需要回复
        return None
    if method == "initialize":
        want = ((msg.get("params") or {}).get("protocolVersion") or "")
        return {"jsonrpc": "2.0", "id": mid, "result": {
            "protocolVersion": want if want in SUPPORTED_PROTOCOLS else PROTOCOL_VERSION,
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
            "instructions": (
                "xmr.pub: read-only Monero MCP toolkit (7 tools; no custody, never signs or broadcasts). "
                "FREE, no auth: xmr_node_status, xmr_fee_oracle, xmr_mempool_health. "
                "PAID (x402 credit packs, USDC): xmr_verify_tx_proof, xmr_verify_payment, "
                "xmr_merchant_report, xmr_mining_profitability — 1 USDC = 100 calls or 0.1 USDC = 10 calls "
                "(" + _x402_networks_text() + "). Call a paid tool without payment to get the x402 "
                "PaymentRequired (accepts[]); retry with params._meta[\"x402/payment\"]. A settled payment "
                "returns an API key (X-API-Key) for the remaining credits. Arguments are never logged. "
                "免费：节点状态/费率/内存池；付费（x402 信用包）：tx-proof 对账、批量到账核验、收款流水、挖矿收益测算。"),
        }}
    if method == "ping":
        return {"jsonrpc": "2.0", "id": mid, "result": {}}
    if method == "tools/list":
        return {"jsonrpc": "2.0", "id": mid, "result": {
            "tools": [{"name": t["name"], "description": t["description"],
                       "inputSchema": t["inputSchema"]} for t in TOOLS]}}
    if method == "tools/call":
        params = msg.get("params") or {}
        name = params.get("name")
        tool = TOOL_BY_NAME.get(name)
        if not tool:
            return {"jsonrpc": "2.0", "id": mid,
                    "error": {"code": -32602, "message": "unknown tool: %s" % name}}

        # ---- 参数预检：参数错误直接拒，不扣额度、不进钱包 ----
        arg_err = precheck(name, params.get("arguments") or {})
        if arg_err:
            body = {"error": "invalid_arguments", "detail": arg_err}
            return {"jsonrpc": "2.0", "id": mid, "result": {
                "content": [{"type": "text", "text": json.dumps(body, ensure_ascii=False)}],
                "isError": True,
                "structuredContent": body,
            }}

        # ---- x402 门禁：仅付费工具；先结算/扣信用，再执行 ----
        # 顺序刻意如此：参数已预检通过；付款成功但执行失败 = 客户买的是额度、不退。
        x402_info = None
        if name in PAID_TOOLS and _x402_enabled():
            pay = ((params.get("_meta") or {}) or {}).get("x402/payment")
            raw_key = (ctx or {}).get("_raw_key") or ""
            if pay:
                if not isinstance(pay, dict):
                    return _payment_required(name, "x402 payment rejected: invalid_payload "
                                                   "(x402/payment must be an object)", mid)
                try:
                    res = x402_pay.try_pay(name, pay)
                except Exception as exc:  # noqa: BLE001  任何内部异常都回干净的 x402 错误，不再让进程掉包
                    print("x402 try_pay error: %r" % exc, flush=True)
                    res = {"ok": False, "reason": "server_error (%s)" % type(exc).__name__}
                if res.get("pending"):
                    # 已广播、未确认：绝不让客户重签（会重复扣款）。原样重发同一载荷即可拿到结果。
                    return {"jsonrpc": "2.0", "id": mid, "result": {
                        "isError": True,
                        "_meta": {"x402/payment-response": {"success": False, "errorReason": "settlement_pending",
                                                            "transaction": res.get("tx", ""),
                                                            "network": (pay.get("accepted") or {}).get("network", ""),
                                                            "payer": res.get("payer", "")}},
                        "structuredContent": {"x402Version": 2, "error": "settlement_pending",
                                              "transaction": res.get("tx", ""), "retryable": True,
                                              "action": "Do NOT sign a new payment. Resend this exact same request "
                                                        "(same x402/payment payload) in ~10 seconds."},
                        "content": [{"type": "text", "text": "Payment broadcast (tx %s) but not confirmed yet. Do NOT "
                                     "re-sign; resend the same request with the same payload in ~10s to receive "
                                     "credits and the tool result." % res.get("tx", "")}]}}
                if not res.get("ok"):
                    return _payment_required(
                        name, "x402 payment rejected: %s. Re-read accepts[] and sign a fresh "
                              "payment payload." % res.get("reason"), mid)
                left = x402_pay.spend(res["payer"], name)
                if left < 0:
                    return _payment_required(
                        name, "payer %s has no remaining credit; buy another pack." % res["payer"], mid)
                x402_info = {"payer": res["payer"], "credit_remaining": left, "creditRemaining": left,
                             "settled_tx": res.get("tx", ""), "api_key": res.get("key"),
                             "network": ((pay.get("accepted") or {}).get("network")
                                         or x402_pay.cfg().get("network") or "")}
                if res.get("key"):
                    # key 只在签发它的那一次回执里出现一次；给个前缀方便对账
                    x402_info["api_key_prefix"] = res["key"][:12]
            elif raw_key:
                ent = x402_pay.lookup_key(raw_key)
                if not ent:
                    return _payment_required(name, "unrecognized or expired x402 API key", mid)
                left = x402_pay.spend(ent["payer"], name)
                if left < 0:
                    return _payment_required(
                        name, "x402 key for %s has no remaining credit" % ent["payer"], mid)
                x402_info = {"payer": ent["payer"], "credit_remaining": left, "creditRemaining": left,
                             "network": ent.get("network", ""), "auth": "api_key"}
            else:
                return _payment_required(name, "payment required", mid)

        t0 = time.time()
        try:
            data = tool["handler"](params.get("arguments") or {})
            ok = not (isinstance(data, dict) and data.get("error"))
        except Exception as exc:
            data = {"error": "tool_failed", "detail": str(exc)[:200]}
            ok = False
        # 日志只记工具名与成败，绝不记参数（可能含 tx_key / view_key）
        ms = int((time.time() - t0) * 1000)
        print("tools/call %s -> %s (%dms)" % (name, "ok" if ok else "error", ms), flush=True)
        if ctx is not None:                       # 免费观察期的用量事实依据
            record_call(name, ok, ms, ctx.get("key_id"), ctx.get("ip_h"))
        result = {
            "content": [{"type": "text", "text": json.dumps(data, ensure_ascii=False, indent=2)}],
            "isError": not ok,
        }
        if x402_info:                     # 结算回执：本次扣了几次信用、链上 tx、签发的 key
            result["structuredContent"] = {"_x402": x402_info}
            # 付款结算与 API key 两条路径都回 payment-response（key 路径没有新交易，transaction 为空）
            result["_meta"] = {"x402/payment-response": {
                "success": True, "transaction": x402_info.get("settled_tx", "") or "",
                "network": x402_info.get("network", ""), "payer": x402_info.get("payer", ""),
                "creditRemaining": x402_info.get("credit_remaining"),
                "credit_remaining": x402_info.get("credit_remaining"),
                "auth": x402_info.get("auth", "x402_payment"),
                **({"apiKey": x402_info["api_key"]} if x402_info.get("api_key") else {})}}
            if isinstance(data, dict):
                # 面向 LLM/日志的文本块绝不放明文 key：只留前缀；明文只走 structuredContent
                data["_x402"] = {k: v for k, v in x402_info.items() if k != "api_key"}
                result["content"] = [{"type": "text",
                                      "text": json.dumps(data, ensure_ascii=False, indent=2)}]
        return {"jsonrpc": "2.0", "id": mid, "result": result}
    return {"jsonrpc": "2.0", "id": mid,
            "error": {"code": -32601, "message": "method not found: %s" % method}}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "xmrpub-mcp"

    def log_message(self, fmt, *args):
        pass                       # 用下面的显式日志，避免把请求行写进日志

    # ---- 计量上下文：只保留不可逆指纹，绝不记录原始 IP 或 API key ----
    def _ctx(self):
        ip = self.headers.get("CF-Connecting-IP") or self.client_address[0]
        key = self.headers.get("X-API-Key") or ""
        q = urllib.parse.urlparse(self.path).query
        if not key:
            key = (urllib.parse.parse_qs(q).get("key") or [""])[0]
        known = bool(key) and key in valid_keys()
        if key and not known:
            print("note: unrecognized X-API-Key (treated as anonymous)", flush=True)
        return {
            "ip_h": hashlib.sha256(("xmrpub-ip:" + ip).encode()).hexdigest()[:16],
            "key_id": (hashlib.sha256(("xmrpub-key:" + key).encode()).hexdigest()[:16]
                       if known else None),
            # 仅供 x402 结算 key 的本地校验使用；**不参与日志与计量**（record_call 只取 key_id/ip_h）
            "_raw_key": key,
        }

    def _send(self, code, obj, extra_headers=None):
        data = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers",
                         "Content-Type, Mcp-Session-Id, X-API-Key")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        for k, v in (extra_headers or ()):
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers",
                         "Content-Type, Mcp-Session-Id, X-API-Key")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        path = self.path.split("?")[0]
        if path in ("/healthz", "/mcp/healthz"):
            return self._send(200, {"ok": True, "server": SERVER_NAME, "version": SERVER_VERSION,
                                    "tools": len(TOOLS), "wallet_rpc": bool(WALLET_USER),
                                    "usage_db": os.path.exists(USAGE_DB)})
        if path in ("/stats", "/mcp/stats"):
            q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            try:
                hours = int((q.get("hours") or ["24"])[0])
            except ValueError:
                hours = 24
            s = usage_summary(hours)
            s["source"] = "usage.db（仅工具名/成败/耗时/key 指纹/截断 IP 哈希，不含任何参数）"
            s["server"] = SERVER_NAME
            s["version"] = SERVER_VERSION
            return self._send(200, s)
        # 边缘（nginx `location /mcp { proxy_pass http://mcp/; }`）会剥掉 /mcp 前缀，
        # 因此这里要同时认 "/mcp" 与上游看到的 ""/"/" 两种形态。
        if path in ("", "/", "/mcp", "/mcp/"):
            # MCP Streamable HTTP：本端点不提供 SSE 事件流，规范要求 GET 返回 405 而不是 404
            # （客户端靠状态码区分「端点不存在」与「端点存在但不支持 GET」）。
            return self._send(405, {
                "error": "method_not_allowed",
                "message": ("This is an MCP Streamable HTTP endpoint. POST JSON-RPC "
                            "(initialize / tools/list / tools/call) to https://xmr.pub/mcp. "
                            "No SSE stream is offered on GET."),
                "endpoint": "https://xmr.pub/mcp",
                "docs": "https://xmr.pub/#mcp",
                "source": "https://github.com/wygogogo19/xmr-pub-mcp",
                "registry": "pub.xmr/xmr-pub-mcp",
            }, extra_headers=[("Allow", "POST, OPTIONS")])
        return self._send(404, {"error": "not found", "see": "https://xmr.pub/#mcp"})

    def do_DELETE(self):
        # 本服务不使用会话 ID，也就没有可终止的会话；按规范允许回 405。
        self._send(405, {"error": "method_not_allowed",
                         "message": "This endpoint is stateless: no Mcp-Session-Id is issued, "
                                    "so there is no session to delete. POST JSON-RPC to /mcp."},
                   extra_headers=[("Allow", "POST, OPTIONS")])

    @staticmethod
    def _safe_handle(m, ctx):
        """任何未捕获异常都回 JSON-RPC -32603，而不是让连接断掉（边缘会把断连变成 502）。"""
        try:
            if not isinstance(m, dict):
                return {"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "invalid request"}}
            return mcp_handle(m, ctx)
        except Exception as exc:  # noqa: BLE001
            print("mcp_handle error: %r" % exc, flush=True)
            return {"jsonrpc": "2.0", "id": m.get("id") if isinstance(m, dict) else None,
                    "error": {"code": -32603, "message": "internal error"}}

    def do_POST(self):
        path = self.path.split("?")[0].rstrip("/") or "/"
        if path not in ("/", "/mcp"):
            return self._send(404, {"error": "not found"})
        ctx = self._ctx()
        bucket = ("key:" + ctx["key_id"]) if ctx["key_id"] else ("ip:" + ctx["ip_h"])
        limit = KEY_RATE_PER_MIN if ctx["key_id"] else ANON_RATE_PER_MIN
        allowed, retry = rate_ok(bucket, limit)
        if not allowed:
            self.send_response(429)
            self.send_header("Content-Type", "application/json")
            self.send_header("Retry-After", str(max(1, retry)))
            data = json.dumps({"jsonrpc": "2.0", "id": None, "error": {
                "code": -32029, "message": "rate limited",
                "data": {"limit_per_min": limit, "retry_after_seconds": max(1, retry),
                         "hint": "匿名 120 次/分/IP；带 X-API-Key 可到 600 次/分"}}},
                ensure_ascii=False).encode()
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        try:
            n = int(self.headers.get("Content-Length") or 0)
            if n <= 0 or n > 65536:
                return self._send(413, {"error": "bad_body"})
            raw = self.rfile.read(n)
            msg = json.loads(raw.decode("utf-8"))
        except Exception:
            return self._send(400, {"jsonrpc": "2.0", "id": None,
                                    "error": {"code": -32700, "message": "parse error"}})
        if isinstance(msg, list):                        # 批量
            out = [r for r in (self._safe_handle(m, ctx) for m in msg) if r is not None]
            return self._send(200 if out else 202, out if out else {})
        resp = self._safe_handle(msg, ctx)
        if resp is None:
            self.send_response(202)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        self._send(200, resp)


if __name__ == "__main__":
    _usage()                                        # 启动即建库，早暴露权限问题
    print("xmrpub-mcp %s on :%d  tools=%d  wallet_rpc=%s  usage_db=%s"
          % (SERVER_VERSION, PORT, len(TOOLS), "on" if WALLET_USER else "off",
             USAGE_DB if _usage_conn is not None else "unavailable"), flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()

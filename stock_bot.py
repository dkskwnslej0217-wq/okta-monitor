import os
import math
import requests
import pandas as pd
import yfinance as yf

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
FRED_API_KEY = os.getenv("FRED_API_KEY")
SHEET_ID = os.getenv("SHEET_ID")

DEFAULT_SETTINGS = {
    "total_asset": 7000000.0,
    "core_ratio": 0.60,
    "rotation_ratio": 0.30,
    "future_ratio": 0.10,
    "min_trade": 200000.0,
}

CORE_MIN_SHARES = {
    "PANW": 5,
    "CRWD": 1,
    "AMT": 1,
    "KMI": 1,
    "UNH": 1,
}

DEFAULT_WATCHLIST = [
    ("MSFT", "AI"),
    ("AMZN", "AI"),
    ("NVDA", "AI"),
    ("PANW", "Cyber"),
    ("CRWD", "Cyber"),
    ("V", "Payment"),
    ("COST", "Retail"),
    ("AMT", "Infra"),
    ("EQIX", "Infra"),
    ("MCK", "Healthcare"),
    ("UNH", "Healthcare"),
    ("KMI", "Energy"),
    ("CEG", "Energy"),
    ("GLD", "Macro"),
    ("TLT", "Bond"),
    ("COIN", "Crypto"),
    ("LUNR", "Space"),
    ("RKLB", "Space"),
    ("QBTS", "Quantum"),
    ("IONQ", "Quantum"),
    ("ROK", "Robotics"),
]

ROTATION_SECTORS = {
    "AI", "Cyber", "Payment", "Retail", "Infra",
    "Healthcare", "Energy", "Macro", "Bond", "Crypto"
}
FUTURE_SECTORS = {"Space", "Quantum", "Robotics"}

PAIR_MAP = {
    "PANW": "V",
    "CRWD": "KMI",
    "COIN": "TLT",
    "OKTA": "UNH",
    "ZS": "COST",
    "NVDA": "GLD",
}


def send_telegram(message: str) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(message)
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(
        url,
        data={"chat_id": TELEGRAM_CHAT_ID, "text": message},
        timeout=20,
    )


def normalize_columns(columns) -> list[str]:
    fixed = []
    for col in columns:
        c = str(col).strip().lower().replace(" ", "_")
        if c == "share":
            c = "shares"
        if c in ["avgprice", "avg_price_(won)", "avg_price_won"]:
            c = "avg_price"
        fixed.append(c)
    return fixed


def clean_number(value) -> float:
    s = str(value).strip()
    s = s.replace(",", "").replace("원", "").replace("$", "")
    if s == "" or s.lower() == "nan":
        return 0.0
    return float(s)


def sheet_csv_url(sheet_name: str) -> str:
    if not SHEET_ID:
        raise ValueError("SHEET_ID secret missing")
    return f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={sheet_name}"


def load_sheet(sheet_name: str) -> pd.DataFrame:
    return pd.read_csv(sheet_csv_url(sheet_name))


def load_portfolio() -> dict:
    df = load_sheet("PORTFOLIO")
    df.columns = normalize_columns(df.columns)

    required = {"ticker", "shares", "avg_price", "type"}
    if not required.issubset(set(df.columns)):
        raise ValueError("PORTFOLIO sheet columns must be: ticker, shares, avg_price, type")

    portfolio = {}
    for _, row in df.iterrows():
        ticker = str(row["ticker"]).strip().upper()
        if not ticker or ticker == "NAN":
            continue

        portfolio[ticker] = {
            "shares": int(clean_number(row["shares"])),
            "avg": clean_number(row["avg_price"]),
            "type": str(row["type"]).strip().lower(),
        }
    return portfolio


def load_settings() -> dict:
    try:
        df = load_sheet("SETTINGS")
        df.columns = normalize_columns(df.columns)

        settings = {}
        for _, row in df.iterrows():
            key = str(row["key"]).strip()
            settings[key] = row["value"]

        merged = DEFAULT_SETTINGS.copy()
        merged["total_asset"] = float(clean_number(settings.get("total_asset", merged["total_asset"])))
        merged["core_ratio"] = float(settings.get("core_ratio", merged["core_ratio"]))
        merged["rotation_ratio"] = float(settings.get("rotation_ratio", merged["rotation_ratio"]))
        merged["future_ratio"] = float(settings.get("future_ratio", merged["future_ratio"]))
        merged["min_trade"] = float(clean_number(settings.get("min_trade", merged["min_trade"])))
        return merged
    except Exception:
        return DEFAULT_SETTINGS.copy()


def load_watchlist() -> list[tuple[str, str]]:
    try:
        df = load_sheet("WATCHLIST")
        df.columns = normalize_columns(df.columns)
        out = []
        for _, row in df.iterrows():
            ticker = str(row["ticker"]).strip().upper()
            sector = str(row["sector"]).strip()
            if ticker and ticker != "NAN":
                out.append((ticker, sector))
        return out if out else DEFAULT_WATCHLIST
    except Exception:
        return DEFAULT_WATCHLIST


def get_history(ticker: str, period: str = "1y") -> tuple[pd.Series, pd.Series]:
    try:
        df = yf.download(
            ticker,
            period=period,
            interval="1d",
            progress=False,
            auto_adjust=False,
            threads=False,
        )
        if isinstance(df, pd.DataFrame) and not df.empty:
            close = df["Close"].squeeze().dropna()
            volume = df["Volume"].squeeze().dropna() if "Volume" in df else pd.Series(dtype=float)
            return close, volume
    except Exception:
        pass
    return pd.Series(dtype=float), pd.Series(dtype=float)


def calc_return(close: pd.Series, days: int):
    if len(close) < days + 1:
        return None
    return (float(close.iloc[-1]) / float(close.iloc[-days - 1]) - 1) * 100


def rsi(series: pd.Series, period: int = 14):
    if len(series) < period + 1:
        return None
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()
    rs = avg_gain / avg_loss
    out = 100 - (100 / (1 + rs))
    if len(out.dropna()) == 0:
        return None
    return float(out.dropna().iloc[-1])


def get_usdkrw() -> float:
    close, _ = get_history("USDKRW=X", "10d")
    if len(close) > 0:
        return float(close.iloc[-1])
    return 1350.0


def fred_series(series_id: str):
    if not FRED_API_KEY:
        return None
    try:
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": series_id,
            "api_key": FRED_API_KEY,
            "file_type": "json",
        }
        r = requests.get(url, params=params, timeout=20)
        data = r.json()
        vals = []
        for row in data.get("observations", []):
            v = row.get("value")
            if v == ".":
                continue
            vals.append(float(v))
        return vals if len(vals) >= 2 else None
    except Exception:
        return None


def compare_vs_benchmark(ticker: str, benchmark: str, period: str = "6mo") -> dict:
    a, _ = get_history(ticker, period=period)
    b, _ = get_history(benchmark, period=period)
    if len(a) < 61 or len(b) < 61:
        return {"20d": None, "60d": None}

    a20, b20 = calc_return(a, 20), calc_return(b, 20)
    a60, b60 = calc_return(a, 60), calc_return(b, 60)

    d20 = None if a20 is None or b20 is None else round(a20 - b20, 1)
    d60 = None if a60 is None or b60 is None else round(a60 - b60, 1)
    return {"20d": d20, "60d": d60}


def fundamental_status(ticker: str) -> str:
    try:
        info = yf.Ticker(ticker).info
        revenue_growth = info.get("revenueGrowth")
        earnings_growth = info.get("earningsQuarterlyGrowth")
        operating_margin = info.get("operatingMargins")
        gross_margin = info.get("grossMargins")

        score = 0

        if revenue_growth is not None:
            if revenue_growth > 0.10:
                score += 2
            elif revenue_growth > 0:
                score += 1
            else:
                score -= 2

        if earnings_growth is not None:
            if earnings_growth > 0:
                score += 2
            else:
                score -= 2

        if operating_margin is not None:
            if operating_margin > 0.15:
                score += 1
            elif operating_margin < 0:
                score -= 2

        if gross_margin is not None:
            if gross_margin > 0.50:
                score += 1
            elif gross_margin < 0.20:
                score -= 1

        if score >= 3:
            return "양호"
        if score >= 0:
            return "보통"
        return "약함"
    except Exception:
        return "보통"


def market_engine() -> tuple[str, int, list[str]]:
    score = 0
    notes = []

    spy, _ = get_history("SPY", "2y")
    qqq, _ = get_history("QQQ", "2y")
    vix, _ = get_history("^VIX", "3mo")

    if len(spy) >= 240:
        spy_ma200 = float(spy.rolling(200).mean().iloc[-1])
        spy_ma240 = float(spy.rolling(240).mean().iloc[-1])
        if float(spy.iloc[-1]) > spy_ma200:
            score += 2
            notes.append("SPY > 200MA")
        else:
            score -= 2
            notes.append("SPY < 200MA")

        if float(spy.iloc[-1]) > spy_ma240:
            score += 1
            notes.append("SPY > 240MA")
        else:
            score -= 1
            notes.append("SPY < 240MA")

    if len(qqq) >= 240:
        qqq_ma200 = float(qqq.rolling(200).mean().iloc[-1])
        qqq_ma240 = float(qqq.rolling(240).mean().iloc[-1])
        if float(qqq.iloc[-1]) > qqq_ma200:
            score += 2
            notes.append("QQQ > 200MA")
        else:
            score -= 2
            notes.append("QQQ < 200MA")

        if float(qqq.iloc[-1]) > qqq_ma240:
            score += 1
            notes.append("QQQ > 240MA")
        else:
            score -= 1
            notes.append("QQQ < 240MA")

    if len(vix) > 0:
        vv = float(vix.iloc[-1])
        if vv < 18:
            score += 1
            notes.append("VIX 안정")
        elif vv > 25:
            score -= 1
            notes.append("VIX 위험")
        else:
            notes.append("VIX 중간")

    if score >= 4:
        return "🟢 Risk ON", score, notes
    if score >= 1:
        return "🟡 Neutral", score, notes
    return "🔴 Risk OFF", score, notes


def liquidity_engine() -> tuple[str, float, list[str]]:
    score = 0.0
    notes = []

    tnx, _ = get_history("^TNX", "6mo")
    if len(tnx) >= 21:
        if float(tnx.iloc[-1]) < float(tnx.iloc[-21]):
            score += 1
            notes.append("10Y 하락")
        else:
            score -= 1
            notes.append("10Y 상승")

    tga = fred_series("WTREGEN")
    if tga:
        if tga[-1] < tga[-2]:
            score += 1
            notes.append("TGA 감소")
        else:
            score -= 1
            notes.append("TGA 증가")
    else:
        notes.append("TGA 없음")

    rrp = fred_series("RRPONTSYD")
    if rrp:
        if rrp[-1] < rrp[-2]:
            score += 1
            notes.append("RRP 감소")
        else:
            score -= 0.5
            notes.append("RRP 증가")
    else:
        notes.append("RRP 없음")

    hy = fred_series("BAMLH0A0HYM2")
    if hy:
        if hy[-1] < hy[-2]:
            score += 1
            notes.append("HY 축소")
        else:
            score -= 1
            notes.append("HY 확대")
    else:
        notes.append("HY 없음")

    if score >= 2:
        return "🟢 유동성 우호", score, notes
    if score >= 0:
        return "🟡 유동성 중립", score, notes
    return "🔴 유동성 부담", score, notes


def risk_engine(market_state: str, liquidity_state: str) -> tuple[str, dict]:
    score = 0
    if "Risk ON" in market_state:
        score += 1
    else:
        score -= 1

    if "우호" in liquidity_state:
        score += 1
    elif "부담" in liquidity_state:
        score -= 1

    if score >= 2:
        return "LOW", {"stock": 0.80, "bond": 0.10, "cash": 0.10}
    if score >= 0:
        return "MEDIUM", {"stock": 0.60, "bond": 0.25, "cash": 0.15}
    return "HIGH", {"stock": 0.40, "bond": 0.40, "cash": 0.20}


def sector_rotation_engine(market_state: str) -> list[str]:
    if "Risk ON" in market_state:
        return ["AI", "Cyber", "Payment", "Infra"]
    return ["Healthcare", "Energy", "Macro", "Bond"]


def snapshot(ticker: str, usdkrw: float, portfolio: dict) -> dict | None:
    close, _ = get_history(ticker, "2y")
    if len(close) == 0:
        return None

    price = float(close.iloc[-1])
    ma50 = float(close.rolling(50).mean().iloc[-1]) if len(close) >= 50 else None
    ma200 = float(close.rolling(200).mean().iloc[-1]) if len(close) >= 200 else None
    ma240 = float(close.rolling(240).mean().iloc[-1]) if len(close) >= 240 else None
    ma365 = float(close.rolling(365).mean().iloc[-1]) if len(close) >= 365 else None
    rr = rsi(close)

    qqq = compare_vs_benchmark(ticker, "QQQ")
    spy = compare_vs_benchmark(ticker, "SPY")

    shares = portfolio.get(ticker, {}).get("shares", 0)
    avg = portfolio.get(ticker, {}).get("avg")
    bucket = portfolio.get(ticker, {}).get("type", "")

    pnl_pct = None
    if avg and avg > 0:
        pnl_pct = round((price * usdkrw / avg - 1) * 100, 1)

    return {
        "ticker": ticker,
        "price_usd": round(price, 2),
        "price_krw": round(price * usdkrw),
        "ma50": ma50,
        "ma200": ma200,
        "ma240": ma240,
        "ma365": ma365,
        "rsi": rr,
        "shares": shares,
        "avg": avg,
        "type": bucket,
        "pnl_pct": pnl_pct,
        "qqq20": qqq["20d"],
        "qqq60": qqq["60d"],
        "spy20": spy["20d"],
        "spy60": spy["60d"],
        "fundamental": fundamental_status(ticker),
    }


def grade_from_score(score: int) -> str:
    if score >= 70:
        return "A+ 강력매수"
    if score >= 50:
        return "A 매수"
    if score >= 30:
        return "B 관망"
    return "C 제외"


def rate_candidate(ticker: str, sector: str, usdkrw: float, portfolio: dict, preferred_sectors: list[str]) -> dict | None:
    info = snapshot(ticker, usdkrw, portfolio)
    if not info:
        return None

    score = 0
    reasons = []

    if info["ma365"] and info["price_usd"] > info["ma365"]:
        score += 15
        reasons.append("365일선 위")
    elif info["ma365"]:
        score -= 10
        reasons.append("365일선 아래")

    if info["ma240"] and info["price_usd"] > info["ma240"]:
        score += 10
        reasons.append("240일선 위")
    elif info["ma240"]:
        score -= 8
        reasons.append("240일선 아래")

    if info["ma200"] and info["price_usd"] > info["ma200"]:
        score += 8
        reasons.append("200일선 위")
    elif info["ma200"]:
        score += 10
        reasons.append("200일선 아래 눌림")

    if info["ma50"] and info["price_usd"] < info["ma50"]:
        score += 8
        reasons.append("50일선 아래 눌림")

    if info["rsi"] is not None:
        if info["rsi"] < 35:
            score += 15
            reasons.append("RSI 과매도")
        elif info["rsi"] < 45:
            score += 8
            reasons.append("RSI 중립하단")
        elif info["rsi"] > 72:
            score -= 8
            reasons.append("RSI 과열")

    if info["qqq20"] is not None:
        if info["qqq20"] >= 2:
            score += 8
            reasons.append("QQQ 대비 강함")
        elif info["qqq20"] <= -8:
            score -= 8
            reasons.append("QQQ 대비 약함")

    if info["spy20"] is not None and info["spy20"] >= 2:
        score += 5
        reasons.append("SPY 대비 강함")

    if info["fundamental"] == "양호":
        score += 15
        reasons.append("실적 양호")
    elif info["fundamental"] == "보통":
        score += 5
        reasons.append("실적 보통")
    else:
        score -= 15
        reasons.append("실적 약함")

    if sector in preferred_sectors:
        score += 10
        reasons.append("선호 섹터")

    return {
        "ticker": ticker,
        "sector": sector,
        "score": score,
        "grade": grade_from_score(score),
        "price_krw": info["price_krw"],
        "info": info,
        "reason": ", ".join(reasons[:5]),
    }


def build_scanner(usdkrw: float, portfolio: dict, watchlist: list[tuple[str, str]], preferred_sectors: list[str]) -> list[dict]:
    recs = []
    for ticker, sector in watchlist:
        item = rate_candidate(ticker, sector, usdkrw, portfolio, preferred_sectors)
        if item:
            recs.append(item)

    for src, dst in PAIR_MAP.items():
        src_info = snapshot(src, usdkrw, portfolio)
        dst_info = snapshot(dst, usdkrw, portfolio)
        if not src_info or not dst_info:
            continue

        src_hot = src_info["rsi"] is not None and src_info["rsi"] >= 68
        dst_dip = dst_info["ma200"] is not None and dst_info["price_usd"] < dst_info["ma200"]

        if src_hot and dst_dip:
            for r in recs:
                if r["ticker"] == dst:
                    r["score"] += 12
                    r["grade"] = grade_from_score(r["score"])
                    r["reason"] = f"{src} 과열 → {dst} 눌림"

    recs = sorted(recs, key=lambda x: x["score"], reverse=True)

    seen = set()
    final = []
    for r in recs:
        if r["ticker"] in seen:
            continue
        seen.add(r["ticker"])
        final.append(r)

    return final


def current_value_krw(ticker: str, usdkrw: float, portfolio: dict) -> int:
    info = snapshot(ticker, usdkrw, portfolio)
    if not info:
        return 0
    return info["shares"] * info["price_krw"]


def rebalance_engine(portfolio: dict, usdkrw: float) -> dict:
    values = {}
    total = 0

    for ticker, data in portfolio.items():
        info = snapshot(ticker, usdkrw, portfolio)
        if not info:
            continue
        value = info["shares"] * info["price_krw"]
        values[ticker] = value
        total += value

    weights = {}
    if total <= 0:
        return weights

    for ticker, value in values.items():
        weights[ticker] = round(value / total, 4)
    return weights


def build_execution_plan(
    usdkrw: float,
    market_state: str,
    liquidity_state: str,
    portfolio: dict,
    settings: dict,
    watchlist: list[tuple[str, str]],
    preferred_sectors: list[str],
):
    sells = []
    buys = []
    holds = []

    total_asset = settings["total_asset"]
    min_trade = settings["min_trade"]

    bucket_targets = {
        "core": total_asset * settings["core_ratio"],
        "rotation": total_asset * settings["rotation_ratio"],
        "future": total_asset * settings["future_ratio"],
    }

    risk_level, risk_alloc = risk_engine(market_state, liquidity_state)

    bucket_current = {"core": 0, "rotation": 0, "future": 0}
    for ticker in portfolio.keys():
        info = snapshot(ticker, usdkrw, portfolio)
        if not info:
            continue
        bucket = info["type"]
        if bucket in bucket_current:
            bucket_current[bucket] += info["shares"] * info["price_krw"]

    weights = rebalance_engine(portfolio, usdkrw)

    for ticker in portfolio.keys():
        info = snapshot(ticker, usdkrw, portfolio)
        if not info or info["shares"] <= 0:
            continue

        sellable = info["shares"]
        if ticker in CORE_MIN_SHARES:
            sellable = max(0, info["shares"] - CORE_MIN_SHARES[ticker])

        qty = 0
        reason = None

        # 코어 보호
        is_core = portfolio[ticker]["type"] == "core"

        # 1) 실적 약하고 240/365 둘 다 아래면 감축
        weak_long_trend = (
            info["ma240"] is not None and info["price_usd"] < info["ma240"] and
            info["ma365"] is not None and info["price_usd"] < info["ma365"]
        )

        if weak_long_trend and info["fundamental"] == "약함":
            qty = min(sellable, max(1, math.floor(info["shares"] * 0.20)))
            reason = "장기 추세 약화 + 실적 약함"

        # 2) 과열 차익: 코어는 1주만, 비코어는 20%
        elif info["rsi"] is not None and info["rsi"] >= 74 and (info["pnl_pct"] or 0) >= 8:
            if is_core:
                qty = min(sellable, 1)
                if qty > 0:
                    reason = "과열 차익 1주 검토"
            else:
                qty = min(sellable, max(1, math.floor(info["shares"] * 0.20)))
                reason = "과열 차익"

        # 3) 비코어만 약세 감축
        elif (not is_core) and info["ma200"] and info["price_usd"] < info["ma200"] and info["qqq60"] is not None and info["qqq60"] <= -8:
            if info["fundamental"] == "약함":
                qty = min(sellable, 1)
                reason = "약세 감축"
            else:
                holds.append(f"{ticker} 기술 약세지만 실적 {info['fundamental']} → 관망")

        # 4) 비중 과다 조절
        if ticker in weights and weights[ticker] > 0.22 and sellable > 0:
            qty = max(qty, 1)
            reason = reason or "비중 과다 조절"

        # 5) 손실 문구는 보수적으로
        if qty == 0:
            if info["pnl_pct"] is not None and info["pnl_pct"] <= -20 and info["fundamental"] != "약함":
                holds.append(f"{ticker} 손실 구간이지만 실적 {info['fundamental']} → 관망")
            elif weak_long_trend and info["fundamental"] != "약함":
                holds.append(f"{ticker} 장기 추세 약화지만 실적 {info['fundamental']} → 관망")

        if qty > 0 and reason:
            sells.append({
                "ticker": ticker,
                "qty": qty,
                "amount": round(qty * info["price_krw"]),
                "reason": reason,
            })

    total_sell = sum(x["amount"] for x in sells)
    scanner = build_scanner(usdkrw, portfolio, watchlist, preferred_sectors)

    if risk_level == "HIGH":
        scanner = [
            x for x in scanner
            if x["grade"] == "A+ 강력매수" and x["sector"] in {"Bond", "Macro", "Healthcare"}
        ]
    else:
        scanner = [x for x in scanner if x["grade"] in {"A+ 강력매수", "A 매수"}]

    remaining = total_sell

    for item in scanner:
        ticker = item["ticker"]
        sector = item["sector"]

        # 이미 충분히 들고 있으면 패스
        existing_value = current_value_krw(ticker, usdkrw, portfolio)
        if existing_value > total_asset * 0.08:
            continue

        if sector in FUTURE_SECTORS:
            bucket = "future"
            single_cap = total_asset * 0.03
        else:
            bucket = "rotation"
            single_cap = total_asset * 0.08

        bucket_gap = max(0, bucket_targets[bucket] - bucket_current[bucket])
        stock_gap = max(0, single_cap - existing_value)
        buy_budget = min(bucket_gap, stock_gap, remaining)

        if buy_budget <= 0:
            continue

        qty = math.floor(buy_budget / max(item["price_krw"], 1))
        if qty <= 0:
            continue

        amount = qty * item["price_krw"]
        if amount < min_trade and remaining >= min_trade:
            continue

        buys.append({
            "ticker": ticker,
            "qty": qty,
            "amount": round(amount),
            "grade": item["grade"],
            "sector": sector,
            "fundamental": item["info"]["fundamental"],
        })

        remaining -= amount
        bucket_current[bucket] += amount

    return risk_level, risk_alloc, sells, buys, holds[:10], round(total_sell), round(sum(x["amount"] for x in buys)), round(remaining)


def header_message(usdkrw: float):
    market_state, market_score, market_notes = market_engine()
    liquidity_state, liquidity_score, liquidity_notes = liquidity_engine()
    risk_level, risk_alloc = risk_engine(market_state, liquidity_state)

    lines = []
    lines.append("📊 AI 자산운용 시스템 v16")
    lines.append("")
    lines.append(f"- 환율: {round(usdkrw):,}원")
    lines.append(f"- 시장: {market_state} | 점수 {market_score}")
    lines.append(f"- 유동성: {liquidity_state} | 점수 {liquidity_score}")
    lines.append(f"- 리스크: {risk_level}")
    lines.append(f"- 권장 비중: 주식 {int(risk_alloc['stock']*100)}% / 채권 {int(risk_alloc['bond']*100)}% / 현금 {int(risk_alloc['cash']*100)}%")
    lines.append("")
    lines.append("핵심")
    for x in (market_notes[:4] + liquidity_notes[:3]):
        lines.append(f"- {x}")

    return "\n".join(lines), market_state, liquidity_state


def portfolio_state_message(portfolio: dict) -> str:
    lines = []
    lines.append("🗂 시트에서 읽은 포트폴리오")
    lines.append("")
    for ticker, data in portfolio.items():
        lines.append(f"- {ticker} | {data['shares']}주 | 평단 {round(data['avg'])}원 | {data['type']}")
    return "\n".join(lines)


def sector_message(preferred_sectors: list[str]) -> str:
    lines = []
    lines.append("🔄 섹터 로테이션")
    lines.append("")
    for sector in preferred_sectors:
        lines.append(f"- {sector}")
    return "\n".join(lines)


def scanner_message(scanner: list[dict]) -> str:
    lines = []
    lines.append("🚀 AI 종목 스캐너")
    lines.append("")
    if not scanner:
        lines.append("- 스캔 결과 없음")
        return "\n".join(lines)

    for item in scanner[:10]:
        lines.append(
            f"- {item['ticker']} | {item['sector']} | {item['grade']} | {item['score']}점 | 약 {item['price_krw']:,}원"
        )
    return "\n".join(lines)


def execution_message(
    usdkrw: float,
    market_state: str,
    liquidity_state: str,
    portfolio: dict,
    settings: dict,
    watchlist: list[tuple[str, str]],
    preferred_sectors: list[str],
):
    risk_level, risk_alloc, sells, buys, holds, total_sell, total_buy, remain = build_execution_plan(
        usdkrw, market_state, liquidity_state, portfolio, settings, watchlist, preferred_sectors
    )

    if total_sell < settings["min_trade"] and total_buy < settings["min_trade"]:
        return None

    lines = []
    lines.append("📌 오늘 실행 필요")
    lines.append("")
    lines.append(f"- 리스크: {risk_level}")
    lines.append(f"- 총 매도 예정: {total_sell:,}원")
    lines.append(f"- 총 매수 예정: {total_buy:,}원")
    lines.append(f"- 실행 후 예상 잔여현금: {remain:,}원")
    lines.append("")

    lines.append("[매도/감축 검토]")
    if sells:
        for s in sells:
            lines.append(f"- {s['ticker']} {s['qty']}주 | 약 {s['amount']:,}원 | {s['reason']}")
    else:
        lines.append("- 없음")

    lines.append("")
    lines.append("[매수 검토]")
    if buys:
        for b in buys:
            lines.append(f"- {b['ticker']} {b['qty']}주 | 약 {b['amount']:,}원 | {b['grade']} | {b['sector']} | 실적 {b['fundamental']}")
    else:
        lines.append("- 없음")

    lines.append("")
    lines.append("[관망/유지]")
    if holds:
        for h in holds[:8]:
            lines.append(f"- {h}")
    else:
        lines.append("- 없음")

    return "\n".join(lines)


def main():
    portfolio = load_portfolio()
    settings = load_settings()
    watchlist = load_watchlist()
    usdkrw = get_usdkrw()

    header, market_state, liquidity_state = header_message(usdkrw)
    preferred_sectors = sector_rotation_engine(market_state)
    scanner = build_scanner(usdkrw, portfolio, watchlist, preferred_sectors)

    send_telegram(header)
    send_telegram(portfolio_state_message(portfolio))
    send_telegram(sector_message(preferred_sectors))
    send_telegram(scanner_message(scanner))

    exe = execution_message(
        usdkrw,
        market_state,
        liquidity_state,
        portfolio,
        settings,
        watchlist,
        preferred_sectors,
    )
    if exe:
        send_telegram(exe)


if __name__ == "__main__":
    main()

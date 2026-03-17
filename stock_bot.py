import os
import math
import requests
import yfinance as yf
import pandas as pd

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
FRED_API_KEY = os.getenv("FRED_API_KEY")
SHEET_ID = os.getenv("SHEET_ID")

# 코어 최소 보유
CORE_MIN_SHARES = {
    "PANW": 5,
    "CRWD": 1,
    "AMT": 1,
    "KMI": 1,
    "UNH": 1,
    "MSFT": 0,
    "AMZN": 0,
    "V": 0,
    "COST": 0,
}

# 장기적으로 가져갈 코어 후보
CORE_WISHLIST = {"MSFT", "AMZN", "PANW", "CRWD", "UNH", "V", "AMT", "KMI", "COST"}

# 반대 성향 로테이션
PAIR_MAP = {
    "PANW": "V",
    "CRWD": "KMI",
    "COIN": "TLT",
    "OKTA": "UNH",
    "ZS": "COST",
    "MSFT": "AMT",
    "NVDA": "GLD",
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
]

ROTATION_SECTORS = {"AI", "Cyber", "Payment", "Retail", "Infra", "Healthcare", "Energy", "Macro", "Bond", "Crypto"}
FUTURE_SECTORS = {"Space", "Quantum", "Robotics"}

def send_telegram(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(msg)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=20)

def sheet_csv_url(sheet_name: str) -> str:
    return f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={sheet_name}"

def normalize_columns(cols):
    out = []
    for c in cols:
        s = str(c).strip().lower().replace(" ", "_")
        if s == "share":
            s = "shares"
        if s in ["avgprice", "avg_price_(won)", "avg_price_won"]:
            s = "avg_price"
        out.append(s)
    return out

def clean_number(v):
    s = str(v).strip().replace(",", "").replace("원", "").replace("$", "")
    return float(s)

def load_sheet(sheet_name: str) -> pd.DataFrame:
    if not SHEET_ID:
        raise ValueError("SHEET_ID secret missing")
    return pd.read_csv(sheet_csv_url(sheet_name))

def load_portfolio():
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
            "type": str(row["type"]).strip().lower()
        }
    return portfolio

def load_settings():
    try:
        df = load_sheet("SETTINGS")
        df.columns = normalize_columns(df.columns)
        settings = {}
        for _, row in df.iterrows():
            settings[str(row["key"]).strip()] = row["value"]
        return {
            "total_asset": float(clean_number(settings.get("total_asset", 7000000))),
            "core_ratio": float(settings.get("core_ratio", 0.60)),
            "rotation_ratio": float(settings.get("rotation_ratio", 0.30)),
            "future_ratio": float(settings.get("future_ratio", 0.10)),
            "min_trade": float(clean_number(settings.get("min_trade", 200000))),
        }
    except Exception:
        return {
            "total_asset": 7000000.0,
            "core_ratio": 0.60,
            "rotation_ratio": 0.30,
            "future_ratio": 0.10,
            "min_trade": 200000.0,
        }

def load_watchlist():
    try:
        df = load_sheet("WATCHLIST")
        df.columns = normalize_columns(df.columns)
        items = []
        for _, row in df.iterrows():
            ticker = str(row["ticker"]).strip().upper()
            sector = str(row["sector"]).strip()
            if ticker and ticker != "NAN":
                items.append((ticker, sector))
        return items if items else DEFAULT_WATCHLIST
    except Exception:
        return DEFAULT_WATCHLIST

def get_history(ticker: str, period="1y"):
    df = yf.download(ticker, period=period, interval="1d", progress=False, auto_adjust=False, threads=False)
    if isinstance(df, pd.DataFrame) and not df.empty:
        close = df["Close"].squeeze().dropna()
        volume = df["Volume"].squeeze().dropna() if "Volume" in df else pd.Series(dtype=float)
        return close, volume
    return pd.Series(dtype=float), pd.Series(dtype=float)

def rsi(series: pd.Series, period: int = 14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def calc_return(close: pd.Series, days: int):
    if len(close) < days + 1:
        return None
    return (float(close.iloc[-1]) / float(close.iloc[-days - 1]) - 1) * 100

def get_usdkrw():
    try:
        close, _ = get_history("USDKRW=X", period="10d")
        if len(close) > 0:
            return round(float(close.iloc[-1]), 2)
    except Exception:
        pass
    return 1350.0

def fred_series(series_id: str):
    if not FRED_API_KEY:
        return None
    try:
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {"series_id": series_id, "api_key": FRED_API_KEY, "file_type": "json"}
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

def compare_vs_benchmark(ticker: str, benchmark: str, period="6mo"):
    try:
        a, _ = get_history(ticker, period=period)
        b, _ = get_history(benchmark, period=period)
        if len(a) < 61 or len(b) < 61:
            return {"20d": None, "60d": None}
        a20, b20 = calc_return(a, 20), calc_return(b, 20)
        a60, b60 = calc_return(a, 60), calc_return(b, 60)
        d20 = None if a20 is None or b20 is None else round(a20 - b20, 1)
        d60 = None if a60 is None or b60 is None else round(a60 - b60, 1)
        return {"20d": d20, "60d": d60}
    except Exception:
        return {"20d": None, "60d": None}

def fundamental_status(ticker: str):
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
        elif score >= 0:
            return "보통"
        else:
            return "약함"
    except Exception:
        return "보통"

def market_engine():
    score = 0
    notes = []
    try:
        spy, _ = get_history("SPY", "1y")
        qqq, _ = get_history("QQQ", "1y")
        vix, _ = get_history("^VIX", "3mo")

        if len(spy) >= 200:
            if float(spy.iloc[-1]) > float(spy.rolling(200).mean().iloc[-1]):
                score += 2
                notes.append("SPY > 200MA")
            else:
                score -= 2
                notes.append("SPY < 200MA")

        if len(qqq) >= 200:
            if float(qqq.iloc[-1]) > float(qqq.rolling(200).mean().iloc[-1]):
                score += 2
                notes.append("QQQ > 200MA")
            else:
                score -= 2
                notes.append("QQQ < 200MA")

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
    except Exception:
        notes.append("시장데이터 오류")

    if score >= 3:
        return "🟢 Risk ON", score, notes
    elif score >= 1:
        return "🟡 Neutral", score, notes
    else:
        return "🔴 Risk OFF", score, notes

def liquidity_engine():
    score = 0
    notes = []

    try:
        tnx, _ = get_history("^TNX", "6mo")
        if len(tnx) >= 21:
            if float(tnx.iloc[-1]) < float(tnx.iloc[-21]):
                score += 1
                notes.append("10Y 하락")
            else:
                score -= 1
                notes.append("10Y 상승")
    except Exception:
        notes.append("10Y 오류")

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
        return "🟢 유동성 우호", round(score, 1), notes
    elif score >= 0:
        return "🟡 유동성 중립", round(score, 1), notes
    else:
        return "🔴 유동성 부담", round(score, 1), notes

def risk_engine(market_state: str, liquidity_state: str):
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
    elif score >= 0:
        return "MEDIUM", {"stock": 0.60, "bond": 0.25, "cash": 0.15}
    else:
        return "HIGH", {"stock": 0.40, "bond": 0.40, "cash": 0.20}

def sector_rotation_engine(market_state: str):
    if "Risk ON" in market_state:
        return ["AI", "Cyber", "Payment", "Infra"]
    return ["Healthcare", "Energy", "Macro", "Bond"]

def snapshot(ticker: str, usdkrw: float, portfolio: dict):
    close, _ = get_history(ticker, period="1y")
    if len(close) == 0:
        return None

    price = float(close.iloc[-1])
    ma50 = float(close.rolling(50).mean().iloc[-1]) if len(close) >= 50 else None
    ma200 = float(close.rolling(200).mean().iloc[-1]) if len(close) >= 200 else None
    rr = float(rsi(close).iloc[-1]) if len(close) >= 20 else None
    qqq = compare_vs_benchmark(ticker, "QQQ")
    spy = compare_vs_benchmark(ticker, "SPY")
    shares = portfolio.get(ticker, {}).get("shares", 0)
    avg = portfolio.get(ticker, {}).get("avg")
    bucket = portfolio.get(ticker, {}).get("type", "")
    pnl_pct = None if avg is None else round((price / avg - 1) * 100, 1)
    f_status = fundamental_status(ticker)

    return {
        "ticker": ticker,
        "price": round(price, 2),
        "price_krw": round(price * usdkrw),
        "ma50": ma50,
        "ma200": ma200,
        "rsi": rr,
        "shares": shares,
        "avg": avg,
        "type": bucket,
        "pnl_pct": pnl_pct,
        "qqq20": qqq["20d"],
        "qqq60": qqq["60d"],
        "spy20": spy["20d"],
        "spy60": spy["60d"],
        "fundamental": f_status,
    }

def grade_from_score(score: int):
    if score >= 60:
        return "A+ 강력매수"
    elif score >= 45:
        return "A 매수"
    elif score >= 25:
        return "B 관망"
    else:
        return "C 제외"

def rate_candidate(ticker: str, sector: str, usdkrw: float, portfolio: dict, preferred_sectors: list):
    info = snapshot(ticker, usdkrw, portfolio)
    if not info:
        return None

    score = 0
    reasons = []

    if info["ma200"] and info["price"] < info["ma200"]:
        score += 15
        reasons.append("200일선 아래")
    else:
        score += 5
        reasons.append("200일선 위")

    if info["ma50"] and info["price"] < info["ma50"]:
        score += 10
        reasons.append("50일선 아래 눌림")

    if info["rsi"] is not None:
        if info["rsi"] < 35:
            score += 20
            reasons.append("RSI 과매도")
        elif info["rsi"] < 45:
            score += 10
            reasons.append("RSI 중립하단")

    if info["qqq20"] is not None:
        if info["qqq20"] >= 2:
            score += 10
            reasons.append("QQQ 대비 강함")
        elif info["qqq20"] <= -8:
            score -= 10

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
        "reason": ", ".join(reasons[:5]),
        "price_krw": info["price_krw"],
        "info": info,
    }

def build_scanner(usdkrw: float, portfolio: dict, watchlist: list, preferred_sectors: list):
    recs = []
    for ticker, sector in watchlist:
        r = rate_candidate(ticker, sector, usdkrw, portfolio, preferred_sectors)
        if r:
            recs.append(r)

    for src, dst in PAIR_MAP.items():
        src_info = snapshot(src, usdkrw, portfolio)
        dst_info = snapshot(dst, usdkrw, portfolio)
        if not src_info or not dst_info:
            continue
        src_hot = src_info["rsi"] is not None and src_info["rsi"] >= 68
        dst_dip = dst_info["ma200"] and dst_info["price"] < dst_info["ma200"]
        if src_hot and dst_dip:
            for r in recs:
                if r["ticker"] == dst:
                    r["score"] += 15
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

def current_value_krw(ticker: str, usdkrw: float, portfolio: dict):
    info = snapshot(ticker, usdkrw, portfolio)
    if not info:
        return 0
    return info["shares"] * info["price_krw"]

def rebalance_engine(portfolio: dict, usdkrw: float):
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

def build_execution_plan(usdkrw: float, market_state: str, liquidity_state: str, portfolio: dict, settings: dict, watchlist: list, preferred_sectors: list):
    sells, buys, holds = [], [], []

    total_asset = settings["total_asset"]
    core_ratio = settings["core_ratio"]
    rotation_ratio = settings["rotation_ratio"]
    future_ratio = settings["future_ratio"]
    min_trade = settings["min_trade"]

    risk_level, risk_alloc = risk_engine(market_state, liquidity_state)

    bucket_targets = {
        "core": total_asset * core_ratio,
        "rotation": total_asset * rotation_ratio,
        "future": total_asset * future_ratio,
    }

    bucket_current = {"core": 0, "rotation": 0, "future": 0}
    for ticker in portfolio.keys():
        info = snapshot(ticker, usdkrw, portfolio)
        if not info:
            continue
        bucket = info["type"]
        if bucket in bucket_current:
            bucket_current[bucket] += info["shares"] * info["price_krw"]

    current_weights = rebalance_engine(portfolio, usdkrw)

    for ticker in portfolio.keys():
        info = snapshot(ticker, usdkrw, portfolio)
        if not info or info["shares"] <= 0:
            continue

        sellable = info["shares"]
        if ticker in CORE_MIN_SHARES:
            sellable = max(0, info["shares"] - CORE_MIN_SHARES[ticker])

        qty = 0
        reason = None

        # 손실인데 실적 약하면 정리
        if info["pnl_pct"] is not None and info["pnl_pct"] <= -20:
            if info["fundamental"] == "약함":
                qty = min(sellable, max(1, math.floor(info["shares"] * 0.20)))
                reason = "손실 정리"
            else:
                holds.append(f"{ticker} 손실이지만 실적 {info['fundamental']} → 관망")

        # 과열 차익
        elif info["rsi"] is not None and info["rsi"] >= 72 and (info["pnl_pct"] or 0) > 5:
            qty = min(sellable, max(1, math.floor(info["shares"] * 0.20)))
            reason = "과열 차익"

        # 약세 감축
        elif info["ma200"] and info["price"] < info["ma200"] and info["qqq60"] is not None and info["qqq60"] <= -8:
            if info["fundamental"] == "약함":
                qty = min(sellable, 1)
                reason = "약세 감축"
            else:
                holds.append(f"{ticker} 기술 약세지만 실적 {info['fundamental']} → 관망")

        # 비중 과다 자동 감축
        if ticker in current_weights and current_weights[ticker] > 0.20 and sellable > 0:
            qty = max(qty, 1)
            reason = reason or "비중 과다 조절"

        if qty > 0 and reason:
            sells.append({
                "ticker": ticker,
                "qty": qty,
                "amount": round(qty * info["price_krw"]),
                "reason": reason,
            })

    total_sell = sum(x["amount"] for x in sells)
    recs = build_scanner(usdkrw, portfolio, watchlist, preferred_sectors)

    if risk_level == "HIGH":
        recs = [x for x in recs if x["grade"] == "A+ 강력매수" and x["sector"] in {"Bond", "Macro", "Healthcare"}]
    elif risk_level == "MEDIUM":
        recs = [x for x in recs if x["grade"] in ["A+ 강력매수", "A 매수"]]
    else:
        recs = [x for x in recs if x["grade"] in ["A+ 강력매수", "A 매수"]]

    remaining = total_sell

    for r in recs:
        ticker = r["ticker"]
        sector = r["sector"]

        if sector in FUTURE_SECTORS:
            target_bucket = "future"
        elif sector in ROTATION_SECTORS:
            target_bucket = "rotation"
        else:
            target_bucket = "rotation"

        bucket_gap = max(0, bucket_targets[target_bucket] - bucket_current[target_bucket])
        single_cap = total_asset * (0.08 if target_bucket == "rotation" else 0.03)
        current_val = current_value_krw(ticker, usdkrw, portfolio)
        stock_gap = max(0, single_cap - current_val)
        buy_budget = min(bucket_gap, stock_gap, remaining)

        price_krw = r["price_krw"]
        qty = math.floor(buy_budget / max(price_krw, 1))

        if qty <= 0:
            continue

        amount = qty * price_krw
        if amount < min_trade and remaining >= min_trade:
            continue

        remaining -= amount
        bucket_current[target_bucket] += amount

        buys.append({
            "ticker": ticker,
            "qty": qty,
            "amount": round(amount),
            "grade": r["grade"],
            "fundamental": r["info"]["fundamental"],
            "sector": sector,
        })

    return risk_level, risk_alloc, sells, buys, holds[:10], round(total_sell), round(sum(x["amount"] for x in buys)), round(remaining)

def header_message(usdkrw: float):
    market_state, market_score, market_notes = market_engine()
    liquidity_state, liquidity_score, liquidity_notes = liquidity_engine()
    risk_level, risk_alloc = risk_engine(market_state, liquidity_state)

    lines = []
    lines.append("📊 AI 자산운용 시스템 v10")
    lines.append("")
    lines.append(f"- 환율: {usdkrw:,.0f}원")
    lines.append(f"- 시장: {market_state} | 점수 {market_score}")
    lines.append(f"- 유동성: {liquidity_state} | 점수 {liquidity_score}")
    lines.append(f"- 리스크: {risk_level}")
    lines.append(f"- 권장 비중: 주식 {int(risk_alloc['stock']*100)}% / 채권 {int(risk_alloc['bond']*100)}% / 현금 {int(risk_alloc['cash']*100)}%")
    lines.append("")
    lines.append("핵심")
    for x in (market_notes[:2] + liquidity_notes[:3]):
        lines.append(f"- {x}")

    return "\n".join(lines), market_state, liquidity_state

def portfolio_state_message(portfolio: dict):
    lines = []
    lines.append("🗂 시트에서 읽은 포트폴리오")
    lines.append("")
    for ticker, data in portfolio.items():
        lines.append(f"- {ticker} | {data['shares']}주 | 평단 {round(data['avg'])}원 | {data['type']}")
    return "\n".join(lines)

def sector_message(preferred_sectors: list):
    lines = []
    lines.append("🔄 섹터 로테이션")
    lines.append("")
    for s in preferred_sectors:
        lines.append(f"- {s}")
    return "\n".join(lines)

def scanner_message(scanner: list):
    lines = []
    lines.append("🚀 AI 종목 스캐너")
    lines.append("")
    if not scanner:
        lines.append("- 스캔 결과 없음")
        return "\n".join(lines)

    for r in scanner[:10]:
        lines.append(f"- {r['ticker']} | {r['sector']} | {r['grade']} | {r['score']}점 | 약 {r['price_krw']:,}원")
    return "\n".join(lines)

def execution_message(usdkrw: float, market_state: str, liquidity_state: str, portfolio: dict, settings: dict, watchlist: list, preferred_sectors: list):
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

    lines.append("[매도]")
    if sells:
        for s in sells:
            lines.append(f"- {s['ticker']} {s['qty']}주 | 약 {s['amount']:,}원 | {s['reason']}")
    else:
        lines.append("- 없음")

    lines.append("")
    lines.append("[매수]")
    if buys:
        for b in buys:
            lines.append(f"- {b['ticker']} {b['qty']}주 | 약 {b['amount']:,}원 | {b['grade']} | {b['sector']} | 실적 {b['fundamental']}")
    else:
        lines.append("- 없음")

    lines.append("")
    lines.append("[관망/보류]")
    if holds:
        for h in holds[:8]:
            lines.추가하다(플"-{h}")
    다른:
        lines.추가하다("- 없음")

    반환 "".합류하다(lines)

def main():
    portfolio = 로드포트폴리오()
    settings = 로드 설정()
    watchlist = load_watchlist()
    usdkrw = get_usdkrw()

    header, market_state, liquidity_state = header_message(usdkrw)
    preferred_sectors = sector_rotation_engine(market_state)
    scanner = build_scanner(usdkrw, portfolio, watchlist, preferred_sectors)

    send_텔레그램(header)
    send_텔레그램(portfolio_state_message(portfolio))
    send_텔레그램(sector_message(preferred_sectors))
    send_텔레그램(scanner_message(scanner))

    exe = execution_message(usdkrw, market_state, liquidity_state, portfolio, settings, watchlist, preferred_sectors)
    if엑세:
        send_텔레그램(exe)

만약 __name__ == "__main__":
    주요()

import os
import math
import requests
import yfinance as yf
import pandas as pd

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
FRED_API_KEY = os.getenv("FRED_API_KEY")

TOTAL_KRW = 7_000_000

# 현재 보유
portfolio = {
    "PANW": {"avg": 160.45, "shares": 8},
    "CRWD": {"avg": 405.55, "shares": 1},
    "COIN": {"avg": 197.85, "shares": 2},
    "AMT": {"avg": 179.86, "shares": 1},
    "KMI": {"avg": 33.08, "shares": 1},
    "UNH": {"avg": 281.75, "shares": 1},
    "OKTA": {"avg": 82.53, "shares": 8},
    "ZS": {"avg": 167.57, "shares": 4},
    "ACN": {"avg": 239.92, "shares": 3},
    "LAES": {"avg": 4.15, "shares": 47}
}

# 장기 코어
core_targets = {
    "PANW": {"weight": 0.10, "min_shares": 5},
    "CRWD": {"weight": 0.06, "min_shares": 1},
    "AMT": {"weight": 0.05, "min_shares": 1},
    "KMI": {"weight": 0.04, "min_shares": 1},
    "UNH": {"weight": 0.05, "min_shares": 1},
    "V": {"weight": 0.05, "min_shares": 0},
    "MSFT": {"weight": 0.08, "min_shares": 0},
    "COST": {"weight": 0.05, "min_shares": 0},
}

# 로테이션 레이어
rotation_targets = {
    "GLD": {"weight": 0.06},
    "TLT": {"weight": 0.05},
    "NVDA": {"weight": 0.06},
    "V": {"weight": 0.05},
    "KMI": {"weight": 0.04},
    "AMT": {"weight": 0.04},
    "EQIX": {"weight": 0.04},
    "MCK": {"weight": 0.04},
    "CEG": {"weight": 0.04},
}

# 미래성장 레이더
future_targets = {
    "LUNR": {"weight": 0.02},
    "RKLB": {"weight": 0.02},
    "QBTS": {"weight": 0.02},
    "ROK": {"weight": 0.02},
    "IONQ": {"weight": 0.02},
}

# 반대 성향 페어
pair_map = {
    "PANW": "V",
    "CRWD": "KMI",
    "COIN": "TLT",
    "OKTA": "UNH",
    "ZS": "COST",
    "NVDA": "GLD",
    "MSFT": "AMT",
}

WATCHLIST = sorted(set(list(core_targets.keys()) + list(rotation_targets.keys()) + list(future_targets.keys()) + list(portfolio.keys())))

def send_telegram(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(msg)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=20)

def get_history(ticker: str, period="1y"):
    df = yf.download(ticker, period=period, interval="1d", progress=False, auto_adjust=False)
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

def snapshot(ticker: str, usdkrw: float):
    close, vol = get_history(ticker, period="1y")
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
    pnl_pct = None if not avg else round((price / avg - 1) * 100, 1)
    return {
        "ticker": ticker,
        "price": round(price, 2),
        "price_krw": round(price * usdkrw),
        "ma50": ma50,
        "ma200": ma200,
        "rsi": rr,
        "shares": shares,
        "avg": avg,
        "pnl_pct": pnl_pct,
        "qqq20": qqq["20d"],
        "qqq60": qqq["60d"],
        "spy20": spy["20d"],
        "spy60": spy["60d"],
    }

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
    if score >= 1:
        return "🟡 Neutral", score, notes
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
    if score >= 0:
        return "🟡 유동성 중립", round(score, 1), notes
    return "🔴 유동성 부담", round(score, 1), notes

def tier_from_score(score: int):
    if score >= 50:
        return "A급"
    if score >= 30:
        return "B급"
    return "C급"

def rate_rotation_candidate(ticker: str, usdkrw: float):
    info = snapshot(ticker, usdkrw)
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

    if info["spy20"] is not None:
        if info["spy20"] >= 2:
            score += 5
            reasons.append("SPY 대비 강함")

    return {
        "ticker": ticker,
        "score": score,
        "tier": tier_from_score(score),
        "reason": ", ".join(reasons[:4]),
        "price_krw": info["price_krw"],
        "info": info,
    }

def build_recommendations(usdkrw: float):
    recs = []

    # 전략상 중요 후보 우선
    for ticker in ["GLD", "TLT", "V", "NVDA", "MSFT", "AMT", "EQIX", "MCK", "CEG", "LUNR", "RKLB", "QBTS"]:
        r = rate_rotation_candidate(ticker, usdkrw)
        if r:
            recs.append(r)

    # 반대팀 로테이션 보정
    for src, dst in pair_map.items():
        src_info = snapshot(src, usdkrw)
        dst_info = snapshot(dst, usdkrw)
        if not src_info or not dst_info:
            continue

        src_hot = src_info["rsi"] is not None and src_info["rsi"] >= 68
        dst_dip = dst_info["ma200"] and dst_info["price"] < dst_info["ma200"]

        if src_hot and dst_dip:
            for r in recs:
                if r["ticker"] == dst:
                    r["score"] += 15
                    r["tier"] = tier_from_score(r["score"])
                    r["reason"] = f"{src} 과열 → {dst} 눌림"

    # 미래성장은 최대 2개만
    recs = sorted(recs, key=lambda x: x["score"], reverse=True)

    seen = set()
    final = []
    future_count = 0
    for r in recs:
        if r["ticker"] in seen:
            continue
        seen.add(r["ticker"])

        if r["ticker"] in future_targets:
            if future_count >= 2:
                continue
            future_count += 1

        final.append(r)

    return final[:8]

def current_value_krw(ticker: str, usdkrw: float):
    info = snapshot(ticker, usdkrw)
    if not info:
        return 0
    return info["shares"] * info["price_krw"]

def target_value_krw(ticker: str):
    if ticker in core_targets:
        return round(TOTAL_KRW * core_targets[ticker]["weight"])
    if ticker in rotation_targets:
        return round(TOTAL_KRW * rotation_targets[ticker]["weight"])
    if ticker in future_targets:
        return round(TOTAL_KRW * future_targets[ticker]["weight"])
    return 0

def build_execution_plan(usdkrw: float, market_state: str, liq_state: str):
    sells = []
    buys = []
    holds = []

    # 1) 매도 후보
    for ticker in portfolio.keys():
        info = snapshot(ticker, usdkrw)
        if not info or info["shares"] <= 0:
            continue

        min_keep = 1
        if ticker in core_targets:
            min_keep = core_targets[ticker]["min_shares"]
        elif ticker in future_targets:
            min_keep = max(1, math.floor(info["shares"] * 0.20))
        elif ticker in rotation_targets:
            min_keep = 0

        sellable = max(0, info["shares"] - min_keep)
        if sellable <= 0:
            holds.append(f"{ticker} 최소 보유 유지")
            continue

        qty = 0
        reason = None

        if info["pnl_pct"] is not None and info["pnl_pct"] <= -20:
            qty = min(sellable, max(1, math.floor(info["shares"] * 0.20)))
            reason = "손실 정리"
        elif info["rsi"] is not None and info["rsi"] >= 72 and (info["pnl_pct"] or 0) > 5:
            qty = min(sellable, max(1, math.floor(info["shares"] * 0.20)))
            reason = "과열 차익"
        elif info["ma200"] and info["price"] < info["ma200"] and info["qqq60"] is not None and info["qqq60"] <= -8:
            qty = min(sellable, 1)
            reason = "약세 감축"

        if qty > 0 and reason:
            amount = qty * info["price_krw"]
            sells.append({
                "ticker": ticker,
                "qty": qty,
                "amount": round(amount),
                "reason": reason,
            })

    total_sell = sum(x["amount"] for x in sells)

    # 2) 매수 후보
    recs = build_recommendations(usdkrw)

    # 유동성 나쁘면 A급만
    if "부담" in liq_state or "Risk OFF" in market_state:
        recs = [x for x in recs if x["tier"] == "A급"]
    else:
        recs = [x for x in recs if x["tier"] in ["A급", "B급"]]

    remaining = total_sell

    for r in recs:
        ticker = r["ticker"]
        price_krw = r["price_krw"]
        current_val = current_value_krw(ticker, usdkrw)
        target_val = target_value_krw(ticker)
        gap = max(0, target_val - current_val)

        # 미래성장은 너무 크게 안 삼
        if ticker in future_targets:
            gap = min(gap, round(TOTAL_KRW * 0.03))

        if gap <= 0:
            holds.append(f"{ticker} 목표비중 근접")
            continue

        qty = min(math.floor(gap / max(price_krw, 1)), math.floor(remaining / max(price_krw, 1)))
        if qty <= 0:
            continue

        amount = qty * price_krw
        remaining -= amount
        buys.append({
            "ticker": ticker,
            "qty": qty,
            "amount": round(amount),
            "tier": r["tier"],
        })

    return sells, buys, holds[:8], round(total_sell), round(sum(x["amount"] for x in buys)), round(remaining)

def header_message(usdkrw: float):
    market_state, market_score, market_notes = market_engine()
    liq_state, liq_score, liq_notes = liquidity_engine()

    lines = []
    lines.append("📊 AI 자산운용 시스템 v2")
    lines.append("")
    lines.append(f"- 환율: {usdkrw:,.0f}원")
    lines.append(f"- 시장: {market_state} | 점수 {market_score}")
    lines.append(f"- 유동성: {liq_state} | 점수 {liq_score}")
    lines.append("")
    lines.append("핵심")
    for x in (market_notes[:2] + liq_notes[:3]):
        lines.append(f"- {x}")

    return "\n".join(lines), market_state, liq_state

def portfolio_state_message(usdkrw: float):
    lines = []
    lines.append("🗂 포트폴리오 상태")
    lines.append("")
    lines.append("코어")
    for ticker in core_targets.keys():
        info = snapshot(ticker, usdkrw)
        if info and info["shares"] > 0:
            lines.append(f"- {ticker} {info['shares']}주 | 최소유지 {core_targets[ticker]['min_shares']}주")

    lines.append("")
    lines.append("로테이션 관심")
    for ticker in list(rotation_targets.keys())[:6]:
        info = snapshot(ticker, usdkrw)
        if info:
            lines.append(f"- {ticker} | 약 {info['price_krw']:,}원")

    lines.append("")
    lines.append("미래성장 레이더")
    for ticker in future_targets.keys():
        info = snapshot(ticker, usdkrw)
        if info:
            lines.append(f"- {ticker} | 약 {info['price_krw']:,}원")

    return "\n".join(lines)

def execution_message(usdkrw: float, market_state: str, liq_state: str):
    sells, buys, holds, total_sell, total_buy, remain = build_execution_plan(usdkrw, market_state, liq_state)

    # 실행이 너무 작으면 실행표 안 보냄
    if total_sell < 200_000 and total_buy < 200_000:
        return None

    lines = []
    lines.append("📌 오늘 실행 필요")
    lines.append("")
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
            lines.append(f"- {b['ticker']} {b['qty']}주 | 약 {b['amount']:,}원 | {b['tier']}")
    else:
        lines.append("- 없음")

    lines.append("")
    lines.append("[보류]")
    if holds:
        for h in holds[:6]:
            lines.append(f"- {h}")
    else:
        lines.append("- 없음")

    return "\n".join(lines)

def main():
    usdkrw = get_usdkrw()
    header, market_state, liq_state = header_message(usdkrw)
    send_telegram(header)
    send_telegram(portfolio_state_message(usdkrw))
    exe = execution_message(usdkrw, market_state, liq_state)
    if exe:
        send_telegram(exe)

if __name__ == "__main__":
    main()

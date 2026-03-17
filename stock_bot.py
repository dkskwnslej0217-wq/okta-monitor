import os
import math
import requests
import yfinance as yf
import pandas as pd

# =========================================================
# ENV
# =========================================================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
FRED_API_KEY = os.getenv("FRED_API_KEY")

TOTAL_KRW = 7_000_000

# =========================================================
# 현재 보유
# =========================================================
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

# =========================================================
# 전략서 기반 24종목 구조
# 공격팀 12 / 방어팀 12
# =========================================================
attack_team = {
    "CRWD": {"weight": 0.05, "status": "hold"},
    "PANW": {"weight": 0.05, "status": "hold"},
    "OKTA": {"weight": 0.05, "status": "hold"},
    "ZS": {"weight": 0.05, "status": "hold"},
    "NVDA": {"weight": 0.05, "status": "buy"},
    "MSFT": {"weight": 0.05, "status": "buy"},
    "COIN": {"weight": 0.05, "status": "hold"},
    "CRCL": {"weight": 0.03, "status": "wait"},
    "PLTR": {"weight": 0.03, "status": "wait"},
    "RKLB": {"weight": 0.03, "status": "wait"},
    "ACN": {"weight": 0.03, "status": "hold"},
    "LAES": {"weight": 0.03, "status": "hold"},
}

defense_team = {
    "V": {"weight": 0.03, "status": "buy"},
    "UNH": {"weight": 0.03, "status": "hold"},
    "KMI": {"weight": 0.03, "status": "buy"},
    "AMT": {"weight": 0.03, "status": "buy"},
    "EQIX": {"weight": 0.03, "status": "buy"},
    "COST": {"weight": 0.03, "status": "buy"},
    "CEG": {"weight": 0.03, "status": "buy"},
    "PLD": {"weight": 0.03, "status": "buy"},
    "MCK": {"weight": 0.03, "status": "buy"},
    "GLD": {"weight": 0.04, "status": "buy"},
    "TLT": {"weight": 0.04, "status": "buy"},
    "AWK": {"weight": 0.03, "status": "wait"},
}

# 전략서의 반대 성향 페어
pair_map = {
    "NVDA": "GLD",
    "COIN": "TLT",
    "CRWD": "KMI",
    "MSFT": "AMT",
    "OKTA": "UNH",
    "ZS": "COST",
    "PANW": "V",
    "PLTR": "MCK",
    "RKLB": "CEG",
    "ACN": "PLD",
    "CRCL": "EQIX",
    "LAES": "AWK",
}

all_attack = list(attack_team.keys())
all_defense = list(defense_team.keys())
all_watch = all_attack + all_defense

# =========================================================
# 유틸
# =========================================================
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

def strength_label(diff: float):
    if diff is None:
        return "비교불가"
    if diff >= 8:
        return "매우강함"
    elif diff >= 2:
        return "강함"
    elif diff > -2:
        return "비슷"
    elif diff > -8:
        return "약함"
    else:
        return "매우약함"

def compare_vs_benchmark(ticker: str, benchmark: str, period="6mo"):
    try:
        a, _ = get_history(ticker, period=period)
        b, _ = get_history(benchmark, period=period)
        if len(a) < 61 or len(b) < 61:
            return {"20d": None, "60d": None, "20_label": "비교불가", "60_label": "비교불가"}

        a20 = calc_return(a, 20)
        b20 = calc_return(b, 20)
        a60 = calc_return(a, 60)
        b60 = calc_return(b, 60)

        d20 = None if a20 is None or b20 is None else round(a20 - b20, 1)
        d60 = None if a60 is None or b60 is None else round(a60 - b60, 1)

        return {
            "20d": d20,
            "60d": d60,
            "20_label": strength_label(d20),
            "60_label": strength_label(d60)
        }
    except Exception:
        return {"20d": None, "60d": None, "20_label": "비교불가", "60_label": "비교불가"}

def fred_series(series_id: str):
    if not FRED_API_KEY:
        return None
    try:
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": series_id,
            "api_key": FRED_API_KEY,
            "file_type": "json"
        }
        r = requests.get(url, params=params, timeout=20)
        data = r.json()
        vals = []
        for x in data.get("observations", []):
            v = x.get("value")
            if v == ".":
                continue
            vals.append(float(v))
        return vals if len(vals) >= 2 else None
    except Exception:
        return None

# =========================================================
# 시장 / 유동성
# =========================================================
def market_engine():
    score = 0
    details = []

    try:
        spy, _ = get_history("SPY", "1y")
        qqq, _ = get_history("QQQ", "1y")
        vix, _ = get_history("^VIX", "3mo")

        if len(spy) >= 200:
            if float(spy.iloc[-1]) > float(spy.rolling(200).mean().iloc[-1]):
                score += 2
                details.append("SPY > 200MA : +2")
            else:
                score -= 2
                details.append("SPY < 200MA : -2")

        if len(qqq) >= 200:
            if float(qqq.iloc[-1]) > float(qqq.rolling(200).mean().iloc[-1]):
                score += 2
                details.append("QQQ > 200MA : +2")
            else:
                score -= 2
                details.append("QQQ < 200MA : -2")

        if len(vix) > 0:
            vv = float(vix.iloc[-1])
            if vv < 18:
                score += 1
                details.append("VIX 안정 : +1")
            elif vv > 25:
                score -= 1
                details.append("VIX 위험 : -1")
            else:
                details.append("VIX 중간 : 0")
    except Exception:
        details.append("시장 데이터 오류")

    if score >= 5:
        grade = "최고의 안전"
        state = "🟢 Risk ON"
    elif score >= 3:
        grade = "안전"
        state = "🟢 Risk ON"
    elif score >= 1:
        grade = "보통"
        state = "🟡 Neutral"
    elif score >= -1:
        grade = "위험"
        state = "🟠 Caution"
    else:
        grade = "엄청위험"
        state = "🔴 Risk OFF"

    return state, score, grade, details

def liquidity_engine():
    score = 0
    details = []

    try:
        tnx, _ = get_history("^TNX", "6mo")
        if len(tnx) >= 21:
            prev, now = float(tnx.iloc[-21]), float(tnx.iloc[-1])
            if now < prev:
                score += 1
                details.append("10Y 하락 : +1")
            else:
                score -= 1
                details.append("10Y 상승 : -1")
    except Exception:
        details.append("10Y 오류")

    try:
        dxy, _ = get_history("DX-Y.NYB", "6mo")
        if len(dxy) >= 21:
            prev, now = float(dxy.iloc[-21]), float(dxy.iloc[-1])
            if now < prev:
                score += 1
                details.append("DXY 약세 : +1")
            else:
                score -= 1
                details.append("DXY 강세 : -1")
    except Exception:
        details.append("DXY 오류")

    tga = fred_series("WTREGEN")
    if tga:
        if tga[-1] < tga[-2]:
            score += 1
            details.append("TGA 감소 : +1")
        else:
            score -= 1
            details.append("TGA 증가 : -1")
    else:
        details.append("TGA 없음")

    rrp = fred_series("RRPONTSYD")
    if rrp:
        if rrp[-1] < rrp[-2]:
            score += 1
            details.append("RRP 감소 : +1")
        else:
            score -= 0.5
            details.append("RRP 증가 : -0.5")
    else:
        details.append("RRP 없음")

    hy = fred_series("BAMLH0A0HYM2")
    if hy:
        if hy[-1] < hy[-2]:
            score += 1
            details.append("HY 스프레드 축소 : +1")
        else:
            score -= 1
            details.append("HY 스프레드 확대 : -1")
    else:
        details.append("HY 없음")

    if score >= 3:
        grade = "최고의 안전"
        state = "🟢 유동성 우호"
    elif score >= 1:
        grade = "안전"
        state = "🟢 유동성 우호"
    elif score >= 0:
        grade = "보통"
        state = "🟡 유동성 중립"
    elif score >= -1:
        grade = "위험"
        state = "🟠 유동성 주의"
    else:
        grade = "엄청위험"
        state = "🔴 유동성 부담"

    return state, round(score, 1), grade, details

# =========================================================
# 데이터 계산
# =========================================================
def ticker_snapshot(ticker: str, usdkrw: float):
    close, vol = get_history(ticker, "1y")
    if len(close) == 0:
        return None

    current_price = float(close.iloc[-1])
    ma50 = float(close.rolling(50).mean().iloc[-1]) if len(close) >= 50 else None
    ma200 = float(close.rolling(200).mean().iloc[-1]) if len(close) >= 200 else None
    rr = float(rsi(close).iloc[-1]) if len(close) >= 20 else None

    shares = portfolio.get(ticker, {}).get("shares", 0)
    avg = portfolio.get(ticker, {}).get("avg")
    pnl_pct = None
    if avg:
        pnl_pct = round((current_price / avg - 1) * 100, 1)

    qqq_rs = compare_vs_benchmark(ticker, "QQQ")
    spy_rs = compare_vs_benchmark(ticker, "SPY")

    return {
        "ticker": ticker,
        "price": round(current_price, 2),
        "price_krw": round(current_price * usdkrw),
        "ma50": ma50,
        "ma200": ma200,
        "rsi": rr,
        "shares": shares,
        "avg": avg,
        "pnl_pct": pnl_pct,
        "qqq_rs": qqq_rs,
        "spy_rs": spy_rs
    }

def target_amount_krw(ticker: str):
    if ticker in attack_team:
        return round(TOTAL_KRW * attack_team[ticker]["weight"])
    if ticker in defense_team:
        return round(TOTAL_KRW * defense_team[ticker]["weight"])
    return 0

def min_seed_shares(ticker: str, usdkrw: float):
    info = ticker_snapshot(ticker, usdkrw)
    if not info:
        return 0
    target_amt = target_amount_krw(ticker)
    if target_amt <= 0:
        return 0
    seed_amt = target_amt * 0.20
    return max(1, math.floor(seed_amt / max(info["price_krw"], 1)))

# =========================================================
# 추천 로직
# =========================================================
def recommendation_tier(ticker: str, usdkrw: float):
    info = ticker_snapshot(ticker, usdkrw)
    if not info:
        return "관찰", -999, "데이터 부족"

    score = 0
    reasons = []

    if info["ma200"] and info["price"] > info["ma200"]:
        score += 20
        reasons.append("200일선 위")
    else:
        reasons.append("200일선 아래")

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

    if info["qqq_rs"]["20_label"] in ["강함", "매우강함"]:
        score += 15
        reasons.append("QQQ 대비 강함")
    elif info["qqq_rs"]["20_label"] in ["약함", "매우약함"]:
        score -= 10

    if info["spy_rs"]["20_label"] in ["강함", "매우강함"]:
        score += 10
        reasons.append("SPY 대비 강함")

    if ticker in defense_team:
        # 방어팀은 눌림 매수 우대
        if info["ma200"] and info["price"] < info["ma200"]:
            score += 15
            reasons.append("방어팀 200일선 아래")
    else:
        # 공격팀은 추세 회복형 우대
        if info["ma200"] and info["price"] > info["ma200"]:
            score += 10

    if score >= 45:
        return "강력추천", score, ", ".join(reasons[:4])
    elif score >= 25:
        return "추천", score, ", ".join(reasons[:4])
    return "관찰", score, ", ".join(reasons[:4])

def build_rotation_recommendations(usdkrw: float):
    recs = []

    # 전략서의 반대팀 로테이션 + 추가 예정 우선 반영
    for atk, dfn in pair_map.items():
        atk_info = ticker_snapshot(atk, usdkrw) if atk in all_watch else None
        dfn_info = ticker_snapshot(dfn, usdkrw) if dfn in all_watch else None

        if not atk_info or not dfn_info:
            continue

        atk_tier, atk_score, atk_reason = recommendation_tier(atk, usdkrw)
        dfn_tier, dfn_score, dfn_reason = recommendation_tier(dfn, usdkrw)

        # 공격팀이 과열/상승하면 방어팀 추천
        if atk_info["rsi"] and atk_info["rsi"] > 68 and dfn_info["ma200"] and dfn_info["price"] < dfn_info["ma200"]:
            recs.append({
                "from_ticker": atk,
                "to_ticker": dfn,
                "to_team": "방어",
                "tier": dfn_tier,
                "reason": f"{atk} 상승/과열 → {dfn} 눌림"
            })

        # 방어팀이 과열하면 공격팀 추천
        if dfn_info["rsi"] and dfn_info["rsi"] > 68 and atk_info["ma200"] and atk_info["price"] < atk_info["ma200"]:
            recs.append({
                "from_ticker": dfn,
                "to_ticker": atk,
                "to_team": "공격",
                "tier": atk_tier,
                "reason": f"{dfn} 상승/과열 → {atk} 눌림"
            })

    # 전략서 우선순위 보강
    for ticker in ["GLD", "NVDA", "MSFT", "AMT", "V"]:
        tier, score, reason = recommendation_tier(ticker, usdkrw)
        recs.append({
            "from_ticker": None,
            "to_ticker": ticker,
            "to_team": "우선순위",
            "tier": tier,
            "reason": reason
        })

    # 중복 제거
    seen = set()
    final = []
    for r in recs:
        key = r["to_ticker"]
        if key in seen:
            continue
        seen.add(key)
        final.append(r)

    tier_order = {"강력추천": 0, "추천": 1, "관찰": 2}
    final = sorted(final, key=lambda x: tier_order.get(x["tier"], 9))
    return final[:8]

# =========================================================
# 실행 요약
# =========================================================
def execution_plan(usdkrw: float):
    sells = []
    buys = []
    holds = []

    recs = build_rotation_recommendations(usdkrw)

    # 매도 후보: 전략서 기준 20% 단위
    for ticker in portfolio.keys():
        info = ticker_snapshot(ticker, usdkrw)
        if not info or info["shares"] <= 0:
            continue

        seed = min_seed_shares(ticker, usdkrw)
        sellable = max(0, info["shares"] - seed)
        if sellable <= 0:
            holds.append(f"{ticker} 씨앗 유지")
            continue

        reason = None
        qty = 0

        # 1차: 50일선 위 + RSI 과열 → 20%
        if info["ma50"] and info["price"] > info["ma50"] and info["rsi"] and info["rsi"] >= 70:
            qty = max(1, math.floor(info["shares"] * 0.20))
            reason = "1차 분할매도"

        # 2차: 200일선 크게 위 + 수익권 → 20%
        elif info["ma200"] and info["price"] > info["ma200"] * 1.10 and info["pnl_pct"] and info["pnl_pct"] > 5:
            qty = max(1, math.floor(info["shares"] * 0.20))
            reason = "2차 분할매도"

        # 손실 경고
        elif info["pnl_pct"] is not None and info["pnl_pct"] <= -20 and sellable > 0:
            qty = min(max(1, math.floor(info["shares"] * 0.20)), sellable)
            reason = "손실 정리"

        qty = min(qty, sellable)
        if qty > 0 and reason:
            sells.append({
                "ticker": ticker,
                "qty": qty,
                "amount": qty * info["price_krw"],
                "reason": reason
            })

    total_sell = sum(x["amount"] for x in sells)

    # 매수 후보
    remaining = total_sell
    for r in recs:
        if r["tier"] == "관찰":
            continue

        ticker = r["to_ticker"]
        info = ticker_snapshot(ticker, usdkrw)
        if not info:
            continue

        target_amt = target_amount_krw(ticker)
        current_amt = 0
        if ticker in portfolio:
            current_amt = portfolio[ticker]["shares"] * info["price_krw"]

        gap = max(0, target_amt - current_amt)
        if gap <= 0:
            holds.append(f"{ticker} 목표비중 근접")
            continue

        qty = min(math.floor(gap / max(info["price_krw"], 1)), math.floor(remaining / max(info["price_krw"], 1)))
        if qty <= 0:
            continue

        amount = qty * info["price_krw"]
        remaining -= amount

        buys.append({
            "ticker": ticker,
            "qty": qty,
            "amount": amount,
            "tier": r["tier"],
            "reason": r["reason"]
        })

    return sells, buys, holds[:8], round(total_sell), round(sum(x["amount"] for x in buys)), round(remaining)

# =========================================================
# 메시지
# =========================================================
def header_message(usdkrw: float):
    market_state, market_score, market_grade, _ = market_engine()
    liq_state, liq_score, liq_grade, liq_details = liquidity_engine()

    lines = []
    lines.append("📊 AI 투자 시스템 5.0")
    lines.append("")
    lines.append(f"- 환율: {usdkrw:,.0f}원")
    lines.append(f"- 시장: {market_state} | {market_grade} | 점수 {market_score}")
    lines.append(f"- 유동성: {liq_state} | {liq_grade} | 점수 {liq_score}")
    lines.append("")
    lines.append("채권/유동성 핵심")
    for d in liq_details[:5]:
        lines.append(f"- {d}")

    return "\n".join(lines), market_state, liq_state

def recommendation_message(usdkrw: float):
    recs = build_rotation_recommendations(usdkrw)

    lines = []
    lines.append("🔥 오늘의 로테이션 추천")
    lines.append("")

    if not recs:
        lines.append("- 추천 없음")
        return "\n".join(lines)

    for r in recs[:6]:
        info = ticker_snapshot(r["to_ticker"], usdkrw)
        if not info:
            continue
        lines.append(
            f"- {r['to_ticker']} | {r['tier']} | 약 {info['price_krw']:,}원 | {r['reason']}"
        )

    return "\n".join(lines)

def execution_message(usdkrw: float):
    sells, buys, holds, total_sell, total_buy, remain = execution_plan(usdkrw)

    lines = []
    lines.append("📌 오늘 실행 요약")
    lines.append("")
    lines.append(f"- 총 매도 예정: {total_sell:,}원")
    lines.append(f"- 총 매수 예정: {total_buy:,}원")
    lines.append(f"- 실행 후 예상 잔여현금: {remain:,}원")
    lines.append("")

    lines.append("[매도]")
    if sells:
        for s in sells:
            lines.append(f"- {s['ticker']} {s['qty']}주 | 약 {round(s['amount']):,}원 | {s['reason']}")
    else:
        lines.append("- 없음")

    lines.append("")
    lines.append("[매수]")
    if buys:
        for b in buys:
            lines.append(f"- {b['ticker']} {b['qty']}주 | 약 {round(b['amount']):,}원 | {b['tier']}")
    else:
        lines.append("- 없음")

    lines.append("")
    lines.append("[보류]")
    if holds:
        for h in holds:
            lines.append(f"- {h}")
    else:
        lines.append("- 없음")

    return "\n".join(lines)

def protection_message(usdkrw: float):
    lines = []
    lines.append("⚠ 손실/보호 메시지")
    lines.append("")

    found = False
    for ticker in portfolio.keys():
        info = ticker_snapshot(ticker, usdkrw)
        if not info:
            continue

        seed = min_seed_shares(ticker, usdkrw)
        lines.append(f"- {ticker} | 현재 {info['shares']}주 | 씨앗 최소 {seed}주")
        found = True

        if info["pnl_pct"] is not None and info["pnl_pct"] <= -20:
            lines.append(f"  · 손실 {info['pnl_pct']}% 주의")
        if info["qqq_rs"]["60_label"] in ["약함", "매우약함"]:
            lines.append(f"  · 시장대비 약세")
        if info["rsi"] and info["rsi"] >= 70:
            lines.append(f"  · 과열 구간")

    if not found:
        lines.append("- 보유 종목 없음")

    return "\n".join(lines)

# =========================================================
# main
# =========================================================
def main():
    usdkrw = get_usdkrw()
    header, _, _ = header_message(usdkrw)

    send_telegram(header)
    send_telegram(recommendation_message(usdkrw))
    send_telegram(execution_message(usdkrw))
    send_telegram(protection_message(usdkrw))

if __name__ == "__main__":
    main()

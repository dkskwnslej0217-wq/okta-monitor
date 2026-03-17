import os
import math
import requests
import yfinance as yf
import pandas as pd
import numpy as np

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
NEWS_API_KEY = os.getenv("NEWS_API_KEY")
FRED_API_KEY = os.getenv("FRED_API_KEY")

TOTAL_KRW = 7_000_000

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

core_stocks = [
    "PANW", "CRWD", "ZS", "OKTA", "MSFT", "AMZN", "GOOGL", "NOW", "ACN",
    "V", "MA", "ICE", "CME", "ADP",
    "AMT", "EQIX", "KMI", "WMB", "ENB",
    "UNH", "MCK", "COST", "PG", "KO", "PEP", "ABBV", "LLY", "AWK", "NEE", "CEG"
]

future_map = {
    "양자": ["IONQ", "RGTI", "QBTS", "QUBT"],
    "우주": ["RKLB", "ASTS", "LUNR", "RDW"],
    "장수": ["CRSP", "BEAM", "NTLA", "VRTX"],
    "로봇": ["PATH", "SYM", "ABB", "ROK"]
}

future_etf_map = {
    "양자": "QTUM",
    "우주": "UFO",
    "장수": "ARKG",
    "로봇": "BOTZ"
}


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


def get_usdkrw():
    try:
        close, _ = get_history("USDKRW=X", period="10d")
        if len(close) > 0:
            return round(float(close.iloc[-1]), 2)
    except Exception:
        pass
    return 1350.0


def calc_return(close: pd.Series, days: int):
    if len(close) < days + 1:
        return None
    return (float(close.iloc[-1]) / float(close.iloc[-days - 1]) - 1) * 100


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
        close_a, _ = get_history(ticker, period=period)
        close_b, _ = get_history(benchmark, period=period)

        if len(close_a) < 61 or len(close_b) < 61:
            return {"20d_diff": None, "60d_diff": None, "20d_label": "비교불가", "60d_label": "비교불가"}

        a20 = calc_return(close_a, 20)
        b20 = calc_return(close_b, 20)
        a60 = calc_return(close_a, 60)
        b60 = calc_return(close_b, 60)

        d20 = None if a20 is None or b20 is None else round(a20 - b20, 1)
        d60 = None if a60 is None or b60 is None else round(a60 - b60, 1)

        return {
            "20d_diff": d20,
            "60d_diff": d60,
            "20d_label": strength_label(d20),
            "60d_label": strength_label(d60)
        }
    except Exception:
        return {"20d_diff": None, "60d_diff": None, "20d_label": "비교불가", "60d_label": "비교불가"}


def conviction_label(score: int):
    if score >= 75:
        return "강력추천"
    elif score >= 50:
        return "추천"
    else:
        return "관찰"


def score_stock(ticker: str):
    try:
        close, vol = get_history(ticker, period="1y")
        if len(close) < 200:
            return None

        ma200 = close.rolling(200).mean()
        current_rsi = rsi(close)
        last_close = float(close.iloc[-1])
        last_ma200 = float(ma200.iloc[-1])
        last_rsi = float(current_rsi.iloc[-1])

        score = 0

        if last_close > last_ma200:
            score += 35

        if last_rsi < 35:
            score += 30
        elif last_rsi < 45:
            score += 15

        if len(vol) >= 20:
            vol_avg20 = float(vol.rolling(20).mean().iloc[-1])
            if float(vol.iloc[-1]) > vol_avg20:
                score += 20

        mid = close.rolling(20).mean().iloc[-1]
        std = close.rolling(20).std().iloc[-1]
        lower = mid - 2 * std

        if last_close <= lower:
            score += 15

        return {
            "ticker": ticker,
            "score": int(score),
            "label": conviction_label(int(score)),
            "price": round(last_close, 2),
            "rsi": round(last_rsi, 1),
            "ma200": round(last_ma200, 2)
        }
    except Exception:
        return None


def market_engine():
    score = 0
    try:
        spy_close, _ = get_history("SPY", period="1y")
        qqq_close, _ = get_history("QQQ", period="1y")
        vix_close, _ = get_history("^VIX", period="3mo")

        if len(spy_close) >= 200:
            if float(spy_close.iloc[-1]) > float(spy_close.rolling(200).mean().iloc[-1]):
                score += 2
            else:
                score -= 2

        if len(qqq_close) >= 200:
            if float(qqq_close.iloc[-1]) > float(qqq_close.rolling(200).mean().iloc[-1]):
                score += 2
            else:
                score -= 2

        if len(vix_close) > 0:
            vix = float(vix_close.iloc[-1])
            if vix < 18:
                score += 1
            elif vix > 25:
                score -= 1
    except Exception:
        pass

    if score >= 3:
        return "🟢 Risk ON", score
    elif score <= -2:
        return "🔴 Risk OFF", score
    return "🟡 Neutral", score


def portfolio_snapshot(usdkrw: float):
    rows = []
    for ticker, data in portfolio.items():
        try:
            close, _ = get_history(ticker, period="6mo")
            if len(close) == 0:
                continue
            current_price = float(close.iloc[-1])
            eval_krw = current_price * data["shares"] * usdkrw
            pnl_pct = (current_price / data["avg"] - 1) * 100
            pnl_krw = (current_price - data["avg"]) * data["shares"] * usdkrw
            rows.append({
                "ticker": ticker,
                "shares": data["shares"],
                "current_price": current_price,
                "eval_krw": round(eval_krw),
                "pnl_pct": round(pnl_pct, 1),
                "pnl_krw": round(pnl_krw)
            })
        except Exception:
            pass
    return rows


def build_sell_candidates(usdkrw: float):
    rows = portfolio_snapshot(usdkrw)
    sells = []

    for row in rows:
        ticker = row["ticker"]
        try:
            close, _ = get_history(ticker, period="1y")
            if len(close) < 200:
                continue

            ma200 = float(close.rolling(200).mean().iloc[-1])
            current_price = float(close.iloc[-1])
            current_rsi = float(rsi(close).iloc[-1])

            qqq_rs = compare_vs_benchmark(ticker, "QQQ")
            weak = qqq_rs["60d_label"] in ["매우약함", "약함"]

            priority = 0
            reason = "유지"

            if row["pnl_pct"] <= -20:
                priority = 100
                reason = "-20% 이하"
            elif current_price < ma200 and weak:
                priority = 80
                reason = "장기선 아래 + 시장대비 약함"
            elif current_rsi > 70 and row["pnl_pct"] > 0:
                priority = 60
                reason = "과열"
            elif ticker not in core_stocks and ticker not in sum(future_map.values(), []):
                priority = 70
                reason = "비핵심 종목"

            if priority > 0:
                price_krw = current_price * usdkrw
                max_sell = row["shares"]
                # 씨앗 최소 1주 남기기 (미래성장만)
                if ticker in sum(future_map.values(), []) and row["shares"] > 1:
                    max_sell = row["shares"] - 1

                sells.append({
                    "ticker": ticker,
                    "priority": priority,
                    "reason": reason,
                    "shares_owned": row["shares"],
                    "max_sell": max_sell,
                    "price_krw": round(price_krw),
                    "eval_krw": row["eval_krw"]
                })
        except Exception:
            pass

    return sorted(sells, key=lambda x: x["priority"], reverse=True)


def build_future_targets(usdkrw: float):
    targets = []

    target_sector_budget = TOTAL_KRW * 0.11  # 섹터당 약 11%

    for sector_name, tickers in future_map.items():
        scored = []
        for t in tickers:
            s = score_stock(t)
            if s:
                etf = future_etf_map.get(sector_name)
                etf_rs = compare_vs_benchmark(t, etf)
                qqq_rs = compare_vs_benchmark(t, "QQQ")
                spy_rs = compare_vs_benchmark(t, "SPY")

                bonus = 0
                if etf_rs["20d_label"] in ["강함", "매우강함"]:
                    bonus += 10
                if qqq_rs["20d_label"] in ["강함", "매우강함"]:
                    bonus += 10
                if spy_rs["20d_label"] in ["강함", "매우강함"]:
                    bonus += 5

                s["final_rank"] = s["score"] + bonus
                s["sector"] = sector_name
                s["etf_rs"] = etf_rs
                s["qqq_rs"] = qqq_rs
                s["spy_rs"] = spy_rs
                scored.append(s)

        if not scored:
            continue

        best = sorted(scored, key=lambda x: x["final_rank"], reverse=True)[0]
        price_krw = best["price"] * usdkrw
        target_shares = math.floor(target_sector_budget / price_krw)

        current_shares = portfolio.get(best["ticker"], {}).get("shares", 0)
        need_buy = max(0, target_shares - current_shares)

        targets.append({
            "sector": sector_name,
            "ticker": best["ticker"],
            "label": best["label"],
            "price_krw": round(price_krw),
            "current_shares": current_shares,
            "target_shares": target_shares,
            "need_buy": need_buy,
            "rank": best["final_rank"]
        })

    return sorted(targets, key=lambda x: x["rank"], reverse=True)


def execution_plan(usdkrw: float):
    sell_candidates = build_sell_candidates(usdkrw)
    future_targets = build_future_targets(usdkrw)

    # 관찰은 공격매수 안 함
    buy_targets = [x for x in future_targets if x["label"] in ["강력추천", "추천"]]

    total_buy_need = sum(x["need_buy"] * x["price_krw"] for x in buy_targets)

    sells = []
    cash_from_sells = 0

    for s in sell_candidates:
        if cash_from_sells >= total_buy_need:
            break
        if s["max_sell"] <= 0:
            continue

        # 한 번에 다 팔지 말고 최대 1주 또는 저가주는 일부 조정
        if s["price_krw"] > 300_000:
            sell_qty = min(1, s["max_sell"])
        else:
            sell_qty = min(max(1, math.ceil((total_buy_need - cash_from_sells) / max(s["price_krw"], 1))), s["max_sell"])
            sell_qty = min(sell_qty, 3)

        if sell_qty <= 0:
            continue

        sell_amount = sell_qty * s["price_krw"]
        cash_from_sells += sell_amount
        sells.append({
            "ticker": s["ticker"],
            "qty": sell_qty,
            "amount": round(sell_amount),
            "reason": s["reason"]
        })

    buys = []
    remaining_cash = cash_from_sells

    for b in buy_targets:
        if b["need_buy"] <= 0:
            continue
        affordable = remaining_cash // b["price_krw"]
        qty = min(b["need_buy"], affordable)
        if qty <= 0:
            continue

        amount = qty * b["price_krw"]
        remaining_cash -= amount
        buys.append({
            "sector": b["sector"],
            "ticker": b["ticker"],
            "qty": int(qty),
            "amount": round(amount),
            "label": b["label"]
        })

    holds = []
    for b in future_targets:
        if b["label"] == "관찰":
            holds.append(f"{b['sector']} {b['ticker']} 관찰 유지")
        elif b["need_buy"] <= 0:
            holds.append(f"{b['sector']} {b['ticker']} 현재 비중 적정")

    return sells, buys, holds, round(cash_from_sells), round(total_buy_need), round(remaining_cash)


def drawdown_report():
    lines = []
    danger = False
    for ticker, data in portfolio.items():
        try:
            close, _ = get_history(ticker, period="1y")
            if len(close) < 200:
                continue
            current_price = float(close.iloc[-1])
            pnl_pct = (current_price / data["avg"] - 1) * 100
            if pnl_pct <= -20:
                current_rsi = float(rsi(close).iloc[-1])
                lines.append(f"- {ticker} | 손익 {round(pnl_pct,1)}% | RSI {round(current_rsi,1)} | 감축 검토")
                danger = True
        except Exception:
            pass
    if not danger:
        return "🩺 -20% 손실 종목 점검\n\n- 해당 없음"
    return "🩺 -20% 손실 종목 점검\n\n" + "\n".join(lines)


def main():
    usdkrw = get_usdkrw()
    market_state, market_score = market_engine()

    sells, buys, holds, total_sell_cash, total_buy_need, remain = execution_plan(usdkrw)

    lines = []
    lines.append("📌 오늘 실행 요약")
    lines.append("")
    lines.append(f"- 환율: {usdkrw:,.0f}원")
    lines.append(f"- 시장: {market_state} ({market_score})")
    lines.append(f"- 총 매도 예정: {total_sell_cash:,}원")
    lines.append(f"- 총 매수 필요: {total_buy_need:,}원")
    lines.append(f"- 실행 후 예상 잔여현금: {remain:,}원")
    lines.append("")

    lines.append("[매도]")
    if sells:
        for s in sells:
            lines.append(f"- {s['ticker']} {s['qty']}주 매도 | 약 {s['amount']:,}원 | 이유: {s['reason']}")
    else:
        lines.append("- 없음")

    lines.append("")
    lines.append("[매수]")
    if buys:
        for b in buys:
            lines.append(f"- {b['ticker']} {b['qty']}주 매수 | 약 {b['amount']:,}원 | {b['label']}")
    else:
        lines.append("- 없음")

    lines.append("")
    lines.append("[보류]")
    if holds:
        for h in holds[:6]:
            lines.append(f"- {h}")
    else:
        lines.append("- 없음")

    send_telegram("\n".join(lines))
    send_telegram(drawdown_report())


if __name__ == "__main__":
    main()

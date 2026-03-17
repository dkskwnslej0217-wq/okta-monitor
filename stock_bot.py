import os
import math
from datetime import datetime, timezone

import requests
import yfinance as yf
import pandas as pd
import numpy as np


# =========================================
# 환경변수
# =========================================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# 선택: NewsAPI 키
NEWS_API_KEY = os.getenv("NEWS_API_KEY")

# 원화 기준 총자산 (예: 700만원)
TOTAL_KRW = 7_000_000

# 최소 현금 비중
BASE_CASH_RATIO = 0.15

# 미래 섹터 비중 (양자/우주/장수/로봇 합)
FUTURE_TOTAL_RATIO = 0.40

# 코어 비중
CORE_TOTAL_RATIO = 0.45

# 나머지 = 현금
# 45% core + 40% future + 15% cash = 100%


# =========================================
# 현재 포트폴리오
# avg = 달러 기준 평단, shares = 보유 주식 수
# =========================================
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


# =========================================
# 후보 풀
# =========================================
core_stocks = [
    "PANW", "CRWD", "ZS", "OKTA", "MSFT", "AMZN", "GOOGL", "NOW", "ACN",
    "V", "MA", "ICE", "CME", "ADP",
    "AMT", "EQIX", "KMI", "WMB", "ENB",
    "UNH", "MCK", "COST", "PG", "KO", "PEP", "ABBV", "LLY", "AWK", "NEE", "CEG"
]

future_quantum = ["IONQ", "RGTI", "QBTS", "QUBT"]
future_space = ["RKLB", "ASTS", "LUNR", "RDW"]
future_longevity = ["CRSP", "BEAM", "NTLA", "VRTX"]
future_robotics = ["PATH", "SYM", "ABB", "ROK"]

future_map = {
    "양자": future_quantum,
    "우주": future_space,
    "장수": future_longevity,
    "로봇": future_robotics
}

all_candidates = sorted(list(set(core_stocks + sum(future_map.values(), []))))


# =========================================
# 텔레그램
# =========================================
def send_telegram(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(msg)
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg
    }
    requests.post(url, data=payload, timeout=20)


# =========================================
# 보조 함수
# =========================================
def safe_float(x, default=None):
    try:
        return float(x)
    except Exception:
        return default


def rsi(series: pd.Series, period: int = 14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def get_usdkrw():
    # Yahoo Finance 환율 심볼
    try:
        fx = yf.download("KRW=X", period="10d", interval="1d", progress=False)
        # KRW=X 는 1 USD = ? KRW 가 아니라 1 KRW = ? USD 형태로 혼동될 수 있어
        # 대신 USD/KRW는 보통 USDKRW=X 가 잘 안 나오는 경우가 있어,
        # 여기서는 FRED 등 대신 현실적으로 1300원대 기본값 fallback 사용
        close = fx["Close"].squeeze().dropna()
        if len(close) > 0:
            val = safe_float(close.iloc[-1])
            # KRW=X가 너무 작은 값이면 반대로 처리
            if val and val < 10:
                return round(1 / val, 2)
    except Exception:
        pass

    # fallback
    return 1350.0


def get_history(ticker: str, period="1y"):
    df = yf.download(ticker, period=period, interval="1d", progress=False, auto_adjust=False)
    if isinstance(df, pd.DataFrame) and not df.empty:
        close = df["Close"].squeeze().dropna()
        volume = df["Volume"].squeeze().dropna() if "Volume" in df else pd.Series(dtype=float)
        return close, volume
    return pd.Series(dtype=float), pd.Series(dtype=float)


# =========================================
# 유동성 엔진
# 간단 점수 버전
# =========================================
def liquidity_engine():
    score = 0
    notes = []

    # 10년물 금리
    try:
        tn_close, _ = get_history("^TNX", period="6mo")
        if len(tn_close) >= 21:
            now = tn_close.iloc[-1]
            prev = tn_close.iloc[-21]
            if now < prev:
                score += 1
                notes.append("10년물 금리 하락")
            else:
                score -= 1
                notes.append("10년물 금리 상승")
    except Exception:
        notes.append("10년물 금리 오류")

    # 달러 인덱스
    try:
        dxy_close, _ = get_history("DX-Y.NYB", period="6mo")
        if len(dxy_close) >= 21:
            now = dxy_close.iloc[-1]
            prev = dxy_close.iloc[-21]
            if now < prev:
                score += 1
                notes.append("달러 약세")
            else:
                score -= 1
                notes.append("달러 강세")
    except Exception:
        notes.append("DXY 오류")

    # 금 ETF
    try:
        gld_close, _ = get_history("GLD", period="6mo")
        if len(gld_close) >= 21:
            now = gld_close.iloc[-1]
            prev = gld_close.iloc[-21]
            # 금이 너무 강하면 위험회피 심리로 해석
            if now > prev:
                score -= 1
                notes.append("금 강세")
            else:
                score += 0.5
                notes.append("금 안정/약세")
    except Exception:
        notes.append("GLD 오류")

    if score >= 1.5:
        state = "🟢 유동성 우호"
    elif score <= -1.0:
        state = "🔴 유동성 부담"
    else:
        state = "🟡 유동성 중립"

    return {
        "score": round(score, 1),
        "state": state,
        "notes": notes
    }


# =========================================
# 시장 엔진
# =========================================
def market_engine():
    score = 0
    notes = []

    try:
        spy_close, _ = get_history("SPY", period="1y")
        qqq_close, _ = get_history("QQQ", period="1y")
        vix_close, _ = get_history("^VIX", period="3mo")

        if len(spy_close) >= 200:
            spy_ma200 = spy_close.rolling(200).mean().iloc[-1]
            if spy_close.iloc[-1] > spy_ma200:
                score += 2
                notes.append("SPY > 200MA")
            else:
                score -= 2
                notes.append("SPY < 200MA")

        if len(qqq_close) >= 200:
            qqq_ma200 = qqq_close.rolling(200).mean().iloc[-1]
            if qqq_close.iloc[-1] > qqq_ma200:
                score += 2
                notes.append("QQQ > 200MA")
            else:
                score -= 2
                notes.append("QQQ < 200MA")

        if len(vix_close) > 0:
            vix_now = vix_close.iloc[-1]
            if vix_now < 18:
                score += 1
                notes.append("VIX 안정")
            elif vix_now > 25:
                score -= 1
                notes.append("VIX 위험")
            else:
                notes.append("VIX 중간")

    except Exception:
        notes.append("시장 지표 오류")

    if score >= 3:
        state = "🟢 Risk ON"
    elif score <= -2:
        state = "🔴 Risk OFF"
    else:
        state = "🟡 Neutral"

    return {
        "score": score,
        "state": state,
        "notes": notes
    }


# =========================================
# 뉴스 엔진
# 각 섹터별 뉴스 감성 간이 점수
# 키 없으면 0점
# =========================================
positive_keywords = [
    "partnership", "contract", "approval", "expansion", "breakthrough",
    "launch", "wins", "investment", "funding", "surge", "record", "growth"
]

negative_keywords = [
    "delay", "lawsuit", "probe", "cut", "miss", "downgrade",
    "decline", "fraud", "bankruptcy", "loss", "warning", "recall"
]

sector_queries = {
    "양자": "quantum computing",
    "우주": "space industry OR satellite launch",
    "장수": "longevity biotech OR gene editing",
    "로봇": "robotics automation humanoid"
}


def sector_news_score(sector_name: str):
    if not NEWS_API_KEY:
        return {
            "score": 0,
            "summary": "뉴스 API 키 없음"
        }

    query = sector_queries.get(sector_name, sector_name)

    try:
        url = "https://newsapi.org/v2/everything"
        params = {
            "q": query,
            "sortBy": "publishedAt",
            "language": "en",
            "pageSize": 10,
            "apiKey": NEWS_API_KEY
        }
        r = requests.get(url, params=params, timeout=20)
        data = r.json()

        articles = data.get("articles", [])
        score = 0

        for art in articles:
            text = f"{art.get('title','')} {art.get('description','')}".lower()
            for kw in positive_keywords:
                if kw in text:
                    score += 1
            for kw in negative_keywords:
                if kw in text:
                    score -= 1

        if score > 3:
            summary = "뉴스 우호"
        elif score < -2:
            summary = "뉴스 부정"
        else:
            summary = "뉴스 중립"

        return {
            "score": score,
            "summary": summary
        }

    except Exception:
        return {
            "score": 0,
            "summary": "뉴스 조회 오류"
        }


# =========================================
# 섹터 모멘텀 엔진
# =========================================
def sector_momentum_engine():
    results = []

    for sector_name, tickers in future_map.items():
        stock_scores = []
        for ticker in tickers:
            try:
                close, vol = get_history(ticker, period="6mo")
                if len(close) < 60:
                    continue

                ret_20 = (close.iloc[-1] / close.iloc[-21] - 1) * 100
                ret_60 = (close.iloc[-1] / close.iloc[-61] - 1) * 100
                ma50 = close.rolling(50).mean().iloc[-1]

                s = 0
                if close.iloc[-1] > ma50:
                    s += 40
                if ret_20 > 0:
                    s += 30
                if ret_60 > 0:
                    s += 30

                stock_scores.append(s)
            except Exception:
                pass

        price_score = round(np.mean(stock_scores), 1) if stock_scores else 0
        news_data = sector_news_score(sector_name)

        final_score = round(price_score * 0.6 + news_data["score"] * 4, 1)

        if final_score >= 60:
            state = "강함"
        elif final_score >= 40:
            state = "보통"
        else:
            state = "약함"

        results.append({
            "sector": sector_name,
            "price_score": price_score,
            "news_score": news_data["score"],
            "news_summary": news_data["summary"],
            "final_score": final_score,
            "state": state
        })

    return sorted(results, key=lambda x: x["final_score"], reverse=True)


# =========================================
# 종목 점수 엔진
# =========================================
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
            "price": round(last_close, 2),
            "rsi": round(last_rsi, 1),
            "ma200": round(last_ma200, 2)
        }
    except Exception:
        return None


# =========================================
# -20% 포지션 점검 엔진
# 펀더멘털은 간이 버전
# =========================================
def drawdown_check(ticker: str, avg_price: float, shares: int):
    try:
        close, _ = get_history(ticker, period="1y")
        if len(close) < 200:
            return None

        current_price = float(close.iloc[-1])
        pnl_pct = (current_price / avg_price - 1) * 100

        if pnl_pct > -20:
            return None

        ma200 = close.rolling(200).mean().iloc[-1]
        current_rsi = rsi(close).iloc[-1]

        # 간이 펀더멘털
        f_ok = "보통"
        try:
            info = yf.Ticker(ticker).info
            revenue_growth = info.get("revenueGrowth")
            earnings_growth = info.get("earningsQuarterlyGrowth")

            good_count = 0
            if revenue_growth is not None and revenue_growth > 0.10:
                good_count += 1
            if earnings_growth is not None and earnings_growth > 0:
                good_count += 1

            if good_count >= 2:
                f_ok = "양호"
            elif good_count == 1:
                f_ok = "보통"
            else:
                f_ok = "약함"
        except Exception:
            pass

        if f_ok == "양호" and current_price > ma200:
            action = "보류/추가매수 검토"
        elif f_ok == "보통" and current_rsi < 35:
            action = "보류"
        else:
            action = "감축 검토"

        return {
            "ticker": ticker,
            "current_price": round(current_price, 2),
            "pnl_pct": round(pnl_pct, 1),
            "fundamental": f_ok,
            "rsi": round(float(current_rsi), 1),
            "action": action
        }
    except Exception:
        return None


# =========================================
# 포트폴리오 평가 + 원화 기준 계산
# =========================================
def portfolio_engine(usdkrw: float):
    rows = []
    total_eval_krw = 0

    for ticker, data in portfolio.items():
        try:
            close, _ = get_history(ticker, period="6mo")
            if len(close) == 0:
                continue

            current_price = float(close.iloc[-1])
            eval_usd = current_price * data["shares"]
            eval_krw = eval_usd * usdkrw

            pnl_usd = (current_price - data["avg"]) * data["shares"]
            pnl_krw = pnl_usd * usdkrw

            total_eval_krw += eval_krw

            rows.append({
                "ticker": ticker,
                "avg_price": data["avg"],
                "shares": data["shares"],
                "current_price": round(current_price, 2),
                "eval_krw": round(eval_krw),
                "pnl_krw": round(pnl_krw),
                "pnl_pct": round((current_price / data["avg"] - 1) * 100, 1)
            })
        except Exception:
            pass

    return rows, round(total_eval_krw)


# =========================================
# 목표 비중 엔진
# - core 45%
# - future 40%
# - cash 15%
# future는 양자/우주/장수/로봇 비슷하게
# =========================================
def target_allocation_engine(market_state: str, liquidity_state: str):
    cash_ratio = BASE_CASH_RATIO

    if market_state == "🔴 Risk OFF" or liquidity_state == "🔴 유동성 부담":
        cash_ratio = 0.30
    elif market_state == "🟡 Neutral":
        cash_ratio = 0.20
    else:
        cash_ratio = 0.10

    invest_ratio = 1 - cash_ratio

    core_ratio = min(CORE_TOTAL_RATIO, invest_ratio * 0.55)
    future_ratio = invest_ratio - core_ratio

    each_future_sector = future_ratio / 4

    return {
        "cash_ratio": round(cash_ratio, 2),
        "core_ratio": round(core_ratio, 2),
        "future_ratio": round(future_ratio, 2),
        "future_each": round(each_future_sector, 4)
    }


# =========================================
# 1주 단위 매수/매도 제안
# =========================================
def order_suggestion_engine(
    usdkrw: float,
    top_future,
    alloc
):
    """
    top_future: 섹터별 최고점 종목 1개씩
    """
    suggestions = []

    target_future_total_krw = TOTAL_KRW * alloc["future_ratio"]
    target_each_sector_krw = TOTAL_KRW * alloc["future_each"]

    for sector_name, stock_data in top_future.items():
        if stock_data is None:
            continue

        ticker = stock_data["ticker"]
        price_usd = stock_data["price"]
        price_krw = price_usd * usdkrw

        target_shares = math.floor(target_each_sector_krw / price_krw)

        current_shares = portfolio.get(ticker, {}).get("shares", 0)
        diff = target_shares - current_shares

        if diff > 0:
            suggestions.append(
                f"{sector_name} | {ticker} | {diff}주 매수 제안 | 1주 약 {round(price_krw):,}원"
            )
        elif diff < 0:
            suggestions.append(
                f"{sector_name} | {ticker} | {abs(diff)}주 매도 제안 | 1주 약 {round(price_krw):,}원"
            )
        else:
            suggestions.append(
                f"{sector_name} | {ticker} | 현재 적정 비중"
            )

    return suggestions


# =========================================
# 메인
# =========================================
def main():
    usdkrw = get_usdkrw()
    market = market_engine()
    liquidity = liquidity_engine()
    sector_scores = sector_momentum_engine()

    # 미래 섹터별 최고 종목 1개씩 선정
    top_future = {}
    for sector_name, tickers in future_map.items():
        scored = []
        for t in tickers:
            result = score_stock(t)
            if result:
                scored.append(result)
        scored = sorted(scored, key=lambda x: x["score"], reverse=True)
        top_future[sector_name] = scored[0] if scored else None

    portfolio_rows, total_eval_krw = portfolio_engine(usdkrw)
    alloc = target_allocation_engine(market["state"], liquidity["state"])

    dd_checks = []
    for ticker, data in portfolio.items():
        chk = drawdown_check(ticker, data["avg"], data["shares"])
        if chk:
            dd_checks.append(chk)

    order_suggestions = order_suggestion_engine(usdkrw, top_future, alloc)

    # 메시지 생성
    lines = []
    lines.append("📊 AI 통합 포트폴리오 리포트")
    lines.append("")
    lines.append(f"환율(USD/KRW): {usdkrw:,.0f}원")
    lines.append(f"총자산 기준: {TOTAL_KRW:,}원")
    lines.append("")
    lines.append(f"시장 상태: {market['state']} (점수 {market['score']})")
    lines.append(f"유동성 상태: {liquidity['state']} (점수 {liquidity['score']})")
    lines.append("")
    lines.append("💧 유동성 메모")
    for n in liquidity["notes"][:5]:
        lines.append(f"- {n}")

    lines.append("")
    lines.append("🏭 미래 섹터 점수")
    for item in sector_scores:
        lines.append(
            f"- {item['sector']} | 종합 {item['final_score']} | 가격점수 {item['price_score']} | 뉴스 {item['news_summary']}"
        )

    lines.append("")
    lines.append("🎯 목표 자금 배분")
    lines.append(f"- 현금: {int(alloc['cash_ratio'] * 100)}%")
    lines.append(f"- 코어: {int(alloc['core_ratio'] * 100)}%")
    lines.append(f"- 미래섹터 합계: {int(alloc['future_ratio'] * 100)}%")
    lines.append(f"- 미래섹터 각: 약 {int(alloc['future_each'] * 100)}%")

    lines.append("")
    lines.append("📦 현재 포트폴리오")
    for row in portfolio_rows:
        lines.append(
            f"- {row['ticker']} | {row['shares']}주 | 현재가 ${row['current_price']} | "
            f"평가 {row['eval_krw']:,}원 | 손익 {row['pnl_krw']:,}원 ({row['pnl_pct']}%)"
        )

    lines.append("")
    lines.append("🩺 -20% 이하 점검")
    if dd_checks:
        for item in dd_checks:
            lines.append(
                f"- {item['ticker']} | 손익 {item['pnl_pct']}% | 실적 {item['fundamental']} | RSI {item['rsi']} | 판단 {item['action']}"
            )
    else:
        lines.append("- 해당 없음")

    lines.append("")
    lines.append("🚀 미래 섹터 대표 종목")
    for sector_name, data in top_future.items():
        if data:
            lines.append(
                f"- {sector_name} | {data['ticker']} | 점수 {data['score']} | 현재가 ${data['price']} | RSI {data['rsi']}"
            )
        else:
            lines.append(f"- {sector_name} | 후보 없음")

    lines.append("")
    lines.append("🧾 1주 단위 실행 제안")
    for s in order_suggestions:
        lines.append(f"- {s}")

    message = "\n".join(lines)
    send_telegram(message)


if __name__ == "__main__":
    main()

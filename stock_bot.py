import os
import math
import requests
import yfinance as yf
import pandas as pd
import numpy as np

# =========================================================
# 환경변수
# =========================================================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
NEWS_API_KEY = os.getenv("NEWS_API_KEY")

# 총자산 원화 기준
TOTAL_KRW = 7_000_000

# 기본 비중
BASE_CASH_RATIO = 0.15
CORE_RATIO_DEFAULT = 0.55
FUTURE_RATIO_DEFAULT = 0.30

# 미래 섹터 수
FUTURE_SECTOR_COUNT = 4


# =========================================================
# 현재 포트폴리오
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
# 섹터 정의
# =========================================================
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

rotation_pairs = {
    "PANW": "V",
    "CRWD": "KMI",
    "OKTA": "UNH",
    "ZS": "COST",
    "AMT": "EQIX",
    "COIN": "TLT"
}


# =========================================================
# 텔레그램
# =========================================================
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


# =========================================================
# 유틸
# =========================================================
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


def get_history(ticker: str, period="1y"):
    df = yf.download(
        ticker,
        period=period,
        interval="1d",
        progress=False,
        auto_adjust=False
    )

    if isinstance(df, pd.DataFrame) and not df.empty:
        close = df["Close"].squeeze().dropna()
        volume = df["Volume"].squeeze().dropna() if "Volume" in df else pd.Series(dtype=float)
        return close, volume

    return pd.Series(dtype=float), pd.Series(dtype=float)


def get_usdkrw():
    try:
        close, _ = get_history("KRW=X", period="10d")
        if len(close) > 0:
            val = float(close.iloc[-1])
            if val < 10:
                return round(1 / val, 2)
    except Exception:
        pass

    try:
        close, _ = get_history("USDKRW=X", period="10d")
        if len(close) > 0:
            return round(float(close.iloc[-1]), 2)
    except Exception:
        pass

    return 1350.0


# =========================================================
# 점수 → 등급
# =========================================================
def market_grade(score: int):
    if score >= 5:
        return "최고의 안전"
    elif score >= 3:
        return "안전"
    elif score >= 1:
        return "보통"
    elif score >= -1:
        return "위험"
    else:
        return "엄청위험"


def liquidity_grade(score: float):
    if score >= 2:
        return "최고의 안전"
    elif score >= 1:
        return "안전"
    elif score >= 0:
        return "보통"
    elif score >= -1:
        return "위험"
    else:
        return "엄청위험"


# =========================================================
# 시장 엔진
# =========================================================
def market_engine():
    score = 0
    notes = []
    details = []

    try:
        spy_close, _ = get_history("SPY", period="1y")
        qqq_close, _ = get_history("QQQ", period="1y")
        vix_close, _ = get_history("^VIX", period="3mo")

        if len(spy_close) >= 200:
            spy_ma200 = float(spy_close.rolling(200).mean().iloc[-1])
            spy_now = float(spy_close.iloc[-1])

            if spy_now > spy_ma200:
                score += 2
                notes.append("SPY > 200MA")
                details.append(f"SPY {round(spy_now,2)} > 200MA {round(spy_ma200,2)} : +2")
            else:
                score -= 2
                notes.append("SPY < 200MA")
                details.append(f"SPY {round(spy_now,2)} < 200MA {round(spy_ma200,2)} : -2")

        if len(qqq_close) >= 200:
            qqq_ma200 = float(qqq_close.rolling(200).mean().iloc[-1])
            qqq_now = float(qqq_close.iloc[-1])

            if qqq_now > qqq_ma200:
                score += 2
                notes.append("QQQ > 200MA")
                details.append(f"QQQ {round(qqq_now,2)} > 200MA {round(qqq_ma200,2)} : +2")
            else:
                score -= 2
                notes.append("QQQ < 200MA")
                details.append(f"QQQ {round(qqq_now,2)} < 200MA {round(qqq_ma200,2)} : -2")

        if len(vix_close) > 0:
            vix_now = float(vix_close.iloc[-1])

            if vix_now < 18:
                score += 1
                notes.append("VIX 안정")
                details.append(f"VIX {round(vix_now,2)} < 18 : +1")
            elif vix_now > 25:
                score -= 1
                notes.append("VIX 위험")
                details.append(f"VIX {round(vix_now,2)} > 25 : -1")
            else:
                details.append(f"VIX {round(vix_now,2)} 중간 : 0")

    except Exception:
        notes.append("시장 데이터 오류")
        details.append("시장 데이터 오류")

    if score >= 3:
        state = "🟢 Risk ON"
    elif score <= -2:
        state = "🔴 Risk OFF"
    else:
        state = "🟡 Neutral"

    return {
        "score": score,
        "state": state,
        "grade": market_grade(score),
        "notes": notes,
        "details": details
    }


# =========================================================
# 유동성 엔진
# =========================================================
def liquidity_engine():
    score = 0
    notes = []
    details = []

    try:
        tnx_close, _ = get_history("^TNX", period="6mo")
        if len(tnx_close) >= 21:
            now = float(tnx_close.iloc[-1])
            prev = float(tnx_close.iloc[-21])

            if now < prev:
                score += 1
                notes.append("10년물 금리 하락")
                details.append(f"10Y {round(prev,2)} → {round(now,2)} : +1")
            else:
                score -= 1
                notes.append("10년물 금리 상승")
                details.append(f"10Y {round(prev,2)} → {round(now,2)} : -1")
    except Exception:
        notes.append("10년물 오류")
        details.append("10년물 오류")

    try:
        dxy_close, _ = get_history("DX-Y.NYB", period="6mo")
        if len(dxy_close) >= 21:
            now = float(dxy_close.iloc[-1])
            prev = float(dxy_close.iloc[-21])

            if now < prev:
                score += 1
                notes.append("달러 약세")
                details.append(f"DXY {round(prev,2)} → {round(now,2)} : +1")
            else:
                score -= 1
                notes.append("달러 강세")
                details.append(f"DXY {round(prev,2)} → {round(now,2)} : -1")
    except Exception:
        notes.append("달러 오류")
        details.append("달러 오류")

    try:
        gld_close, _ = get_history("GLD", period="6mo")
        if len(gld_close) >= 21:
            now = float(gld_close.iloc[-1])
            prev = float(gld_close.iloc[-21])

            if now > prev:
                score -= 1
                notes.append("금 강세")
                details.append(f"GLD {round(prev,2)} → {round(now,2)} : -1")
            else:
                score += 0.5
                notes.append("금 안정/약세")
                details.append(f"GLD {round(prev,2)} → {round(now,2)} : +0.5")
    except Exception:
        notes.append("금 오류")
        details.append("금 오류")

    if score >= 1.5:
        state = "🟢 유동성 우호"
    elif score <= -1.0:
        state = "🔴 유동성 부담"
    else:
        state = "🟡 유동성 중립"

    return {
        "score": round(score, 1),
        "state": state,
        "grade": liquidity_grade(score),
        "notes": notes,
        "details": details
    }


# =========================================================
# 뉴스 엔진
# =========================================================
positive_keywords = [
    "partnership", "contract", "approval", "expansion", "breakthrough",
    "launch", "wins", "investment", "funding", "record", "growth"
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
        return {"score": 0, "summary": "뉴스 API 키 없음"}

    try:
        url = "https://newsapi.org/v2/everything"
        params = {
            "q": sector_queries.get(sector_name, sector_name),
            "sortBy": "publishedAt",
            "language": "en",
            "pageSize": 10,
            "apiKey": NEWS_API_KEY
        }
        r = requests.get(url, params=params, timeout=20)
        data = r.json()

        score = 0
        for art in data.get("articles", []):
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

        return {"score": score, "summary": summary}

    except Exception:
        return {"score": 0, "summary": "뉴스 조회 오류"}


# =========================================================
# 종목 점수 엔진
# =========================================================
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


# =========================================================
# 미래 섹터 엔진
# =========================================================
def future_sector_engine():
    results = []

    for sector_name, tickers in future_map.items():
        stock_scores = []

        for ticker in tickers:
            try:
                close, _ = get_history(ticker, period="6mo")
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


# =========================================================
# 포트폴리오 스냅샷
# =========================================================
def portfolio_snapshot(usdkrw: float):
    rows = []

    for ticker, data in portfolio.items():
        try:
            close, _ = get_history(ticker, period="6mo")
            if len(close) == 0:
                continue

            current_price = float(close.iloc[-1])
            eval_usd = current_price * data["shares"]
            eval_krw = eval_usd * usdkrw
            pnl_pct = (current_price / data["avg"] - 1) * 100
            pnl_krw = (current_price - data["avg"]) * data["shares"] * usdkrw

            rows.append({
                "ticker": ticker,
                "avg": data["avg"],
                "shares": data["shares"],
                "current_price": round(current_price, 2),
                "eval_krw": round(eval_krw),
                "pnl_pct": round(pnl_pct, 1),
                "pnl_krw": round(pnl_krw)
            })
        except Exception:
            pass

    return rows


# =========================================================
# -20% 점검
# =========================================================
def drawdown_check(ticker: str, avg_price: float, shares: int):
    try:
        close, _ = get_history(ticker, period="1y")
        if len(close) < 200:
            return None

        current_price = float(close.iloc[-1])
        pnl_pct = (current_price / avg_price - 1) * 100
        if pnl_pct > -20:
            return None

        ma200 = float(close.rolling(200).mean().iloc[-1])
        current_rsi = float(rsi(close).iloc[-1])

        f_state = "보통"
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
                f_state = "양호"
            elif good_count == 0:
                f_state = "약함"
        except Exception:
            pass

        if f_state == "양호" and current_price > ma200:
            action = "보류/추가매수 검토"
        elif f_state == "보통" and current_rsi < 35:
            action = "보류"
        else:
            action = "감축 검토"

        return {
            "ticker": ticker,
            "pnl_pct": round(pnl_pct, 1),
            "fundamental": f_state,
            "rsi": round(current_rsi, 1),
            "action": action
        }

    except Exception:
        return None


# =========================================================
# 목표 비중 엔진
# =========================================================
def target_allocation_engine(market_state: str, liquidity_state: str):
    cash_ratio = BASE_CASH_RATIO
    core_ratio = CORE_RATIO_DEFAULT
    future_ratio = FUTURE_RATIO_DEFAULT

    if market_state == "🔴 Risk OFF" or liquidity_state == "🔴 유동성 부담":
        cash_ratio = 0.30
        core_ratio = 0.50
        future_ratio = 0.20
    elif market_state == "🟡 Neutral":
        cash_ratio = 0.20
        core_ratio = 0.50
        future_ratio = 0.30
    else:
        cash_ratio = 0.10
        core_ratio = 0.45
        future_ratio = 0.45

    return {
        "cash_ratio": round(cash_ratio, 2),
        "core_ratio": round(core_ratio, 2),
        "future_ratio": round(future_ratio, 2),
        "future_each_ratio": round(future_ratio / FUTURE_SECTOR_COUNT, 4)
    }


# =========================================================
# 통행료 포트폴리오 리포트
# =========================================================
def core_portfolio_report(usdkrw: float):
    lines = []
    lines.append("🛣 통행료 포트폴리오 리포트")
    lines.append("")

    core_holdings = [x for x in portfolio_snapshot(usdkrw) if x["ticker"] in core_stocks]

    if not core_holdings:
        lines.append("- 현재 보유 종목 없음")
        return "\n".join(lines)

    for row in core_holdings:
        action = "HOLD"
        reason = "기본 유지"

        try:
            close, _ = get_history(row["ticker"], period="1y")
            if len(close) >= 200:
                ma200 = float(close.rolling(200).mean().iloc[-1])
                current_rsi = float(rsi(close).iloc[-1])
                current_price = float(close.iloc[-1])

                if current_rsi > 70 and row["pnl_pct"] > 10:
                    action = "부분매도 검토"
                    reason = "과열"
                elif current_price < ma200 and row["pnl_pct"] < -8:
                    action = "관망"
                    reason = "장기선 아래"
                elif current_rsi < 35 and current_price > ma200:
                    action = "추가매수 검토"
                    reason = "과매도 + 추세 유지"
        except Exception:
            pass

        lines.append(
            f"- {row['ticker']} | {row['shares']}주 | 평가 {row['eval_krw']:,}원 | "
            f"손익 {row['pnl_krw']:,}원 ({row['pnl_pct']}%) | {action} | {reason}"
        )

    hints = []
    for src, dst in rotation_pairs.items():
        if src not in portfolio:
            continue
        try:
            close, _ = get_history(src, period="6mo")
            if len(close) > 30:
                src_rsi = float(rsi(close).iloc[-1])
                if src_rsi > 70:
                    hints.append(f"{src} 과열 → {dst} 이동 검토")
        except Exception:
            pass

    lines.append("")
    lines.append("🔄 코어 로테이션 힌트")
    if hints:
        for h in hints:
            lines.append(f"- {h}")
    else:
        lines.append("- 현재 강한 로테이션 없음")

    return "\n".join(lines)


# =========================================================
# 미래 성장 포트폴리오 리포트
# =========================================================
def future_portfolio_report(usdkrw: float, market_state: str, liquidity_state: str):
    alloc = target_allocation_engine(market_state, liquidity_state)
    sector_scores = future_sector_engine()

    sector_top = {}
    for sector_name, tickers in future_map.items():
        scored = []
        for t in tickers:
            result = score_stock(t)
            if result:
                scored.append(result)
        scored = sorted(scored, key=lambda x: x["score"], reverse=True)
        sector_top[sector_name] = scored[0] if scored else None

    lines = []
    lines.append("🚀 미래 성장 포트폴리오 리포트")
    lines.append("")
    lines.append(f"- 미래섹터 총 비중 목표: {int(alloc['future_ratio'] * 100)}%")
    lines.append(f"- 섹터당 목표: 약 {int(alloc['future_each_ratio'] * 100)}%")
    lines.append("")

    lines.append("🏭 미래 섹터 점수")
    for item in sector_scores:
        lines.append(
            f"- {item['sector']} | 종합 {item['final_score']} | 가격 {item['price_score']} | "
            f"뉴스 {item['news_summary']} | {item['state']}"
        )

    lines.append("")
    lines.append("🌱 섹터별 대표 종목")
    for sector_name, data in sector_top.items():
        if data:
            lines.append(
                f"- {sector_name} | {data['ticker']} | 점수 {data['score']} | 현재가 ${data['price']} | RSI {data['rsi']}"
            )
        else:
            lines.append(f"- {sector_name} | 대표 종목 없음")

    lines.append("")
    lines.append("🧾 1주 단위 실행 제안")

    target_each_krw = TOTAL_KRW * alloc["future_each_ratio"]

    for sector_name, data in sector_top.items():
        if not data:
            lines.append(f"- {sector_name} | 종목 없음")
            continue

        ticker = data["ticker"]
        current_price_krw = data["price"] * usdkrw
        target_shares = math.floor(target_each_krw / current_price_krw)

        current_shares = portfolio.get(ticker, {}).get("shares", 0)
        diff = target_shares - current_shares

        # seed 최소 1주
        if current_shares > 0 and diff < 0:
            if current_shares + diff < 1:
                diff = -(current_shares - 1)

        if diff > 0:
            lines.append(
                f"- {sector_name} | {ticker} | {diff}주 매수 제안 | 1주 약 {round(current_price_krw):,}원"
            )
        elif diff < 0:
            lines.append(
                f"- {sector_name} | {ticker} | {abs(diff)}주 매도 제안 | 1주 약 {round(current_price_krw):,}원"
            )
        else:
            lines.append(f"- {sector_name} | {ticker} | 현재 적정 비중")

    return "\n".join(lines)


# =========================================================
# 손실 리포트
# =========================================================
def drawdown_report():
    lines = []
    lines.append("🩺 -20% 손실 종목 점검")
    lines.append("")

    found = False
    for ticker, data in portfolio.items():
        chk = drawdown_check(ticker, data["avg"], data["shares"])
        if chk:
            found = True
            lines.append(
                f"- {chk['ticker']} | 손익 {chk['pnl_pct']}% | 실적 {chk['fundamental']} | "
                f"RSI {chk['rsi']} | 판단 {chk['action']}"
            )

    if not found:
        lines.append("- 해당 없음")

    return "\n".join(lines)


# =========================================================
# 메인
# =========================================================
def main():
    usdkrw = get_usdkrw()
    market = market_engine()
    liquidity = liquidity_engine()

    header_lines = []
    header_lines.append("📊 AI 투자 시스템 2.1")
    header_lines.append("")
    header_lines.append(f"환율(USD/KRW): {usdkrw:,.0f}원")
    header_lines.append(f"총자산 기준: {TOTAL_KRW:,}원")
    header_lines.append("")
    header_lines.append(f"시장 상태: {market['state']} | 점수 {market['score']} | 등급 {market['grade']}")
    header_lines.append("📈 시장 세부")
    for d in market["details"]:
        header_lines.append(f"- {d}")

    header_lines.append("")
    header_lines.append(f"유동성 상태: {liquidity['state']} | 점수 {liquidity['score']} | 등급 {liquidity['grade']}")
    header_lines.append("💧 유동성 세부")
    for d in liquidity["details"]:
        header_lines.append(f"- {d}")

    header = "\n".join(header_lines)
    core_report = core_portfolio_report(usdkrw)
    future_report = future_portfolio_report(usdkrw, market["state"], liquidity["state"])
    dd_report = drawdown_report()

    send_telegram(header)
    send_telegram(core_report)
    send_telegram(future_report)
    send_telegram(dd_report)


if __name__ == "__main__":
    main()

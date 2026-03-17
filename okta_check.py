import os
import requests
import yfinance as yf
import pandas as pd
import numpy as np

# ====================================
# 텔레그램 설정
# ====================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def send_telegram(msg: str):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print(msg)
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {
        "chat_id": CHAT_ID,
        "text": msg
    }
    requests.post(url, data=data, timeout=20)


# ====================================
# 후보 종목 60
# ====================================

stocks = [
    "NVDA", "AMD", "AVGO", "ASML", "TSM", "LRCX", "AMAT", "KLAC", "MRVL", "ARM",
    "PANW", "CRWD", "ZS", "OKTA", "FTNT", "NET", "DDOG", "CYBR", "TENB", "S",
    "MSFT", "AMZN", "GOOGL", "META", "NOW", "SNOW", "ORCL", "CRM", "ACN", "PLTR",
    "V", "MA", "AXP", "PYPL", "COIN", "ICE", "CME", "ADP", "FIS", "GPN",
    "AMT", "EQIX", "PLD", "CCI", "DLR", "KMI", "WMB", "ENB", "NEE", "CEG",
    "UNH", "MCK", "COST", "PG", "KO", "PEP", "ABBV", "LLY", "AWK", "XLU"
]

# ====================================
# 현재 포트폴리오
# ====================================

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

# ====================================
# 섹터 맵
# ====================================

sector_map = {
    "AI_반도체": ["NVDA", "AMD", "AVGO", "ASML", "TSM", "LRCX", "AMAT", "KLAC", "MRVL", "ARM"],
    "사이버보안": ["PANW", "CRWD", "ZS", "OKTA", "FTNT", "NET", "DDOG", "CYBR", "TENB", "S"],
    "클라우드_플랫폼": ["MSFT", "AMZN", "GOOGL", "META", "NOW", "SNOW", "ORCL", "CRM", "ACN", "PLTR"],
    "금융인프라": ["V", "MA", "AXP", "PYPL", "COIN", "ICE", "CME", "ADP", "FIS", "GPN"],
    "인프라_리츠": ["AMT", "EQIX", "PLD", "CCI", "DLR", "KMI", "WMB", "ENB", "NEE", "CEG"],
    "방어_헬스_필수": ["UNH", "MCK", "COST", "PG", "KO", "PEP", "ABBV", "LLY", "AWK", "XLU"]
}


# ====================================
# 지표 함수
# ====================================

def rsi(series: pd.Series, period: int = 14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


# ====================================
# 시장 상태
# ====================================

def market_state():
    try:
        spy = yf.download("SPY", period="1y", interval="1d", progress=False)
        qqq = yf.download("QQQ", period="1y", interval="1d", progress=False)
        vix = yf.download("^VIX", period="3mo", interval="1d", progress=False)

        spy_close = spy["Close"].squeeze()
        qqq_close = qqq["Close"].squeeze()
        vix_close = vix["Close"].squeeze()

        spy_ma200 = spy_close.rolling(200).mean().iloc[-1]
        qqq_ma200 = qqq_close.rolling(200).mean().iloc[-1]

        spy_now = spy_close.iloc[-1]
        qqq_now = qqq_close.iloc[-1]
        vix_now = vix_close.iloc[-1]

        score = 0

        if spy_now > spy_ma200:
            score += 2
        if qqq_now > qqq_ma200:
            score += 2
        if vix_now < 18:
            score += 1

        if score >= 4:
            return "🟢 Risk ON"
        elif score >= 2:
            return "🟡 Neutral"
        else:
            return "🔴 Risk OFF"
    except Exception:
        return "🟡 Neutral"


# ====================================
# 섹터 강도 엔진
# ====================================

def sector_strength():
    result = []

    for sector_name, tickers in sector_map.items():
        scores = []

        for ticker in tickers:
            try:
                df = yf.download(ticker, period="6mo", interval="1d", progress=False)
                close = df["Close"].squeeze().dropna()
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

                scores.append(s)
            except Exception:
                pass

        if len(scores) > 0:
            avg_score = round(float(np.mean(scores)), 1)

            if avg_score >= 75:
                state = "강함"
            elif avg_score >= 55:
                state = "보통"
            else:
                state = "약함"

            result.append({
                "sector": sector_name,
                "score": avg_score,
                "state": state
            })

    return sorted(result, key=lambda x: x["score"], reverse=True)


# ====================================
# 종목 점수 엔진
# ====================================

def score_stock(ticker: str):
    try:
        df = yf.download(ticker, period="1y", interval="1d", progress=False)
        close = df["Close"].squeeze().dropna()
        vol = df["Volume"].squeeze().dropna()

        if len(close) < 200:
            return None

        ma200 = close.rolling(200).mean()
        r = rsi(close)

        last_close = float(close.iloc[-1])
        last_ma200 = float(ma200.iloc[-1])
        last_rsi = float(r.iloc[-1])

        score = 0

        # 추세
        if last_close > last_ma200:
            score += 35

        # RSI
        if last_rsi < 35:
            score += 30
        elif last_rsi < 45:
            score += 15

        # 거래량
        vol_avg20 = float(vol.rolling(20).mean().iloc[-1])
        if float(vol.iloc[-1]) > vol_avg20:
            score += 20

        # 볼린저
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


# ====================================
# 포트폴리오 분석 엔진
# ====================================

def analyze_portfolio():
    lines = []
    lines.append("📦 포트폴리오 상태")

    for ticker, data in portfolio.items():
        try:
            df = yf.download(ticker, period="6mo", interval="1d", progress=False)
            close = df["Close"].squeeze().dropna()

            if len(close) < 60:
                continue

            current_price = float(close.iloc[-1])
            pnl = (current_price - data["avg"]) * data["shares"]
            pnl_pct = ((current_price / data["avg"]) - 1) * 100

            ma200 = close.rolling(min(200, len(close))).mean().iloc[-1]
            current_rsi = rsi(close).iloc[-1]

            action = "HOLD"
            reason = "추세 유지"

            if current_rsi > 70:
                action = "부분매도"
                reason = "RSI 과열"
            elif current_price < ma200 and pnl_pct < -8:
                action = "관망"
                reason = "장기선 아래 + 손실구간"
            elif current_rsi < 35 and current_price > ma200:
                action = "추가매수 검토"
                reason = "과매도 + 추세 유지"

            lines.append(
                f"{ticker} | 현재가 {round(current_price,2)} | 손익 {round(pnl,2)}달러 ({round(pnl_pct,1)}%) | {action} | {reason}"
            )
        except Exception:
            lines.append(f"{ticker} | 데이터 오류")

    return "\n".join(lines)


# ====================================
# 로테이션 힌트
# ====================================

rotation_pairs = {
    "NVDA": "GLD",
    "COIN": "TLT",
    "CRWD": "KMI",
    "PANW": "V",
    "MSFT": "AMT",
    "OKTA": "UNH",
    "ZS": "COST"
}

def rotation_hints():
    lines = []
    lines.append("🔄 로테이션 힌트")

    for src, dst in rotation_pairs.items():
        if src in portfolio:
            try:
                df = yf.download(src, period="6mo", interval="1d", progress=False)
                close = df["Close"].squeeze().dropna()
                current_rsi = float(rsi(close).iloc[-1])

                if current_rsi > 70:
                    lines.append(f"{src} 과열 → {dst} 쪽으로 일부 이동 검토")
            except Exception:
                pass

    if len(lines) == 1:
        lines.append("현재 강한 로테이션 신호 없음")

    return "\n".join(lines)


# ====================================
# 메인
# ====================================

def main():
    m_state = market_state()
    sectors = sector_strength()

    results = []
    for s in stocks:
        r = score_stock(s)
        if r:
            results.append(r)

    df = pd.DataFrame(results)
    top30 = df.sort_values("score", ascending=False).head(30)
    top5 = top30.head(5)

    msg = "📊 AI 통행료 로테이션 리포트\n\n"
    msg += f"시장 상태: {m_state}\n\n"

    msg += "🏭 섹터 강도 TOP5\n"
    for item in sectors[:5]:
        msg += f"{item['sector']} | 점수 {item['score']} | {item['state']}\n"

    msg += "\n"
    msg += analyze_portfolio()
    msg += "\n\n"
    msg += rotation_hints()
    msg += "\n\n"
    msg += "🔥 TOP5 기회 종목\n"

    for _, row in top5.iterrows():
        msg += (
            f"{row['ticker']} | 점수 {row['score']} | 현재가 {row['price']} | "
            f"RSI {row['rsi']} | MA200 {row['ma200']}\n"
        )

    send_telegram(msg)


if __name__ == "__main__":
    main()

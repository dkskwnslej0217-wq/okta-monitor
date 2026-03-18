import os
import json
import requests
import pandas as pd
import yfinance as yf
import gspread
from google.oauth2.service_account import Credentials

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
SHEET_ID = os.getenv("SHEET_ID")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")


def send_telegram(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": message})


def get_sheet():
    info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SHEET_ID).worksheet("PORTFOLIO")


def load_portfolio():
    ws = get_sheet()
    data = ws.get_all_records()
    df = pd.DataFrame(data)
    if not df.empty:
        df.columns = [str(c).strip().lower() for c in df.columns]
    return df


def get_signal(ticker: str):
    df = yf.download(
        ticker,
        period="2y",
        interval="1d",
        auto_adjust=True,
        progress=False,
    )

    if df.empty or len(df) < 240:
        return {
            "score": 0,
            "status": "데이터 부족",
            "reason": "가격 데이터 부족",
        }

    close_series = df["Close"]
    if isinstance(close_series, pd.DataFrame):
        close_series = close_series.iloc[:, 0]

    close = float(close_series.iloc[-1])
    ma200 = float(close_series.rolling(200).mean().iloc[-1])
    ma240 = float(close_series.rolling(240).mean().iloc[-1])

    score = 0
    reasons = []

    if close < ma200:
        score += 30
        reasons.append("200일선 이탈")

    if close < ma240:
        score += 25
        reasons.append("240일선 이탈")

    recent_20 = float(close_series.pct_change(20).iloc[-1])
    if recent_20 < -0.08:
        score += 20
        reasons.append("최근 20일 약세")

    if score >= 70:
        status = "정리 검토"
    elif score >= 40:
        status = "감축 검토"
    elif score >= 20:
        status = "주의"
    else:
        status = "관망"

    return {
        "score": score,
        "status": status,
        "reason": ", ".join(reasons) if reasons else "추세 양호",
    }


def main():
    pf = load_portfolio()

    if pf.empty:
        send_telegram("PORTFOLIO 시트가 비어 있습니다.")
        return

    if "ticker" not in pf.columns:
        send_telegram("PORTFOLIO 시트에 ticker 컬럼이 없습니다.")
        return

    lines = ["⚠️ 포트폴리오 위험 스캔", ""]
    alert_count = 0

    for _, row in pf.iterrows():
        ticker = str(row["ticker"]).strip().upper()
        if not ticker or ticker == "NAN":
            continue

        result = get_signal(ticker)

        if result["score"] >= 20:
            alert_count += 1
            lines.append(
                f"- {ticker} | 위험점수 {result['score']} | {result['status']} | {result['reason']}"
            )

    if alert_count == 0:
        lines.append("- 위험 경고 종목 없음")

    send_telegram("\n".join(lines))


if __name__ == "__main__":
    main()

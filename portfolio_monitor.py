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


def send_telegram(message):
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
    return pd.DataFrame(data)


def get_signal(ticker):

    df = yf.download(ticker, period="2y", interval="1d", progress=False)

    if len(df) < 240:
        return 0, "데이터 부족"

    close = df["Close"].iloc[-1]
    ma200 = df["Close"].rolling(200).mean().iloc[-1]
    ma240 = df["Close"].rolling(240).mean().iloc[-1]

    score = 0
    reason = []

    if close < ma200:
        score += 30
        reason.append("200일선 이탈")

    if close < ma240:
        score += 30
        reason.append("240일선 이탈")

    if score >= 60:
        status = "정리 검토"
    elif score >= 30:
        status = "감축 검토"
    else:
        status = "관망"

    return score, status + " / " + ", ".join(reason)


def main():

    pf = load_portfolio()

    message = "⚠️ 포트폴리오 위험 스캔\n\n"

    for _, row in pf.iterrows():

        ticker = row["ticker"]

        score, status = get_signal(ticker)

        if score >= 30:
            message += f"{ticker} | 위험 {score}\n{status}\n\n"

    if message == "⚠️ 포트폴리오 위험 스캔\n\n":
        message += "위험 종목 없음"

    send_telegram(message)


if __name__ == "__main__":
    main()

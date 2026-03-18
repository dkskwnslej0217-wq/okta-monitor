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


def get_close_series(df: pd.DataFrame) -> pd.Series:
    close = df["Close"]
    if isinstance(close, pd.DataFrame):
        close = close.iloc[:, 0]
    return close.dropna()


def safe_last(series: pd.Series, window: int):
    rolled = series.rolling(window).mean()
    value = rolled.iloc[-1]
    if pd.isna(value):
        return None
    return float(value)


def recent_weakness(close: pd.Series) -> bool:
    if len(close) < 21:
        return False
    ret_20 = float(close.pct_change(20).iloc[-1])
    return ret_20 < -0.08


def get_type_weights(asset_type: str):
    asset_type = str(asset_type).strip().lower()

    if asset_type == "core":
        return {
            "ma200": 10,
            "ma240": 15,
            "ma365": 20,
            "recent_weak": 10,
            "status_map": [
                (65, "정리 검토"),
                (45, "감축 검토"),
                (25, "주의"),
                (0, "관망"),
            ],
        }

    if asset_type == "rotation":
        return {
            "ma200": 15,
            "ma240": 20,
            "ma365": 20,
            "recent_weak": 15,
            "status_map": [
                (60, "정리 검토"),
                (40, "감축 검토"),
                (20, "주의"),
                (0, "관망"),
            ],
        }

    # future / 그 외
    return {
        "ma200": 20,
        "ma240": 20,
        "ma365": 20,
        "recent_weak": 15,
        "status_map": [
            (55, "정리 검토"),
            (35, "감축 검토"),
            (15, "주의"),
            (0, "관망"),
        ],
    }


def classify_status(score: int, status_map):
    for threshold, label in status_map:
        if score >= threshold:
            return label
    return "관망"


def get_signal(ticker: str, asset_type: str):
    df = yf.download(
        ticker,
        period="2y",
        interval="1d",
        auto_adjust=True,
        progress=False,
    )

    if df.empty or len(df) < 200:
        return {
            "score": 0,
            "status": "데이터 부족",
            "reason": "가격 데이터 부족",
        }

    close = get_close_series(df)
    if len(close) < 200:
        return {
            "score": 0,
            "status": "데이터 부족",
            "reason": "가격 데이터 부족",
        }

    last_close = float(close.iloc[-1])
    ma200 = safe_last(close, 200)
    ma240 = safe_last(close, 240)
    ma365 = safe_last(close, 365)

    weights = get_type_weights(asset_type)

    score = 0
    reasons = []

    if ma200 is not None and last_close < ma200:
        score += weights["ma200"]
        reasons.append("200일선 이탈")

    if ma240 is not None and last_close < ma240:
        score += weights["ma240"]
        reasons.append("240일선 이탈")

    if ma365 is not None and last_close < ma365:
        score += weights["ma365"]
        reasons.append("365일선 이탈")

    if recent_weakness(close):
        score += weights["recent_weak"]
        reasons.append("최근 20일 약세")

    status = classify_status(score, weights["status_map"])

    return {
        "score": score,
        "status": status,
        "reason": ", ".join(reasons) if reasons else "추세 양호",
    }


def build_message(df: pd.DataFrame):
    lines = ["⚠️ 보유 종목 위험 스캔 v20", ""]

    core_lines = []
    rotation_lines = []
    future_lines = []

    for _, row in df.iterrows():
        ticker = str(row.get("ticker", "")).strip().upper()
        asset_type = str(row.get("type", "")).strip().lower()

        if not ticker or ticker == "NAN":
            continue

        result = get_signal(ticker, asset_type)
        line = (
            f"- {ticker} | {asset_type or 'unknown'} | "
            f"위험점수 {result['score']} | {result['status']} | {result['reason']}"
        )

        if asset_type == "core":
            if result["score"] >= 25:
                core_lines.append(line)
        elif asset_type == "rotation":
            if result["score"] >= 20:
                rotation_lines.append(line)
        else:
            if result["score"] >= 15:
                future_lines.append(line)

    if core_lines:
        lines.append("[CORE]")
        lines.extend(core_lines)
        lines.append("")

    if rotation_lines:
        lines.append("[ROTATION]")
        lines.extend(rotation_lines)
        lines.append("")

    if future_lines:
        lines.append("[FUTURE]")
        lines.extend(future_lines)
        lines.append("")

    if not core_lines and not rotation_lines and not future_lines:
        lines.append("- 위험 경고 종목 없음")

    return "\n".join(lines)


def main():
    pf = load_portfolio()

    if pf.empty:
        send_telegram("PORTFOLIO 시트가 비어 있습니다.")
        return

    if "ticker" not in pf.columns:
        send_telegram("PORTFOLIO 시트에 ticker 컬럼이 없습니다.")
        return

    if "type" not in pf.columns:
        pf["type"] = "rotation"

    message = build_message(pf)
    send_telegram(message)


if __name__ == "__main__":
    main()

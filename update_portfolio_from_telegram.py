import os
import re
import json
from datetime import datetime

import requests
import gspread
from google.oauth2.service_account import Credentials


TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
SHEET_ID = os.getenv("SHEET_ID")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")


def send_telegram(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print(message)
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    requests.post(
        url,
        data={"chat_id": TELEGRAM_CHAT_ID, "text": message},
        timeout=20,
    )


def get_gsheet_client():
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON secret missing")

    info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def get_sheet(sheet_name: str):
    gc = get_gsheet_client()
    sh = gc.open_by_key(SHEET_ID)
    return sh.worksheet(sheet_name)


def ensure_worksheet(spreadsheet, title: str, rows: int = 100, cols: int = 10):
    try:
        return spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)


def fetch_latest_telegram_message():
    if not TELEGRAM_BOT_TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN missing")

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    resp = requests.get(url, timeout=20).json()

    results = resp.get("result", [])
    if not results:
        return None

    # 가장 최근 메시지부터 역순 탐색
    for item in reversed(results):
        msg = item.get("message") or item.get("edited_message")
        if not msg:
            continue

        chat_id = str(msg.get("chat", {}).get("id", ""))
        text = msg.get("text", "")

        if TELEGRAM_CHAT_ID and chat_id != str(TELEGRAM_CHAT_ID):
            continue

        if text.strip().startswith("업데이트"):
            return text

    return None


def clean_number(s: str) -> float:
    s = str(s).strip().replace(",", "").replace("원", "").replace("$", "")
    if s == "":
        return 0.0
    return float(s)


def parse_update_message(text: str):
    lines = [x.strip() for x in text.splitlines() if x.strip()]
    if not lines or lines[0] != "업데이트":
        raise ValueError("첫 줄은 반드시 '업데이트' 여야 합니다.")

    portfolio_rows = []
    cash_value = None

    for line in lines[1:]:
        # CASH 338337
        cash_match = re.match(r"^CASH\s+([\d,\.]+)$", line, re.IGNORECASE)
        if cash_match:
            cash_value = int(clean_number(cash_match.group(1)))
            continue

        # TICKER SHARES AVG_PRICE TYPE
        m = re.match(
            r"^([A-Za-z\.\-]+)\s+(\d+)\s+([\d,\.]+)\s+(core|rotation|future)$",
            line,
            re.IGNORECASE,
        )
        if not m:
            raise ValueError(f"형식 오류: {line}")

        ticker = m.group(1).upper().strip()
        shares = int(m.group(2))
        avg_price = int(clean_number(m.group(3)))
        ptype = m.group(4).lower().strip()

        portfolio_rows.append([ticker, shares, avg_price, ptype])

    if not portfolio_rows:
        raise ValueError("포트폴리오 종목이 없습니다.")

    return portfolio_rows, cash_value


def update_portfolio_sheet(portfolio_rows):
    gc = get_gsheet_client()
    sh = gc.open_by_key(SHEET_ID)
    ws = ensure_worksheet(sh, "PORTFOLIO", rows=200, cols=10)

    ws.clear()
    values = [["ticker", "shares", "avg_price", "type"]] + portfolio_rows
    ws.update("A1", values)


def update_settings_cash(cash_value):
    if cash_value is None:
        return

    gc = get_gsheet_client()
    sh = gc.open_by_key(SHEET_ID)
    ws = ensure_worksheet(sh, "SETTINGS", rows=100, cols=5)

    data = ws.get_all_values()
    if not data:
        ws.update("A1", [["key", "value"]])
        data = [["key", "value"]]

    rows = data[1:] if len(data) > 1 else []
    found = False
    new_rows = []

    for row in rows:
        key = row[0].strip() if len(row) > 0 else ""
        value = row[1] if len(row) > 1 else ""
        if key == "cash":
            new_rows.append(["cash", str(cash_value)])
            found = True
        else:
            new_rows.append([key, value])

    if not found:
        new_rows.append(["cash", str(cash_value)])

    final_values = [["key", "value"]] + new_rows
    ws.clear()
    ws.update("A1", final_values)


def append_trade_log(portfolio_rows):
    gc = get_gsheet_client()
    sh = gc.open_by_key(SHEET_ID)
    ws = ensure_worksheet(sh, "TRADE_LOG", rows=500, cols=10)

    existing = ws.get_all_values()
    if not existing:
        ws.update("A1", [["date", "ticker", "action", "shares"]])

    today = datetime.utcnow().strftime("%Y-%m-%d")
    rows = [[today, r[0], "BUY", r[1]] for r in portfolio_rows]
    start_row = len(ws.get_all_values()) + 1
    ws.update(f"A{start_row}", rows)


def main():
    text = fetch_latest_telegram_message()
    if not text:
        send_telegram("업데이트 메시지를 찾지 못했습니다.")
        return

    portfolio_rows, cash_value = parse_update_message(text)
    update_portfolio_sheet(portfolio_rows)
    update_settings_cash(cash_value)

    summary = []
    summary.append("✅ 포트폴리오 자동 업데이트 완료")
    summary.append("")
    summary.append(f"- 종목 수: {len(portfolio_rows)}개")
    if cash_value is not None:
        summary.append(f"- 현금 반영: {cash_value:,}원")
    else:
        summary.append("- 현금 반영: 없음")

    summary.append("")
    summary.append("반영 종목")
    for row in portfolio_rows[:12]:
        summary.append(f"- {row[0]} | {row[1]}주 | 평단 {row[2]:,}원 | {row[3]}")

    send_telegram("\n".join(summary))


if __name__ == "__main__":
    main()

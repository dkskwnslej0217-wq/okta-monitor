import os
import json
from pathlib import Path
from io import StringIO
from datetime import datetime

import requests
import yfinance as yf
import pandas as pd

STATE_FILE = "signal_state.json"

BUY_LEVELS = [0.9, 0.8, 0.7, 0.6, 0.5, 0.4]   # -10, -20, -30, -40, -50, -60
SELL_LEVELS = [1.2, 1.4, 1.6, 1.8, 2.0]       # +20, +40, +60, +80, +100

BUY_WEIGHTS = {
    "200일선": {"-10%": 3, "-20%": 4, "-30%": 5, "-40%": 6, "-50%": 7, "-60%": 8},
    "240일선": {"-10%": 4, "-20%": 5, "-30%": 6, "-40%": 7, "-50%": 8, "-60%": 10},
    "365일선": {"-10%": 5, "-20%": 6, "-30%": 8, "-40%": 10, "-50%": 12, "-60%": 15},
}

SELL_WEIGHTS = {
    "200일선": {"+20%": 10, "+40%": 15, "+60%": 20, "+80%": 25, "+100%": 30},
    "240일선": {"+20%": 8, "+40%": 12, "+60%": 18, "+80%": 22, "+100%": 30},
    "365일선": {"+20%": 6, "+40%": 10, "+60%": 15, "+80%": 20, "+100%": 25},
}

LINE_SCORES = {"200일선": 10, "240일선": 20, "365일선": 30}
BUY_PCT_SCORES = {"-10%": 5, "-20%": 10, "-30%": 15, "-40%": 20, "-50%": 25, "-60%": 30}
SELL_PCT_SCORES = {"+20%": 5, "+40%": 10, "+60%": 15, "+80%": 20, "+100%": 25}


def load_state():
    path = Path(STATE_FILE)
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def save_state(state):
    Path(STATE_FILE).write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def safe_float(v):
    try:
        return float(v)
    except Exception:
        return None


def make_key(ticker, category, detail):
    return f"{ticker}::{category}::{detail}"


def is_new_signal(state, key, value=True):
    old = state.get(key)
    if old == value:
        return False
    state[key] = value
    return True


def reset_signal(state, key):
    state[key] = False


def send_telegram_message(message):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    print("TOKEN 존재:", bool(token))
    print("CHAT_ID 존재:", bool(chat_id))
    print("CHAT_ID 값:", chat_id)

    if not token or not chat_id:
        print("텔레그램 환경변수 누락: TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message
    }

    r = requests.post(url, data=payload, timeout=20)
    print("텔레그램 전송 상태:", r.status_code)
    print(r.text)


def fetch_tables_with_headers(url):
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers, timeout=20)
    response.raise_for_status()
    return pd.read_html(StringIO(response.text))


def get_sp500():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    tables = fetch_tables_with_headers(url)
    symbols = tables[0]["Symbol"].tolist()
    return [str(s).replace(".", "-") for s in symbols]


def get_nasdaq100():
    url = "https://en.wikipedia.org/wiki/Nasdaq-100"
    tables = fetch_tables_with_headers(url)

    for df in tables:
        cols = [str(c).strip().lower() for c in df.columns]
        if "ticker" in cols:
            ticker_col = df.columns[cols.index("ticker")]
            symbols = df[ticker_col].tolist()
            return [str(s).replace(".", "-") for s in symbols]

    return []


def get_market_status():
    try:
        spy = yf.Ticker("SPY").history(period="300d", auto_adjust=False)
        qqq = yf.Ticker("QQQ").history(period="300d", auto_adjust=False)
        vix = yf.Ticker("^VIX").history(period="60d", auto_adjust=False)

        spy_close = spy["Close"].dropna()
        qqq_close = qqq["Close"].dropna()
        vix_close = vix["Close"].dropna()

        spy_ma200 = safe_float(spy_close.rolling(200).mean().iloc[-1])
        qqq_ma200 = safe_float(qqq_close.rolling(200).mean().iloc[-1])

        spy_now = safe_float(spy_close.iloc[-1])
        qqq_now = safe_float(qqq_close.iloc[-1])
        vix_now = safe_float(vix_close.iloc[-1])

        if None in [spy_ma200, qqq_ma200, spy_now, qqq_now, vix_now]:
            return "🟡 중립"

        if spy_now > spy_ma200 and qqq_now > qqq_ma200 and vix_now < 20:
            return "🟢 상승장"
        elif spy_now < spy_ma200 and qqq_now < qqq_ma200:
            return "🔴 하락장"
        else:
            return "🟡 중립"
    except Exception:
        return "🟡 중립"


def get_company_name(ticker):
    try:
        info = yf.Ticker(ticker).info
        name = info.get("shortName") or info.get("longName") or ""
        return name
    except Exception:
        return ""


def calc_buy_score(line_name, pct_label):
    return LINE_SCORES[line_name] + BUY_PCT_SCORES[pct_label]


def calc_sell_score(line_name, pct_label):
    return LINE_SCORES[line_name] + SELL_PCT_SCORES[pct_label]


def assess_action(price_now, ma200_now, ma240_now, ma365_now, close):
    sell_score = 0
    buy_score = 0
    reasons = []

    ma200_slope_down = False
    ma240_slope_down = False
    ma365_slope_down = False

    if len(close) >= 385:
        ma200_series = close.rolling(200).mean()
        ma240_series = close.rolling(240).mean()
        ma365_series = close.rolling(365).mean()

        old_ma200 = safe_float(ma200_series.iloc[-20])
        old_ma240 = safe_float(ma240_series.iloc[-20])
        old_ma365 = safe_float(ma365_series.iloc[-20])

        if old_ma200 is not None and ma200_now < old_ma200:
            ma200_slope_down = True
        if old_ma240 is not None and ma240_now < old_ma240:
            ma240_slope_down = True
        if old_ma365 is not None and ma365_now < old_ma365:
            ma365_slope_down = True

    low_60 = safe_float(close.tail(60).min())
    ret_20 = None

    if len(close) >= 21:
        prev_20 = safe_float(close.iloc[-21])
        if prev_20 and prev_20 != 0:
            ret_20 = (price_now / prev_20 - 1) * 100

    if price_now < ma200_now:
        sell_score += 1
        reasons.append("200일선 아래")
    if price_now < ma240_now:
        sell_score += 2
        buy_score += 2
        reasons.append("240일선 아래")
    if price_now < ma365_now:
        sell_score += 3
        buy_score += 3
        reasons.append("365일선 아래")

    if ma200_slope_down:
        sell_score += 1
        reasons.append("200일선 하락기울기")
    if ma240_slope_down:
        sell_score += 1
        reasons.append("240일선 하락기울기")
    if ma365_slope_down:
        sell_score += 1
        reasons.append("365일선 하락기울기")

    if low_60 is not None and low_60 > 0:
        dist_from_low = (price_now / low_60 - 1) * 100
        if dist_from_low <= 3:
            sell_score += 2
            reasons.append("60일 저점 부근")
        elif dist_from_low <= 10:
            buy_score += 1
            reasons.append("저점권 근처")

    if ret_20 is not None:
        if ret_20 < -10:
            buy_score += 1
            reasons.append("20일 급락 후 구간")
        elif ret_20 > 15:
            sell_score += 1
            reasons.append("20일 단기 과열")

    if sell_score >= 7:
        action = "매도"
    elif buy_score >= 4 and sell_score <= 5:
        action = "추가매수"
    else:
        action = "관망"

    total_score = buy_score - sell_score

    return {
        "action": action,
        "buy_score": buy_score,
        "sell_score": sell_score,
        "total_score": total_score,
        "reason": ", ".join(reasons[:5]) if reasons else "특이사항 없음"
    }


def main():
    market_status = get_market_status()

    sp500 = get_sp500()
    nasdaq100 = get_nasdaq100()
    stocks = sorted(list(set(sp500 + nasdaq100)))

    print("=" * 70)
    print(f"시장 상태: {market_status}")
    print(f"총 감시 종목 수: {len(stocks)}")
    print("=" * 70)

    state = load_state()

    buy_signals = []
    sell_signals = []
    action_signals = []
    errors = []

    for ticker in stocks:
        try:
            data = yf.Ticker(ticker).history(
                period="700d",
                interval="1d",
                auto_adjust=False
            )

            data = data.dropna()

            if len(data) < 365:
                continue

            close = data["Close"]

            ma200 = close.rolling(200).mean()
            ma240 = close.rolling(240).mean()
            ma365 = close.rolling(365).mean()

            price_now = safe_float(close.iloc[-1])
            ma200_now = safe_float(ma200.iloc[-1])
            ma240_now = safe_float(ma240.iloc[-1])
            ma365_now = safe_float(ma365.iloc[-1])

            if None in [price_now, ma200_now, ma240_now, ma365_now]:
                continue

            company_name = get_company_name(ticker)

            base_lines = {
                "200일선": ma200_now,
                "240일선": ma240_now,
                "365일선": ma365_now
            }

            for line_name, base_value in base_lines.items():
                for lv in BUY_LEVELS:
                    pct = int((1 - lv) * 100)
                    pct_label = f"-{pct}%"
                    target_price = base_value * lv
                    key = make_key(ticker, "분할매수", f"{line_name}{pct_label}")

                    if price_now <= target_price:
                        if is_new_signal(state, key, True):
                            buy_signals.append({
                                "ticker": ticker,
                                "name": company_name,
                                "line": line_name,
                                "pct": pct_label,
                                "price": round(price_now, 2),
                                "target": round(target_price, 2),
                                "weight": BUY_WEIGHTS[line_name][pct_label],
                                "score": calc_buy_score(line_name, pct_label)
                            })
                    else:
                        reset_signal(state, key)

            for line_name, base_value in base_lines.items():
                for lv in SELL_LEVELS:
                    pct = int((lv - 1) * 100)
                    pct_label = f"+{pct}%"
                    target_price = base_value * lv
                    key = make_key(ticker, "분할매도", f"{line_name}{pct_label}")

                    if price_now >= target_price:
                        if is_new_signal(state, key, True):
                            sell_signals.append({
                                "ticker": ticker,
                                "name": company_name,
                                "line": line_name,
                                "pct": pct_label,
                                "price": round(price_now, 2),
                                "target": round(target_price, 2),
                                "weight": SELL_WEIGHTS[line_name][pct_label],
                                "score": calc_sell_score(line_name, pct_label)
                            })
                    else:
                        reset_signal(state, key)

            action_data = assess_action(
                price_now=price_now,
                ma200_now=ma200_now,
                ma240_now=ma240_now,
                ma365_now=ma365_now,
                close=close
            )

            action_key = make_key(ticker, "행동판단", action_data["action"])
            if is_new_signal(state, action_key, True):
                for other in ["추가매수", "관망", "매도"]:
                    if other != action_data["action"]:
                        reset_signal(state, make_key(ticker, "행동판단", other))

                action_signals.append({
                    "ticker": ticker,
                    "name": company_name,
                    "action": action_data["action"],
                    "price": round(price_now, 2),
                    "buy_score": action_data["buy_score"],
                    "sell_score": action_data["sell_score"],
                    "score": action_data["total_score"],
                    "reason": action_data["reason"]
                })

        except Exception as e:
            errors.append(f"{ticker}: {str(e)}")

    save_state(state)

    buy_signals = sorted(buy_signals, key=lambda x: x["score"], reverse=True)[:10]
    sell_signals = sorted(sell_signals, key=lambda x: x["score"], reverse=True)[:10]
    action_signals = sorted(action_signals, key=lambda x: abs(x["score"]), reverse=True)[:10]

    print("\n" + "=" * 70)
    print("TOP 10 분할매수 신규 신호")
    print("=" * 70)
    if buy_signals:
        for item in buy_signals:
            print(
                f"{item['ticker']} ({item['name']}) | {item['line']} | {item['pct']} | "
                f"현재가: {item['price']} | 목표가: {item['target']} | "
                f"추천비중: {item['weight']}% | 점수: {item['score']}"
            )
    else:
        print("없음")

    print("\n" + "=" * 70)
    print("TOP 10 분할매도 신규 신호")
    print("=" * 70)
    if sell_signals:
        for item in sell_signals:
            print(
                f"{item['ticker']} ({item['name']}) | {item['line']} | {item['pct']} | "
                f"현재가: {item['price']} | 목표가: {item['target']} | "
                f"추천매도비중: {item['weight']}% | 점수: {item['score']}"
            )
    else:
        print("없음")

    print("\n" + "=" * 70)
    print("TOP 10 최종 행동판단 신규 신호")
    print("=" * 70)
    if action_signals:
        for item in action_signals:
            print(
                f"{item['ticker']} ({item['name']}) | 판단: {item['action']} | 현재가: {item['price']} | "
                f"매수점수: {item['buy_score']} | 매도점수: {item['sell_score']} | "
                f"순점수: {item['score']} | 이유: {item['reason']}"
            )
    else:
        print("없음")

    print("\n" + "=" * 70)
    print("오류 종목")
    print("=" * 70)
    if errors:
        for err in errors[:30]:
            print(err)
    else:
        print("없음")

    if buy_signals or sell_signals or action_signals:
        lines = []
        lines.append("📊 미국주식 감시 리포트")
        lines.append("")
        lines.append(f"시장 상태: {market_status}")
        lines.append(f"감시 종목 수: {len(stocks)}")
        lines.append(f"감시 시각: {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC")
        lines.append("")

        lines.append("🟢 [TOP 10 추가매수 후보]")
        if buy_signals:
            for idx, item in enumerate(buy_signals, start=1):
                name_text = f" ({item['name']})" if item["name"] else ""
                lines.append(
                    f"{idx}) {item['ticker']}{name_text}\n"
                    f"- 기준선: {item['line']}\n"
                    f"- 위치: {item['pct']}\n"
                    f"- 현재가: {item['price']}\n"
                    f"- 목표가: {item['target']}\n"
                    f"- 추천비중: {item['weight']}%\n"
                    f"- 우선점수: {item['score']}"
                )
        else:
            lines.append("없음")

        lines.append("")
        lines.append("🔴 [TOP 10 분할매도 후보]")
        if sell_signals:
            for idx, item in enumerate(sell_signals, start=1):
                name_text = f" ({item['name']})" if item["name"] else ""
                lines.append(
                    f"{idx}) {item['ticker']}{name_text}\n"
                    f"- 기준선: {item['line']}\n"
                    f"- 위치: {item['pct']}\n"
                    f"- 현재가: {item['price']}\n"
                    f"- 목표가: {item['target']}\n"
                    f"- 추천매도비중: {item['weight']}%\n"
                    f"- 우선점수: {item['score']}"
                )
        else:
            lines.append("없음")

        lines.append("")
        lines.append("🧠 [TOP 10 최종 행동판단]")
        if action_signals:
            for idx, item in enumerate(action_signals, start=1):
                name_text = f" ({item['name']})" if item["name"] else ""
                lines.append(
                    f"{idx}) {item['ticker']}{name_text}\n"
                    f"- 판단: {item['action']}\n"
                    f"- 현재가: {item['price']}\n"
                    f"- 매수점수: {item['buy_score']}\n"
                    f"- 매도점수: {item['sell_score']}\n"
                    f"- 이유: {item['reason']}"
                )
        else:
            lines.append("없음")

        message = "\n".join(lines)
        send_telegram_message(message)
    else:
        print("신규 신호 없음 - 텔레그램 발송 안 함")


if __name__ == "__main__":
    main()

import yfinance as yf
import pandas as pd

def get_sp500():
    table = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
    return table[0]["Symbol"].tolist()

def get_nasdaq100():
    table = pd.read_html("https://en.wikipedia.org/wiki/Nasdaq-100")
    return table[4]["Ticker"].tolist()

def safe_float(v):
    try:
        return float(v)
    except:
        return None

# 감시 종목: S&P500 + Nasdaq100
sp500 = get_sp500()
nasdaq100 = get_nasdaq100()
stocks = sorted(list(set(sp500 + nasdaq100)))

print(f"총 감시 종목 수: {len(stocks)}")

for ticker in stocks:
    try:
        data = yf.Ticker(ticker).history(period="500d", interval="1d", auto_adjust=False)
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

        print(f"\n===== {ticker} =====")
        print(f"현재가격: {price_now:.2f}")
        print(f"200일선: {ma200_now:.2f}")
        print(f"240일선: {ma240_now:.2f}")
        print(f"365일선: {ma365_now:.2f}")

        # 분할매수 구간
        if price_now <= ma200_now:
            print(f"🟢 1차 분할매수 구간 도달 (200일선): {ticker}")

        if price_now <= ma240_now:
            print(f"🟢 2차 분할매수 구간 도달 (240일선): {ticker}")

        if price_now <= ma365_now:
            print(f"🟢 3차 분할매수 구간 도달 (365일선): {ticker}")

        # 기준선별 목표 알림 배수
        levels = [1.2, 1.4, 1.6, 1.8, 2.0]

        base_lines = {
            "200일선": ma200_now,
            "240일선": ma240_now,
            "365일선": ma365_now
        }

        for line_name, base_value in base_lines.items():
            for lv in levels:
                target_price = base_value * lv
                pct = int((lv - 1) * 100)

                if price_now >= target_price:
                    print(
                        f"🔔 {ticker} {line_name} 기준 +{pct}% 도달 | "
                        f"목표가: {target_price:.2f} | 현재가: {price_now:.2f}"
                    )

    except Exception as e:
        print(f"{ticker} 오류: {e}")

import yfinance as yf
import pandas as pd

ticker = "OKTA"

data = yf.download(ticker, period="400d", interval="1d")
data = data.dropna()

close = data["Close"]

if isinstance(close, pd.DataFrame):
    close = close.iloc[:, 0]

data["MA200"] = close.rolling(200).mean()
data["MA240"] = close.rolling(240).mean()

latest_price = float(close.iloc[-1])
latest_ma200 = float(data["MA200"].iloc[-1])
latest_ma240 = float(data["MA240"].iloc[-1])

print("현재가격:", latest_price)
print("200일선:", latest_ma200)
print("240일선:", latest_ma240)

if latest_price > latest_ma200 and latest_price > latest_ma240 and latest_ma200 > latest_ma240:
    print("매수조건 충족")
else:
    print("조건 미충족")

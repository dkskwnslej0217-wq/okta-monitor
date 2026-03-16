import yfinance as yf
import pandas as pd

ticker = "OKTA"

data = yf.download(ticker, period="400d", interval="1d")
data = data.dropna()
data["MA200"] = data["Close"].rolling(200).mean()
data["MA240"] = data["Close"].rolling(240).mean()

latest = data.iloc[-1]

price = latest["Close"]
ma200 = latest["MA200"]
ma240 = latest["MA240"]

print("현재가격:", price)
print("200일선:", ma200)
print("240일선:", ma240)

if price > ma200 and price > ma240 and ma200 > ma240:
    print("매수조건 충족")
else:
    print("조건 미충족")

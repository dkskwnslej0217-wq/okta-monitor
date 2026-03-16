import yfinance as yf
import pandas as pd

ticker = "OKTA"

data = yf.download(ticker, period="400d", interval="1d")
data = data.dropna()

close = data["Close"]

# 이동평균 계산
data["MA200"] = close.rolling(200).mean()
data["MA240"] = close.rolling(240).mean()

# 마지막 값만 가져오기 (float)
price = float(close.iloc[-1])
ma200 = float(data["MA200"].iloc[-1])
ma240 = float(data["MA240"].iloc[-1])

print("현재가격:", price)
print("200일선:", ma200)
print("240일선:", ma240)

# 매수 조건
if price > ma200 and price > ma240 and ma200 > ma240:
    print("매수조건 충족")
else:
    print("조건 미충족")

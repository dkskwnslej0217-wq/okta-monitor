import yfinance as yf

ticker = "OKTA"

data = yf.Ticker(ticker).history(period="400d", interval="1d", auto_adjust=False)
data = data.dropna()

close = data["Close"]

ma200 = close.rolling(200).mean()
ma240 = close.rolling(240).mean()

price_now = float(close.iloc[-1])
ma200_now = float(ma200.iloc[-1])
ma240_now = float(ma240.iloc[-1])

print("현재가격:", price_now)
print("200일선:", ma200_now)
print("240일선:", ma240_now)

if price_now > ma200_now and price_now > ma240_now and ma200_now > ma240_now:
    print("매수조건 충족")
else:
    print("조건 미충족")

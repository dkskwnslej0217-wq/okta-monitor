import yfinance as yf
import pandas as pd

ticker = "OKTA"

data = yf.download(
    ticker,
    period="400d",
    interval="1d",
    auto_adjust=False,
    progress=False
)

# yfinance가 멀티인덱스 컬럼으로 줄 때 대비
if isinstance(data.columns, pd.MultiIndex):
    data.columns = data.columns.get_level_values(0)

data = data.dropna()

close = pd.to_numeric(data["Close"], errors="coerce").dropna()

ma200_series = close.rolling(200).mean()
ma240_series = close.rolling(240).mean()

price = float(close.iloc[-1])
ma200 = float(ma200_series.iloc[-1])
ma240 = float(ma240_series.iloc[-1])

print("현재가격:", price)
print("200일선:", ma200)
print("240일선:", ma240)

if price > ma200 and price > ma240 and ma200 > ma240:
    print("매수조건 충족")
else:
    print("조건 미충족")

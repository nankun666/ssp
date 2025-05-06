import yfinance as yf
import requests
import pandas as pd
from datetime import datetime, timedelta
import os

# ========== 1. 股票数据收集 ==========
def fetch_multiple_stocks(tickers, start_date, end_date):
    for ticker in tickers:
        print(f"Fetching stock data for {ticker}")
        stock = yf.download(ticker, start=start_date, end=end_date)
        stock.reset_index(inplace=True)
        stock.to_csv(f"data/stock_{ticker}.csv", index=False)
        print(f"Saved data: data/stock_{ticker}.csv")

# ========== 2. 新闻数据收集 ==========
def fetch_news_data(query, from_date, to_date, api_key, page_size=100):
    print(f"Fetching news about {query} from {from_date} to {to_date}")
    url = "https://newsapi.org/v2/everything"
    all_articles = []
    for page in range(1, 6):  # Get up to 500 articles
        params = {
            'q': query,
            'from': from_date,
            'to': to_date,
            'language': 'en',
            'sortBy': 'relevancy',
            'pageSize': page_size,
            'page': page,
            'apiKey': api_key
        }
        response = requests.get(url, params=params)
        if response.status_code != 200:
            print(f"Error fetching page {page}: {response.status_code}")
            break
        data = response.json()
        articles = data.get('articles', [])
        if not articles:
            break
        all_articles.extend(articles)
    
    # 只保留需要的字段
    df = pd.DataFrame(all_articles)[['publishedAt', 'title', 'description', 'source']]
    df['source'] = df['source'].apply(lambda x: x.get('name'))
    df.to_csv(f"data/news_{query}.csv", index=False)
    print(f"Saved news data to data/news_{query}.csv")

# ========== 3. 主运行逻辑 ==========
if __name__ == "__main__":
    # 创建data目录
    os.makedirs("data", exist_ok=True)

    # 设置参数
    tickers = ["AAPL", "TSLA", "MSFT", "GOOG", "NVDA", "AMZN", "META"]
    today = datetime.today()
    start = today - timedelta(days=90)
    start_str = start.strftime("%Y-%m-%d")
    end_str = today.strftime("%Y-%m-%d")

    news_query = "Apple"
    news_api_key = "YOUR_NEWSAPI_KEY"  # <-- 请替换为你的 API key

    # 执行函数
    fetch_stock_data(ticker, start_str, end_str)
    fetch_news_data(news_query, start_str, end_str, news_api_key)

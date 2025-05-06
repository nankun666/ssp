import pandas as pd
from textblob import TextBlob
from sklearn.feature_extraction.text import TfidfVectorizer
import os

def analyze_sentiment(text):
    if not isinstance(text, str) or len(text.strip()) == 0:
        return 0.0
    return TextBlob(text).sentiment.polarity  # [-1, 1]

def classify_sentiment(polarity):
    if polarity > 0.1:
        return 'positive'
    elif polarity < -0.1:
        return 'negative'
    else:
        return 'neutral'

def extract_keywords(texts, top_n=10):
    vectorizer = TfidfVectorizer(stop_words='english', max_features=100)
    tfidf_matrix = vectorizer.fit_transform(texts)
    feature_array = vectorizer.get_feature_names_out()
    tfidf_scores = tfidf_matrix.sum(axis=0).A1
    top_indices = tfidf_scores.argsort()[::-1][:top_n]
    keywords = [feature_array[i] for i in top_indices]
    return keywords

if __name__ == "__main__":
    input_path = "data/news_Apple.csv"
    output_path = "output/news_analysis_Apple.csv"
    os.makedirs("output", exist_ok=True)

    # 加载新闻数据
    df = pd.read_csv(input_path)

    # 合并 title + description 分析
    df["text"] = df["title"].fillna('') + ". " + df["description"].fillna('')

    # 情感分析
    df["polarity"] = df["text"].apply(analyze_sentiment)
    df["sentiment"] = df["polarity"].apply(classify_sentiment)

    # 提取关键词
    keywords = extract_keywords(df["text"].tolist(), top_n=10)
    print(f"Top keywords: {keywords}")

    # 保存分析结果
    df[["publishedAt", "title", "sentiment", "polarity"]].to_csv(output_path, index=False)
    print(f"✅ 分析完成，结果保存至：{output_path}")

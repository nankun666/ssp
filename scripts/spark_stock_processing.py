from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, avg, stddev
from pyspark.sql.window import Window
import os

# 初始化 Spark
spark = SparkSession.builder \
    .appName("StockAnalysis") \
    .getOrCreate()

# 文件路径
data_dir = "data"
stock_files = [f for f in os.listdir(data_dir) if f.startswith("stock_")]

results = []

for file in stock_files:
    ticker = file.split("_")[1].replace(".csv", "")
    print(f"Processing {ticker}...")

    # 读取 CSV
    df = spark.read.option("header", True).csv(os.path.join(data_dir, file))

    # 转换数据类型
    df = df.withColumn("Date", col("Date")) \
           .withColumn("Close", col("Close").cast("double")) \
           .orderBy("Date")

    # 滑动窗口（用前一天的 close 计算日收益）
    window = Window.orderBy("Date")
    df = df.withColumn("prev_close", lag("Close").over(window))
    df = df.withColumn("daily_return", (col("Close") - col("prev_close")) / col("prev_close"))

    # 过滤掉第一天（没有前值）
    df = df.na.drop(subset=["daily_return"])

    # 计算平均收益和波动率
    metrics = df.agg(
        avg("daily_return").alias("avg_return"),
        stddev("daily_return").alias("volatility")
    ).withColumn("ticker", spark.createDataFrame([[ticker]], ["ticker"]).col("ticker"))

    results.append(metrics)

# 合并结果
final_df = results[0]
for r in results[1:]:
    final_df = final_df.union(r)

# 保存结果
final_df.select("ticker", "avg_return", "volatility").show()
final_df.toPandas().to_csv("output/stock_metrics_summary.csv", index=False)

print("✅ 计算完成，结果保存在 output/stock_metrics_summary.csv")

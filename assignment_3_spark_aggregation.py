# assignment_3_spark_aggregation.py

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum

DATA_DIR = Path("data")
pd.set_option("display.max_columns", None)

# Initialize Spark Session
spark = (
    SparkSession.builder
    .appName("ProductsSoldByCategory")
    .master("local[*]")
    .getOrCreate()
)

# Load sales data via Spark
sales_spark = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(str(DATA_DIR / "sample_sales_data.csv"))
)

sales_spark.printSchema()
sales_spark.show(5)

# Aggregate data by product line
products_by_category = (
    sales_spark
    .groupBy("PRODUCTLINE")
    .agg(spark_sum("QUANTITYORDERED").alias("TOTAL_PRODUCTS_SOLD"))
    .orderBy("TOTAL_PRODUCTS_SOLD", ascending=False)
)

products_by_category.show()

# Convert to Pandas for visualization
spark_result_pd = products_by_category.toPandas()

spark_result_pd.plot(
    kind="bar",
    x="PRODUCTLINE",
    y="TOTAL_PRODUCTS_SOLD",
    figsize=(11, 5),
    legend=False,
    title="Total Products Sold by Product Line"
)
plt.xlabel("Product Line")
plt.ylabel("Total Products Sold")
plt.xticks(rotation=35, ha="right")
plt.tight_layout()
plt.show()

spark.stop()
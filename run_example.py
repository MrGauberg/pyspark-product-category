from pathlib import Path
import os

import pandas as pd
from pyspark.sql import SparkSession

from logic import product_category_pairs

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"] = os.environ["PYSPARK_DRIVER_PYTHON"] = "python"


spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("product-category-demo")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
)


xls_path = Path("test_product_category.xlsx").resolve()

products_pdf = pd.read_excel(xls_path, sheet_name="products")
categories_pdf = pd.read_excel(xls_path, sheet_name="categories")
pc_pdf = pd.read_excel(xls_path, sheet_name="product_category")

products_df = spark.createDataFrame(products_pdf)
categories_df = spark.createDataFrame(categories_pdf)
pc_df = spark.createDataFrame(pc_pdf)

result_df = product_category_pairs(products_df, categories_df, pc_df)

result_df.show(truncate=False)

spark.stop()

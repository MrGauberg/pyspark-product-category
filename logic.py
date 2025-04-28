from pyspark.sql import DataFrame, functions as F


def product_category_pairs(
    products_df: DataFrame,
    categories_df: DataFrame,
    product_category_df: DataFrame,
) -> DataFrame:
    """
    Возвращает датафрейм со столбцами:
      product_name
      category_name (NULL, если у продукта нет категории)
    """
    return (
        products_df.alias("p")
        .join(
            product_category_df.alias("pc"),
            F.col("p.product_id") == F.col("pc.product_id"),
            how="left",
        )
        .join(
            categories_df.alias("c"),
            F.col("pc.category_id") == F.col("c.category_id"),
            how="left",
        )
        .select(
            F.col("p.product_name").alias("product_name"),
            F.col("c.category_name").alias("category_name"),
        )
        .distinct()
    )

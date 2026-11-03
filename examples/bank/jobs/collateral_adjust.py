"""Eligible collateral per loan after supervisory haircuts.

Runs on the shared Spark cluster (task adjust_collateral in DAG regulatory_reporting).
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("collateral_adjust").getOrCreate()

collateral = spark.table("bronze.collateral")

# a haircut of 100% or more means the collateral is not eligible
eligible = collateral.filter(F.col("haircut") < 1)

adjusted = eligible.withColumn(
    "adjusted_value_usd", F.col("collateral_value_usd") * (F.lit(1) - F.col("haircut"))
)

by_loan = adjusted.groupBy("loan_id").agg(F.sum("adjusted_value_usd").alias("collateral_usd"))

by_loan.write.mode("overwrite").saveAsTable("silver.collateral_adj")

"""Batch transform inside Postgres: raw event tables -> analytics tables.

Reads the four raw tables written by spark_stream.py and rebuilds:

    page_views_per_min   views and unique users per page per 1-minute window
    order_status         one row per order with its payment result and seconds_to_pay
    revenue_per_min      paid orders, revenue and failed payments per 1-minute window
    user_activity        per-user clicks, sign-ins, orders, total spent, first/last seen

Each run overwrites these tables, so it is safe to run repeatedly.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config import JDBC_PROPERTIES, SPARK_PACKAGES, WAREHOUSE_JDBC_URL


def page_views_per_minute(clicks: DataFrame) -> DataFrame:
    return (clicks
            .groupBy(F.window("event_time", "1 minute").alias("w"), "page")
            .agg(F.count("*").alias("views"),
                 F.countDistinct("user_id").alias("unique_users"))
            .select(F.col("w.start").alias("window_start"), "page", "views", "unique_users"))


def order_status(orders: DataFrame, payments: DataFrame) -> DataFrame:
    pay = payments.select("order_id",
                          F.col("status").alias("payment_status"),
                          F.col("method").alias("payment_method"),
                          F.col("event_time").alias("paid_at"))
    return (orders.join(pay, "order_id", "left")
            .withColumn("payment_status", F.coalesce("payment_status", F.lit("pending")))
            .withColumn("seconds_to_pay",
                        F.round(F.col("paid_at").cast("double")
                                - F.col("event_time").cast("double"), 3))
            .select("order_id", "user_id", "amount", "items",
                    F.col("event_time").alias("ordered_at"),
                    "payment_status", "payment_method", "paid_at", "seconds_to_pay"))


def revenue_per_minute(payments: DataFrame) -> DataFrame:
    paid = F.col("status") == "success"
    return (payments
            .groupBy(F.window("event_time", "1 minute").alias("w"))
            .agg(F.count(F.when(paid, 1)).alias("paid_orders"),
                 F.round(F.coalesce(F.sum(F.when(paid, F.col("amount"))), F.lit(0.0)), 2)
                  .alias("revenue"),
                 F.count(F.when(~paid, 1)).alias("failed_payments"))
            .select(F.col("w.start").alias("window_start"),
                    "paid_orders", "revenue", "failed_payments"))


def user_activity(clicks: DataFrame, sign_ins: DataFrame, statuses: DataFrame) -> DataFrame:
    click_stats = clicks.groupBy("user_id").agg(
        F.count("*").alias("clicks"),
        F.min("event_time").alias("first_seen"),
        F.max("event_time").alias("last_seen"),
        F.max((F.col("page") == "/checkout").cast("int")).cast("boolean").alias("reached_checkout"))
    sign_in_stats = sign_ins.groupBy("user_id").agg(F.count("*").alias("sign_ins"))
    order_stats = statuses.groupBy("user_id").agg(
        F.count("*").alias("orders"),
        F.round(F.coalesce(F.sum(F.when(F.col("payment_status") == "success", F.col("amount"))),
                           F.lit(0.0)), 2).alias("total_spent"))
    return (click_stats
            .join(sign_in_stats, "user_id", "left")
            .join(order_stats, "user_id", "left")
            .fillna({"sign_ins": 0, "orders": 0, "total_spent": 0.0})
            .select("user_id", "clicks", "sign_ins", "orders", "total_spent",
                    "reached_checkout", "first_seen", "last_seen"))


def main() -> None:
    spark = (SparkSession.builder
             .appName("TransformJob")
             .master("local[*]")
             .config("spark.jars.packages", SPARK_PACKAGES)
             .config("spark.sql.shuffle.partitions", "4")  # small data, keep it fast
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    def read(table: str) -> DataFrame:
        # A retried streaming batch can append the same event twice, so de-duplicate here.
        return (spark.read.jdbc(WAREHOUSE_JDBC_URL, table, properties=JDBC_PROPERTIES)
                .dropDuplicates(["event_id"]))

    def write(df: DataFrame, table: str) -> None:
        df.write.jdbc(WAREHOUSE_JDBC_URL, table, mode="overwrite", properties=JDBC_PROPERTIES)

    try:
        clicks = read("click_events").cache()
        sign_ins = read("user_signed_in")
        orders = read("order_placed")
        payments = read("payment_completed").cache()
        statuses = order_status(orders, payments).cache()

        write(page_views_per_minute(clicks), "page_views_per_min")
        write(statuses, "order_status")
        write(revenue_per_minute(payments), "revenue_per_min")
        write(user_activity(clicks, sign_ins, statuses), "user_activity")
        print(f"Transformed {clicks.count()} clicks, {statuses.count()} orders, "
              f"{payments.count()} payments", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

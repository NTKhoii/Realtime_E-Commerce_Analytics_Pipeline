import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

# Import các công cụ từ hộp đồ nghề (utils)
from src.utils.config_parser import load_config
from src.utils.geo_parser import ip_to_location_udf
from src.utils.transform_helpers import (
    parse_ua_udf, 
    parse_traffic_udf, 
    enrich_time_dimensions, 
    generate_surrogate_keys
)
from src.utils.db_client import process_micro_batch

def start_streaming():
    print("BẮT ĐẦU KHỞI ĐỘNG LUỒNG STREAMING...")
    config = load_config()
    # Nạp thư viện Kafka và Postgres Driver vào Spark
    spark = SparkSession.builder \
        .appName(config['SPARK']['app_name']) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    # Gửi file nhị phân IP (.BIN) lên bộ nhớ RAM của tất cả các Node kĩ thuật này gọi là broadcast 
    bin_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/reference/IP-COUNTRY-REGION-CITY.BIN'))
    spark.sparkContext.addFile(bin_path)

    json_schema = StructType([
        StructField("id", StringType(), True), 
        StructField("ip", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("resolution", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("local_time", StringType(), True),
        StructField("current_url", StringType(), True),
        StructField("referrer_url", StringType(), True),
        StructField("collection", StringType(), True),
        StructField("product_id", StringType(), True)
    ])
    print("Đang kết nối Kafka...")
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config['KAFKA']['bootstrap_servers']) \
        .option("subscribe", config['KAFKA']['topic']) \
        .option("startingOffsets", config['KAFKA']['starting_offsets']) \
        .option("kafka.security.protocol", config['KAFKA']['security_protocol']) \
        .option("kafka.sasl.mechanism", config['KAFKA']['sasl_mechanism']) \
        .option("kafka.sasl.jaas.config", config['KAFKA']['sasl_jaas_config']) \
        .load()

    df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_payload") \
                      .select(from_json(col("json_payload"), json_schema).alias("data")) \
                      .select("data.*")

    print("Đang thiết lập Pipeline làm sạch dữ liệu...")
    df_filtered = df_parsed.filter(col("collection") == "view_product_detail")
    df_transformed = df_filtered.withColumnRenamed("id", "view_id")
    df_transformed = df_transformed.withColumn("ua_struct", parse_ua_udf(col("user_agent"))) \
                                   .withColumn("os", col("ua_struct.os")) \
                                   .withColumn("browser", col("ua_struct.browser"))
    df_transformed = df_transformed.withColumn("traffic_struct", parse_traffic_udf(col("current_url"), col("referrer_url"))) \
                                   .withColumn("domain", col("traffic_struct.domain")) \
                                   .withColumn("referrer_domain", col("traffic_struct.referrer_domain")) \
                                   .withColumn("traffic_type", col("traffic_struct.traffic_type"))
    df_transformed = df_transformed.withColumn("geo_struct", ip_to_location_udf(col("ip"))) \
                                   .withColumn("country_code", col("geo_struct.country_code")) \
                                   .withColumn("country_name", col("geo_struct.country_name")) \
                                   .withColumn("region_name", col("geo_struct.region_name")) \
                                   .withColumn("city_name", col("geo_struct.city_name"))
    df_transformed = enrich_time_dimensions(df_transformed)
    df_final = generate_surrogate_keys(df_transformed)
    print("Băng chuyền đã sẵn sàng cho Data chảy!")

    query = df_final.writeStream \
        .outputMode("append") \
        .foreachBatch(process_micro_batch) \
        .trigger(processingTime=config['SPARK']['trigger_processing_time']) \
        .option("checkpointLocation", config['SPARK']['checkpoint_dir']) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    start_streaming()
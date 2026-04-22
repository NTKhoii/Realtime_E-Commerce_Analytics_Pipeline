from pyspark.sql import SparkSession
from src.utils.config_parser import load_config

# 1. Load cấu hình
config = load_config()

# 2. Khởi tạo Spark (Chạy Local)
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Kafka_Profiler") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR") # Giấu bớt log rác

print("Đang kết nối Kafka để kéo dữ liệu mẫu...")

# 3. ĐỌC BATCH TỪ KAFKA (Không dùng readStream)
df_raw = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", config['KAFKA']['bootstrap_servers']) \
    .option("subscribe", config['KAFKA']['topic']) \
    .option("kafka.security.protocol", config['KAFKA']['security_protocol']) \
    .option("kafka.sasl.mechanism", config['KAFKA']['sasl_mechanism']) \
    .option("kafka.sasl.jaas.config", config['KAFKA']['sasl_jaas_config']) \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Ép kiểu dữ liệu (Kafka lưu value dạng Binary)
df_string = df_raw.selectExpr("CAST(value AS STRING) as json_payload")

# 5. Lấy 5 dòng đầu tiên và in ra toàn bộ nội dung (truncate=False)
print("DỮ LIỆU MẪU TỪ KAFKA:")
df_string.show(5, truncate=False)

spark.stop()
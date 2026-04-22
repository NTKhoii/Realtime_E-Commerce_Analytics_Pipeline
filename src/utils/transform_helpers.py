import urllib.parse
from user_agents import parse
from pyspark.sql.functions import (
    udf, col, md5, concat_ws, to_timestamp, 
    date_format, year, month, dayofmonth, hour, minute
)
from pyspark.sql.types import StringType, StructType, StructField
def get_device_info(ua_string):
    if not ua_string:
        return ("UNKNOWN", "UNKNOWN")
    try:
        ua = parse(ua_string)
        return (ua.os.family, ua.browser.family)
    except Exception:
        return ("UNKNOWN", "UNKNOWN")

device_schema = StructType([
    StructField("os", StringType(), True),
    StructField("browser", StringType(), True)
])

parse_ua_udf = udf(get_device_info, device_schema)

def get_traffic_source(current_url, referrer_url):
    # 1. Bóc lấy domain của cửa hàng hiện tại
    domain = "UNKNOWN"
    if current_url:
        parsed_current = urllib.parse.urlparse(current_url)
        domain = parsed_current.netloc 
    # 2. Xử lý Referrer (Nguồn đến)
    referrer_domain = "DIRECT"
    traffic_type = "Direct"
    if referrer_url:
        parsed_ref = urllib.parse.urlparse(referrer_url)
        referrer_domain = parsed_ref.netloc if parsed_ref.netloc else "UNKNOWN"
        if any(keyword in referrer_domain.lower() for keyword in ["google", "youtube", "facebook", "bing", "twitter", "linkedin", "instagram", "tiktok"]):
            traffic_type = "Social/Search"
        elif "glamira" in referrer_domain.lower():
            traffic_type = "Internal"
        else:
            traffic_type = "Referral"
    return (domain, referrer_domain, traffic_type)

traffic_schema = StructType([
    StructField("domain", StringType(), True),
    StructField("referrer_domain", StringType(), True),
    StructField("traffic_type", StringType(), True)
])

parse_traffic_udf = udf(get_traffic_source, traffic_schema)


# =====================================================================
# 3. HÀM NATIVE: XỬ LÝ THỜI GIAN (Xây dựng Dim Date & Dim Time)
# =====================================================================
def enrich_time_dimensions(df):
    """Biến đổi cột local_time (String) thành các cột thời gian chuẩn"""
    # Ép kiểu chuỗi thành định dạng Timestamp của Spark
    df = df.withColumn("timestamp_obj", to_timestamp(col("local_time"), "yyyy-MM-dd HH:mm:ss"))    
    # Bóc tách Dim Date
    df = df.withColumn("sk_date", date_format(col("timestamp_obj"), "yyyyMMdd").cast("int")) \
           .withColumn("full_date", col("timestamp_obj").cast("date")) \
           .withColumn("day", dayofmonth(col("timestamp_obj"))) \
           .withColumn("month", month(col("timestamp_obj"))) \
           .withColumn("year", year(col("timestamp_obj")))
    # Bóc tách Dim Time
    df = df.withColumn("sk_time", date_format(col("timestamp_obj"), "HHmm").cast("int")) \
           .withColumn("hour", hour(col("timestamp_obj"))) \
           .withColumn("minute", minute(col("timestamp_obj")))
    return df


# =====================================================================
# 4. HÀM NATIVE: BĂM DỮ LIỆU TẠO KHÓA THAY THẾ (Surrogate Keys)
# Dùng thuật toán MD5 để tạo các mã ID kết nối bảng Fact và Dim
# =====================================================================
def generate_surrogate_keys(df):
    """Tạo các mã sk_ dựa trên việc băm (hash) các cột nội dung"""
    # sk_product: Sinh ra từ mã sản phẩm
    df = df.withColumn("sk_product", md5(col("product_id")))
    
    # sk_store: Sinh ra từ sự kết hợp của store_id và domain
    df = df.withColumn("sk_store", md5(concat_ws("_", col("store_id"), col("domain"))))
    
    # sk_device: Sinh ra từ sự kết hợp của OS, Browser và Độ phân giải
    df = df.withColumn("sk_device", md5(concat_ws("_", col("os"), col("browser"), col("resolution"))))
    
    # sk_location: Sinh ra từ Tên thành phố và Mã quốc gia
    df = df.withColumn("sk_location", md5(concat_ws("_", col("city_name"), col("region_name"), col("country_code"))))
    
    # sk_traffic: Sinh ra từ Nguồn đến và Loại Traffic
    df = df.withColumn("sk_traffic", md5(concat_ws("_", col("referrer_domain"), col("traffic_type"))))
    return df
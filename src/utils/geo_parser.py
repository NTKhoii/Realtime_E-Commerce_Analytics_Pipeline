import IP2Location
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import udf
from pyspark import SparkFiles

location_schema = StructType([
    StructField("country_code", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("region_name", StringType(), True),
    StructField("city_name", StringType(), True)
])

def get_location_from_ip(ip_address):
    if not ip_address:
        return ("UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN")

    try:
        bin_path = SparkFiles.get("IP-COUNTRY-REGION-CITY.BIN") 
        database = IP2Location.IP2Location(bin_path) 
        rec = database.get_all(ip_address) 
        return (
            rec.country_short if rec.country_short != "-" else "UNKNOWN",
            rec.country_long if rec.country_long != "-" else "UNKNOWN",
            rec.region if rec.region != "-" else "UNKNOWN",
            rec.city if rec.city != "-" else "UNKNOWN"
        )
    except Exception as e:
        return ("ERROR", "ERROR", "ERROR", "ERROR")

ip_to_location_udf = udf(get_location_from_ip, location_schema) 
# udf này sẽ được sử dụng trong Spark DataFrame để chuyển đổi IP thành thông tin địa lý tương ứng.
# file này để lưu hàm chuyển đổi IP sang thông tin địa lý, sử dụng thư viện IP2Location và được tối ưu để chạy trên Spark.
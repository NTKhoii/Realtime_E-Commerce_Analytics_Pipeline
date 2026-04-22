import psycopg2
from psycopg2.extras import execute_values
from src.utils.config_parser import load_config

def get_db_connection():
    config = load_config()
    return psycopg2.connect(
        host=config['POSTGRES']['host'],
        port=config['POSTGRES']['port'],
        database=config['POSTGRES']['database'],
        user=config['POSTGRES']['user'],
        password=config['POSTGRES']['password']
    )

def upsert_data(cursor, table_name, columns, data, conflict_col):
    if not data:
        return
    
    placeholders = ", ".join(["%s"] * len(columns))
    columns_str = ", ".join(columns)
    
    query = f"""
        INSERT INTO {table_name} ({columns_str}) 
        VALUES %s 
        ON CONFLICT ({conflict_col}) DO NOTHING
    """
    
    execute_values(cursor, query, data)

def process_partition(partition_iterator):
    """
    Hàm này sẽ được gửi xuống và chạy ĐỘC LẬP trên từng máy Worker (Executor).
    Nó chỉ xử lý 1 phần nhỏ của Micro-batch.
    """
    # Bước 1: Khởi tạo các Set/List rỗng trên RAM của máy Worker
    dim_product_set = set()
    dim_store_set = set()
    dim_location_set = set()
    dim_device_set = set()
    dim_traffic_set = set()
    dim_date_set = set()
    dim_time_set = set()
    fact_data_list = []

    # Bước 2: Duyệt qua từng dòng trong Partition để gom nhóm và gỡ trùng (Deduplicate)
    # Lượng data ở đây rất nhỏ, hoàn toàn an toàn cho RAM của Worker
    has_data = False
    for row in partition_iterator:
        has_data = True
        dim_product_set.add((row['sk_product'], row['product_id']))
        dim_store_set.add((row['sk_store'], row['store_id'], row['domain']))
        dim_location_set.add((row['sk_location'], row['city_name'], row['region_name'], row['country_name'], row['country_code']))
        dim_device_set.add((row['sk_device'], row['os'], row['browser'], row['resolution']))
        dim_traffic_set.add((row['sk_traffic'], row['referrer_domain'], row['traffic_type']))
        dim_date_set.add((row['sk_date'], row['full_date'], row['day'], row['month'], row['year']))
        dim_time_set.add((row['sk_time'], row['hour'], row['minute']))
        
        fact_data_list.append((
            row['view_id'], row['sk_date'], row['sk_time'], row['sk_product'], 
            row['sk_store'], row['sk_location'], row['sk_device'], row['sk_traffic']
        ))

    if not has_data:
        return

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        upsert_data(cursor, "dim_product", ["sk_product", "product_id"], list(dim_product_set), "sk_product")
        upsert_data(cursor, "dim_store", ["sk_store", "store_id", "domain"], list(dim_store_set), "sk_store")
        upsert_data(cursor, "dim_location", ["sk_location", "city_name", "region_name", "country_name", "country_code"], list(dim_location_set), "sk_location")
        upsert_data(cursor, "dim_device", ["sk_device", "os", "browser", "resolution"], list(dim_device_set), "sk_device")
        upsert_data(cursor, "dim_traffic_source", ["sk_traffic", "referrer_domain", "traffic_type"], list(dim_traffic_set), "sk_traffic")
        upsert_data(cursor, "dim_date", ["sk_date", "full_date", "day", "month", "year"], list(dim_date_set), "sk_date")
        upsert_data(cursor, "dim_time", ["sk_time", "hour", "minute"], list(dim_time_set), "sk_time")
        
        upsert_data(cursor, "fact_product_views", 
                    ["view_id", "sk_date", "sk_time", "sk_product", "sk_store", "sk_location", "sk_device", "sk_traffic"], 
                    fact_data_list, "view_id")
        conn.commit()
        cursor.close()
    except Exception as e:
        print(f"Lỗi ghi Database tại Worker: {e}")
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            conn.close()

def process_micro_batch(batch_df, batch_id):
    print(f"Bắt đầu phân phối ghi Micro-batch ID: {batch_id} xuống các máy Worker...")
    batch_df.foreachPartition(process_partition)
    
    print(f"Hoàn tất Micro-batch {batch_id}.")
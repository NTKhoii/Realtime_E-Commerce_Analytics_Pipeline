import configparser
import os
from dotenv import load_dotenv

def load_config():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(current_dir))
    
    env_path = os.path.join(project_root, '.env')
    if os.path.exists(env_path):
        load_dotenv(dotenv_path=env_path)
    else:
        print("Cảnh báo: Không tìm thấy file .env")

    config_path = os.path.join(project_root, 'config', 'spark.conf')
    config = configparser.ConfigParser()
    config.read(config_path)
    kafka_pwd = os.getenv("KAFKA_PASSWORD", "KHONG_TIM_THAY_MK")
    pg_pwd = os.getenv("POSTGRES_PASSWORD", "KHONG_TIM_THAY_MK")
    jaas_template = config['KAFKA']['sasl_jaas_config']
    config['KAFKA']['sasl_jaas_config'] = jaas_template.replace("${KAFKA_PASSWORD}", kafka_pwd)
    config['POSTGRES']['password'] = pg_pwd
    
    print(f"Đã tải cấu hình thành công")
    return config

if __name__ == "__main__":
    conf = load_config()
    print("JAAS Config:", conf['KAFKA']['sasl_jaas_config'])
    print("Postgres Password:", conf['POSTGRES']['password'])
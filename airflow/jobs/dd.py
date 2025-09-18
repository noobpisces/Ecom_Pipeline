# # test_minio.py
# import os
# import sys
# import uuid
# import time
# import boto3
# from botocore.client import Config
# from botocore.exceptions import ClientError, EndpointConnectionError, NoCredentialsError
# from clickhouse_driver import Client
# from dotenv import load_dotenv
# load_dotenv()


# def main():
#     try:
#         ch_client = Client(
#             host=os.getenv('CLICKHOUSE_HOST', 'clickhouse'),
#             port=os.getenv('CLICKHOUSE_PORT', '9000'),
#             user=os.getenv('CLICKHOUSE_USER', 'admin'),
#             password=os.getenv('CLICKHOUSE_PASSWORD', '123456'),
#             database=os.getenv('CLICKHOUSE_DB', 'default')
#         )

#         # Kiểm tra kết nối bằng query đơn giản
#         version = ch_client.execute("SELECT version()")
#         print(f"Connected to ClickHouse, version: {version[0][0]}")

#     except Exception as e:
#         print(f"ClickHouse initialization error: {str(e)}")
#         raise

# if __name__ == "__main__":
#     try:
#         print("Starting ClickHouse initialization test...")
#         main()
#     except EndpointConnectionError as e:
#         print(f"[ERR] Cannot connect to endpoint. Check AWS_S3_ENDPOINT. Detail: {e}")
#         sys.exit(2)
#     except NoCredentialsError:
#         print("[ERR] Missing credentials. Set AWS_S3_ACCESS_KEY_ID / AWS_S3_SECRET_ACCESS_KEY.")
#         sys.exit(3)
#     except Exception as e:
#         print(f"[ERR] Unexpected error: {e}")
#         sys.exit(4)

# h.py  — ETL: Postgres + S3  -> ClickHouse (clickhouse-connect)
import os
import io
import json
import uuid
import pandas as pd
import boto3
from sqlalchemy import create_engine
from dotenv import load_dotenv
import psycopg2  # optional: for driver availability via SQLAlchemy
from pathlib import Path
from datetime import datetime
import clickhouse_connect  # <-- dùng clickhouse-connect (HTTP 8123)
load_dotenv()


class InitialHistoricETL:
    def __init__(self):
        # Init sources
        self._init_postgres()
        self._init_s3()
        self._init_clickhouse()  # <-- thay Snowflake bằng ClickHouse

        # Bảng nguồn
        self.postgres_tables = ['categories', 'subcategories', 'order_items', 'interactions']
        self.s3_tables = ['customers', 'products', 'orders', 'reviews']

        # Batch info
        self.batch_id = str(uuid.uuid4())
        self.batch_timestamp = datetime.now()

    # --------------------------
    # Init connections
    # --------------------------
    def _init_postgres(self):
        self.pg_conn_string = (
            f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
            f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
        )
        self.pg_engine = create_engine(self.pg_conn_string)

    def _init_s3(self):
        s3_credentials = {
            'aws_access_key_id': os.getenv('AWS_S3_ACCESS_KEY_ID'),
            'aws_secret_access_key': os.getenv('AWS_S3_SECRET_ACCESS_KEY'),
            'endpoint_url': os.getenv('AWS_S3_ENDPOINT')
        }
        self.s3_client = boto3.client('s3', **s3_credentials)
        self.historic_bucket = os.getenv('AWS_S3_HISTORIC_SYNTH')
        self.latest_bucket = os.getenv('AWS_S3_LATEST_SYNTH')

    def _init_clickhouse(self):
        """Kết nối ClickHouse qua HTTP (clickhouse-connect)."""
        host = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
        port = int(os.getenv('CLICKHOUSE_PORT', '8123'))
        user = os.getenv('CLICKHOUSE_USER', 'default')
        password = os.getenv('CLICKHOUSE_PASSWORD', '')
        database = os.getenv('CLICKHOUSE_DB', 'default')

        # Tạo client
        self.ch_client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password,
            database=database
        )

        # Đảm bảo database tồn tại
        self.ch_client.command(f"CREATE DATABASE IF NOT EXISTS {self._q(database)}")
        ver = self.ch_client.query("SELECT version()").result_rows[0][0]
        print(f"✅ Connected to ClickHouse {ver} | DB={database}")

    # --------------------------
    # Extract
    # --------------------------
    def extract_from_postgres(self, table_name, data_source):
        try:
            table_prefix = 'latest_' if data_source == 'latest' else ''
            query = f"SELECT * FROM {table_prefix}{table_name}"
            return pd.read_sql(query, self.pg_engine)
        except Exception as e:
            print(f"PostgreSQL extraction error for {table_name}: {str(e)}")
            raise

    def extract_from_s3(self, table_name, data_source):
        try:
            bucket = self.latest_bucket if data_source == 'latest' else self.historic_bucket
            response = self.s3_client.get_object(Bucket=bucket, Key=f'json/{table_name}.json')
            json_content = json.loads(response['Body'].read().decode('utf-8'))
            return pd.DataFrame(json_content['data'])
        except Exception as e:
            print(f"S3 extraction error for {table_name}: {str(e)}")
            raise

    # --------------------------
    # Helpers & transforms
    # --------------------------
    def _q(self, ident: str) -> str:
        """Quote identifier for ClickHouse."""
        return f"`{ident}`"

    def get_primary_keys(self, table_name):
        pk_mapping = {
            'customers': ['CUSTOMER_ID'],
            'orders': ['ORDER_ID'],
            'products': ['PRODUCT_ID'],
            'order_items': ['ORDER_ITEM_ID'],
            'categories': ['CATEGORY_ID'],
            'subcategories': ['SUBCATEGORY_ID'],
            'reviews': ['REVIEW_ID'],
            'interactions': ['EVENT_ID']
        }
        return pk_mapping.get(table_name.replace('latest_', ''), [])

    def remove_duplicate_primary_keys(self, df, table_name):
        try:
            primary_keys = self.get_primary_keys(table_name)
            if not primary_keys:
                return df

            missing_keys = [k for k in primary_keys if k not in df.columns]
            if missing_keys:
                print(f"⚠️ Missing PKs {missing_keys} in {table_name}. Skip dedup.")
                return df

            # Ưu tiên bản ghi mới nếu có LOADED_AT
            if "LOADED_AT" in df.columns:
                df = df.sort_values(by="LOADED_AT", ascending=False)
            df = df.drop_duplicates(subset=primary_keys, keep="first")
            return df
        except Exception as e:
            print(f"Dedup error {table_name}: {str(e)}")
            raise

    def flatten_json_df(self, df, table_name):
        try:
            json_columns = [
                col for col in df.columns
                if df[col].dtype == 'object' and
                isinstance(df[col].dropna().iloc[0] if not df[col].isna().all() else None, (dict, list))
            ]
            if not json_columns:
                return df

            flat_df = df.copy()
            for col in json_columns:
                try:
                    sample = df[col].dropna().iloc[0] if not df[col].isna().all() else None
                    if isinstance(sample, dict):
                        flattened = pd.json_normalize(df[col].dropna(), sep='_')
                        flat_df = flat_df.drop(columns=[col])
                        for new_col in flattened.columns:
                            flat_df[f"{col}_{new_col}"] = flattened[new_col].reindex(flat_df.index)
                    elif isinstance(sample, list):
                        flat_df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, list) else x)
                except Exception as e:
                    print(f"Warn: Cannot flatten col {col}: {str(e)}")
                    continue
            return flat_df
        except Exception as e:
            print(f"JSON flattening error for {table_name}: {str(e)}")
            raise

    def transform_data(self, df, table_name):
        try:
            df = self.flatten_json_df(df, table_name)

            # Add metadata
            df['DATA_SOURCE'] = 'historic'
            df['BATCH_ID'] = self.batch_id
            df['LOADED_AT'] = self.batch_timestamp

            # Chuẩn hóa datetime về pandas datetime (để map thành DateTime64 khi tạo bảng)
            for col in df.columns:
                if 'date' in col.lower() or 'time' in col.lower() or col.upper() in ['LOADED_AT', 'CREATED_AT', 'UPDATED_AT']:
                    try:
                        df[col] = pd.to_datetime(df[col], errors='ignore', utc=False)
                    except Exception:
                        pass

            # NA -> None
            df = df.where(pd.notna(df), None)

            # Uppercase tên cột cho đồng nhất
            df.columns = [c.upper() for c in df.columns]

            # Dedup theo PK
            df = self.remove_duplicate_primary_keys(df, table_name)
            return df
        except Exception as e:
            print(f"Transform error for {table_name}: {str(e)}")
            raise

    def get_date_column(self, table_name):
        date_columns = {
            'customers': ['signup_date', 'last_login', 'created_at'],
            'orders': ['order_date', 'created_at', 'updated_at'],
            'products': ['created_at'],
            'order_items': ['created_at', 'order_date'],
            'reviews': ['created_at', 'order_date'],
            'interactions': ['event_date', 'created_at'],
            'categories': ['created_at'],
            'subcategories': ['created_at']
        }
        clean_table_name = table_name.replace('latest_', '')
        return date_columns.get(clean_table_name, ['created_at'])

    def find_valid_date_column(self, df, table_name):
        for col in self.get_date_column(table_name):
            if col in df.columns:
                return col
        return None

    # --------------------------
    # Save to S3 (CSV + metadata)
    # --------------------------
    def save_to_s3_historic(self, df, table_name, metadata):
        try:
            output_dir = Path('ingested_data')
            output_dir.mkdir(parents=True, exist_ok=True)

            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)

            self.s3_client.put_object(
                Bucket=self.historic_bucket,
                Key=f'csv/{table_name}.csv',
                Body=csv_buffer.getvalue().encode('utf-8')
            )
            self.s3_client.put_object(
                Bucket=self.historic_bucket,
                Key=f'csv/{table_name}_metadata.json',
                Body=json.dumps(metadata, default=str)
            )
            print(f"✅ Saved {table_name} to S3 historic (CSV + metadata)")
        except Exception as e:
            print(f"S3 save error: {str(e)}")
            raise

    # --------------------------
    # ClickHouse load
    # --------------------------
    def _map_ch_type(self, dtype) -> str:
        """Map pandas dtype -> ClickHouse type."""
        if pd.api.types.is_bool_dtype(dtype):
            return 'Bool'
        if pd.api.types.is_integer_dtype(dtype):
            return 'Int64'
        if pd.api.types.is_float_dtype(dtype):
            return 'Float64'
        if pd.api.types.is_datetime64_any_dtype(dtype):
            return 'DateTime64(3)'
        # mặc định
        return 'String'

    def _create_clickhouse_table_if_not_exists(self, df: pd.DataFrame, table_name: str):
        db = os.getenv('CLICKHOUSE_DB', 'default')
        tbl = table_name.upper()

        cols = []
        for col_name, dtype in df.dtypes.items():
            ch_type = self._map_ch_type(dtype)
            cols.append(f"{self._q(col_name)} {ch_type}")

        columns_sql = ",\n    ".join(cols)
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self._q(db)}.{self._q(tbl)} (
            {columns_sql}
        )
        ENGINE = MergeTree
        ORDER BY tuple()
        """
        self.ch_client.command(create_sql)
        print(f"🛠️ Ensured table {db}.{tbl} exists")

    def load_to_clickhouse(self, df: pd.DataFrame, table_name: str):
        """Ghi DataFrame vào ClickHouse bằng insert_df (chunk theo CH_INSERT_BATCH)."""
        db = os.getenv('CLICKHOUSE_DB', 'default')
        full_table = f"{db}.{table_name.upper()}"

        # Tạo bảng nếu chưa có
        self._create_clickhouse_table_if_not_exists(df, table_name)

        # Chunk insert để an toàn với DF lớn
        batch = int(os.getenv('CH_INSERT_BATCH', '100000'))
        total = len(df)
        if total == 0:
            print(f"⚠️ {full_table}: DataFrame rỗng, bỏ qua insert")
            return

        for start in range(0, total, batch):
            end = min(start + batch, total)
            self.ch_client.insert_df(full_table, df.iloc[start:end])
            print(f"➡️ Inserted rows {start}..{end - 1} into {full_table}")

        print(f"✅ Loaded {total} rows to {full_table}")

    # --------------------------
    # Main run
    # --------------------------
    def run_initial_load(self):
        try:
            print("\n🚀 Starting initial historic data load...")
            os.makedirs("ingested_data", exist_ok=True)

            for table in self.postgres_tables + self.s3_tables:
                print(f"\n=== Processing {table} ===")
                try:
                    # Extract
                    historic_df = (self.extract_from_s3(table, 'historic')
                                   if table in self.s3_tables
                                   else self.extract_from_postgres(table, 'historic'))

                    latest_df = (self.extract_from_s3(table, 'latest')
                                 if table in self.s3_tables
                                 else self.extract_from_postgres(table, 'latest'))

                    # Info date
                    date_column = self.find_valid_date_column(latest_df, table)
                    if date_column:
                        try:
                            min_latest_date = pd.to_datetime(latest_df[date_column]).min()
                            print(f"ℹ️ Latest data starts from: {min_latest_date}")
                        except Exception:
                            min_latest_date = None
                            print(f"⚠️ Cannot parse date column {date_column}")
                    else:
                        min_latest_date = None
                        print("⚠️ No valid date column")

                    # Transform
                    combined_df = pd.concat([historic_df, latest_df], ignore_index=True)
                    transformed_df = self.transform_data(combined_df, table)

                    # Save local CSV
                    combined_path = f"ingested_data/{table}_combined.csv"
                    transformed_df.to_csv(combined_path, index=False)
                    print(f"💾 Saved local: {combined_path}")

                    # Metadata
                    metadata = {
                        'table_name': table,
                        'batch_id': self.batch_id,
                        'timestamp': self.batch_timestamp,
                        'historic_records': len(historic_df),
                        'latest_records': len(latest_df),
                        'total_records': len(combined_df),
                        'date_column_used': date_column,
                        'columns': transformed_df.columns.tolist(),
                        'data_types': transformed_df.dtypes.astype(str).to_dict(),
                        'min_date': str(min_latest_date) if min_latest_date is not None else None
                    }

                    # Save S3 + Load ClickHouse
                    self.save_to_s3_historic(transformed_df, table, metadata)
                    self.load_to_clickhouse(transformed_df, table)

                    print(f"""
✅ Done {table}:
- Historic: {len(historic_df)} rows
- Latest:   {len(latest_df)} rows
- Total:    {len(combined_df)} rows
- Date col: {date_column if date_column else 'N/A'}
- Saved:    {combined_path}
- Loaded -> ClickHouse table: {table.upper()}
                    """)
                except Exception as e:
                    print(f"❌ Error processing {table}: {str(e)}")
                    continue
        except Exception as e:
            print(f"Initial load process error: {str(e)}")
            raise
        finally:
            try:
                self.ch_client.close()
            except Exception:
                pass
            print("\n🏁 Initial historic data load completed. Connections closed.")


if __name__ == "__main__":
    etl = InitialHistoricETL()
    etl.run_initial_load()

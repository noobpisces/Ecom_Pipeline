import os
import pandas as pd
import numpy as np
import boto3
import json
from sqlalchemy import create_engine
from dotenv import load_dotenv
import psycopg2
from pandas import json_normalize
from datetime import datetime
import uuid
import io
from pathlib import Path

# NEW: ClickHouse client
import clickhouse_connect

load_dotenv()

class IncrementalETL:
    def __init__(self):
        # Initialize connections
        self._init_postgres()
        self._init_s3()
        self._init_clickhouse()  # NEW: replace Snowflake with ClickHouse

        # Define source mappings
        self.postgres_tables = ['categories', 'subcategories', 'order_items', 'interactions']
        self.s3_tables = ['customers', 'products', 'orders', 'reviews']

        # Track batch information
        self.batch_id = str(uuid.uuid4())
        self.batch_timestamp = datetime.now()

    # --------------------------
    # Inits
    # --------------------------
    def _init_postgres(self):
        """Initialize PostgreSQL connection"""
        self.pg_conn_string = (
            f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
            f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
        )
        self.pg_engine = create_engine(self.pg_conn_string)

    def _init_s3(self):
        """Initialize S3 clients"""
        s3_credentials = {
            'aws_access_key_id': os.getenv('AWS_S3_ACCESS_KEY_ID'),
            'aws_secret_access_key': os.getenv('AWS_S3_SECRET_ACCESS_KEY'),
        }
        # Optional MinIO endpoint
        endpoint = os.getenv('AWS_S3_ENDPOINT')
        if endpoint:
            s3_credentials['endpoint_url'] = endpoint

        self.s3_client = boto3.client('s3', **s3_credentials)
        self.historic_bucket = os.getenv('AWS_S3_HISTORIC_SYNTH')
        self.latest_bucket = os.getenv('AWS_S3_LATEST_SYNTH')

    def _init_clickhouse(self):
        """Initialize ClickHouse connection via clickhouse-connect (HTTP 8123)."""
        host = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
        port = int(os.getenv('CLICKHOUSE_PORT', '8123'))
        user = os.getenv('CLICKHOUSE_USER', 'default')
        password = os.getenv('CLICKHOUSE_PASSWORD', '')
        database = os.getenv('CLICKHOUSE_DB', 'default')

        self.ch_client = clickhouse_connect.get_client(
            host=host, port=port, username=user, password=password, database=database
        )
        # make sure DB exists
        self.ch_client.command(f"CREATE DATABASE IF NOT EXISTS {self._q(database)}")
        ver = self.ch_client.query("SELECT version()").result_rows[0][0]
        print(f"✅ Connected to ClickHouse {ver} | DB={database}")

    # --------------------------
    # Extract
    # --------------------------

    def _coerce_dt(x):
        # Trả NaT nếu không parse được
        if x is None:
            return pd.NaT
        # NaN float
        if isinstance(x, float) and pd.isna(x):
            return pd.NaT
        try:
            # Thử parse thẳng; để utc=False (tz-naive) vì ta chỉ kiểm tra hợp lệ
            return pd.to_datetime(x, errors='coerce', utc=False)
        except Exception:
            return pd.NaT

    def debug_dump_invalid_created_at(df, sample_rows=50, csv_path='debug/invalid_created_at_rows.csv'):
        if 'CREATED_AT' not in df.columns:
            print('[DEBUG] Column CREATED_AT không tồn tại trong DataFrame.')
            return

        # Chuẩn hoá các "giá trị trống" thường gặp về NaN để dễ detect
        df = df.copy()
        df['CREATED_AT'] = df['CREATED_AT'].replace(
            ['', ' ', 'NULL', 'null', 'None', 'none', 'NaT', '0000-00-00', '0000-00-00 00:00:00'],
            np.nan
        )

        converted = df['CREATED_AT'].map(_coerce_dt)
        bad_mask = converted.isna()

        total = len(df)
        bad = int(bad_mask.sum())
        print(f'[DEBUG] CREATED_AT null/invalid: {bad} / {total}')

        if bad == 0:
            return

        # Cột gợi ý để in kèm cho dễ truy vết; chỉ in những cột có tồn tại
        cols_hint = [c for c in ['ID', 'INTERACTION_ID', 'USER_ID', 'EVENT_TYPE', 'SOURCE', 'CREATED_AT'] if c in df.columns]
        cols_show = cols_hint if cols_hint else ['CREATED_AT']

        # In vài dòng mẫu ra console
        print('[DEBUG] Top rows có CREATED_AT invalid:')
        print(df.loc[bad_mask, cols_show].head(sample_rows).to_string(index=False))

        # Thống kê kiểu dữ liệu raw để biết đang gặp None/chuỗi/kiểu khác
        raw_series = df.loc[bad_mask, 'CREATED_AT']
        print('[DEBUG] Phân bố kiểu Python của CREATED_AT invalid:')
        print(raw_series.map(lambda x: type(x).__name__).value_counts())

        # Tạo thư mục và dump full rows để soi ngoài đời
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        df.loc[bad_mask].to_csv(csv_path, index=False)
        print(f'[DEBUG] Đã ghi toàn bộ dòng invalid vào: {csv_path}')
    
    def extract_historic_from_s3(self, table_name):
        """Extract historic data from S3 CSV files"""
        try:
            response = self.s3_client.get_object(
                Bucket=self.historic_bucket,
                Key=f'csv/{table_name}.csv'
            )
            csv_content = response['Body'].read().decode('utf-8')
            return pd.read_csv(io.StringIO(csv_content))
        except Exception as e:
            print(f"Historic S3 extraction error for {table_name}: {str(e)}")
            raise

    def extract_latest_from_postgres(self, table_name):
        """Extract latest data from PostgreSQL"""
        try:
            query = f"SELECT * FROM latest_{table_name}"
            df = pd.read_sql(query, self.pg_engine)
            return df
        except Exception as e:
            print(f"PostgreSQL extraction error for {table_name}: {str(e)}")
            raise

    def extract_latest_from_s3(self, table_name):
        """Extract latest data from S3 (JSON)"""
        try:
            response = self.s3_client.get_object(
                Bucket=self.latest_bucket,
                Key=f'json/{table_name}.json'
            )
            json_content = json.loads(response['Body'].read().decode('utf-8'))
            return pd.DataFrame(json_content['data'])
        except Exception as e:
            print(f"Latest S3 extraction error for {table_name}: {str(e)}")
            raise

    # --------------------------
    # Helpers
    # --------------------------
    def _q(self, ident: str) -> str:
        """Backtick quote for CH identifiers."""
        return f"`{ident}`"

    def get_primary_keys(self, table_name):
        """Return primary key columns for each table"""
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
        """Drop duplicate PK rows (keep newest by LOADED_AT if present)."""
        try:
            primary_keys = self.get_primary_keys(table_name)
            if not primary_keys:
                return df

            missing = [k for k in primary_keys if k not in df.columns]
            if missing:
                print(f"⚠️ Missing PKs {missing} in {table_name}. Skip dedup.")
                return df

            if "LOADED_AT" in df.columns:
                df = df.sort_values(by="LOADED_AT", ascending=False)
            df = df.drop_duplicates(subset=primary_keys, keep="first")
            return df
        except Exception as e:
            print(f"Error removing duplicate primary keys for {table_name}: {str(e)}")
            raise

    def flatten_json_df(self, df, table_name):
        """Flatten nested JSON structures"""
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

    def _map_ch_type(self, dtype) -> str:
        if pd.api.types.is_bool_dtype(dtype):
            return 'Bool'
        if pd.api.types.is_integer_dtype(dtype):
            return 'Int64'
        if pd.api.types.is_float_dtype(dtype):
            return 'Float64'
        if pd.api.types.is_datetime64_any_dtype(dtype):
            return 'Nullable(DateTime64(3))'  # ✅ Cho phép NULL
        return 'String'

    def _create_clickhouse_table_if_not_exists(self, df: pd.DataFrame, table_name: str):
        """Create CH table if not exists. Use ReplacingMergeTree for quasi-upsert."""
        db = os.getenv('CLICKHOUSE_DB', 'default')
        tbl = table_name.upper()

        # Build columns
        cols = []
        for col_name, dtype in df.dtypes.items():
            ch_type = self._map_ch_type(dtype)
            cols.append(f"{self._q(col_name)} {ch_type}")

        columns_sql = ",\n    ".join(cols)

        # Engine choice:
        # - If PKs exist -> order by PKs and use LOADED_AT as version for ReplacingMergeTree
        # - Else -> ORDER BY tuple()
        pks = [c.upper() for c in self.get_primary_keys(table_name) if c.upper() in df.columns.str.upper()]
        if len(pks) > 0 and 'LOADED_AT' in df.columns.str.upper():
            order_by = ", ".join(self._q(pk) for pk in pks)
            engine_sql = f"ENGINE = ReplacingMergeTree({self._q('LOADED_AT')}) ORDER BY ({order_by})"
        elif len(pks) > 0:
            order_by = ", ".join(self._q(pk) for pk in pks)
            engine_sql = f"ENGINE = MergeTree ORDER BY ({order_by})"
        else:
            engine_sql = "ENGINE = MergeTree ORDER BY tuple()"

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self._q(db)}.{self._q(tbl)} (
            {columns_sql}
        )
        {engine_sql}
        """
        self.ch_client.command(create_sql)
        print(f"🛠️ Ensured table {db}.{tbl} exists")
    def _ch_temporal_columns(self, db: str, table: str):
        """Lấy danh sách cột kiểu thời gian từ ClickHouse (Date / DateTime / DateTime64)."""
        rows = self.ch_client.query(
            "SELECT name, type FROM system.columns WHERE database=%(db)s AND table=%(tb)s",
            parameters={'db': db, 'tb': table.upper()}
        ).result_rows
        temporals = {}
        for name, typ in rows:
            t = str(typ)
            if t.startswith('DateTime64'):
                temporals[name.upper()] = ('dt64', 3)  # millisecond precision
            elif t.startswith('DateTime'):
                temporals[name.upper()] = ('dt', 0)
            elif t == 'Date':
                temporals[name.upper()] = ('date', 0)
        return temporals

    def _to_iso_ms(self, v):
        """Ép 1 giá trị bất kỳ -> chuỗi ISO phù hợp DateTime64(3) hoặc None."""
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        try:
            # xử lý pandas/py datetime, numpy.datetime64, số epoch (s/ms)
            if isinstance(v, pd.Timestamp):
                dt = v.to_pydatetime()
            elif hasattr(v, 'isoformat') and not isinstance(v, str):
                dt = v  # datetime.datetime
            elif isinstance(v, (np.datetime64,)):
                dt = pd.to_datetime(v).to_pydatetime()
            elif isinstance(v, (int, float)):
                # đoán epoch giây vs milli
                dt = pd.to_datetime(v, unit='s', errors='coerce')
                if pd.isna(dt):
                    dt = pd.to_datetime(v, unit='ms', errors='coerce')
                if pd.isna(dt):
                    return None
                dt = dt.to_pydatetime()
            else:
                # nếu là string rồi: trả về như cũ (để CH parse)
                if isinstance(v, str):
                    return v
                return None
            # format milli: 2025-09-08 12:34:56.789
            s = dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:23]
            return s
        except Exception:
            return None

    def _normalize_temporals_for_clickhouse(self, df: pd.DataFrame, table_name: str):
        """Chuẩn hoá cột thời gian thành chuỗi ISO theo đúng kiểu bảng CH."""
        db = os.getenv('CLICKHOUSE_DB', 'default')
        temporals = self._ch_temporal_columns(db, table_name)
        if not temporals:
            return df

        out = df.copy()
        cols_upper = {c.upper(): c for c in out.columns}
        for up_name, (kind, _) in temporals.items():
            if up_name not in cols_upper:
                continue
            col = cols_upper[up_name]
            if kind == 'date':
                s = out[col]
                if pd.api.types.is_datetime64_any_dtype(s):
                    out[col] = s.dt.strftime('%Y-%m-%d')
                else:
                    out[col] = pd.to_datetime(s, errors='coerce').dt.strftime('%Y-%m-%d')
                out[col] = out[col].where(pd.notna(out[col]), None)
            else:
                out[col] = out[col].map(self._to_iso_ms)  # ✅ None nếu không hợp lệ
        return out

    def load_to_clickhouse(self, df: pd.DataFrame, table_name: str):
        db = os.getenv('CLICKHOUSE_DB', 'default')
        full_table = f"{db}.{table_name.upper()}"

        # Tạo bảng nếu chưa có
        self._create_clickhouse_table_if_not_exists(df, table_name)

        # 🔧 Chuẩn hoá cột thời gian đúng theo schema của CH
        df = self._normalize_temporals_for_clickhouse(df, table_name)

        batch = int(os.getenv('CH_INSERT_BATCH', '100000'))
        total = len(df)
        if total == 0:
            print(f"⚠️ {full_table}: DataFrame empty, skip insert")
            return

        for start in range(0, total, batch):
            end = min(start + batch, total)
            self.ch_client.insert_df(full_table, df.iloc[start:end])
            print(f"➡️ Inserted rows {start}..{end - 1} into {full_table}")

        print(f"✅ Loaded {total} rows to {full_table}")


    # --------------------------
    # Transform
    # --------------------------
    def transform_data(self, df, table_name):
        """Transform data (keep datetimes as datetime; add metadata; uppercase cols; dedup)."""
        try:
            # Normalize/flatten JSON
            df = self.flatten_json_df(df, table_name)

            # Add metadata
            df['DATA_SOURCE'] = 'historic'
            df['BATCH_ID'] = self.batch_id
            df['LOADED_AT'] = self.batch_timestamp  # keep as datetime for CH DateTime64

            # Try convert likely datetime columns to datetime
            for col in df.columns:
                if (
                    'date' in col.lower()
                    or 'time' in col.lower()
                    or col.upper() in ['LOADED_AT', 'CREATED_AT', 'UPDATED_AT']
                ):
                    try:
                        df[col] = pd.to_datetime(df[col], errors='ignore', utc=False)
                    except Exception:
                        pass

            # NA -> None
            df = df.where(pd.notna(df), None)

            # Uppercase column names
            df.columns = [c.upper() for c in df.columns]

            # Dedup by PK
            df = self.remove_duplicate_primary_keys(df, table_name)

            # Ensure LOADED_AT exists & is datetime
            if 'LOADED_AT' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['LOADED_AT']):
                df['LOADED_AT'] = pd.to_datetime(df['LOADED_AT'], errors='coerce')

            # Final duplicate column check
            if df.columns.duplicated().any():
                duplicates = df.columns[df.columns.duplicated()].tolist()
                raise Exception(f"Duplicate columns found: {duplicates}")

            return df

        except Exception as e:
            print(f"Transform error for {table_name}: {str(e)}")
            print("Full column list at error:")
            print(df.columns.tolist())
            raise

    # --------------------------
    # Date helpers (same logic as original)
    # --------------------------
    def convert_date_columns(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """
        Chuyển tất cả các cột datetime trong DataFrame về pandas datetime64[ns]
        dựa trên mapping từ get_date_column().
        """
        date_cols = self.get_date_column(table_name)
        out = df.copy()

        for col in date_cols:
            if col in out.columns:
                out[col] = pd.to_datetime(out[col], errors='coerce')  # ép kiểu, giá trị sai -> NaT

        return out
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
        clean = table_name.replace('latest_', '')
        return date_columns.get(clean, ['created_at'])

    def find_valid_date_column(self, df, table_name):
        for col in self.get_date_column(table_name):
            if col.upper() in df.columns:
                return col.upper()
            if col in df.columns:
                return col
        return None

    # --------------------------
    # Save historic CSV + metadata to S3
    # --------------------------
    def save_to_s3_historic(self, df, table_name, metadata):
        try:
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
            print(f"Error saving to S3 historic bucket: {str(e)}")
            raise

    # --------------------------
    # Main
    # --------------------------
    def run_etl(self):
        """Execute ETL process with historic CSV data and load to ClickHouse."""
        try:
            print("\n🚀 Starting ETL process...")
            os.makedirs("ingested_data", exist_ok=True)

            for table in self.postgres_tables + self.s3_tables:
                print(f"\n=== Processing {table} ===")
                try:
                    # Extract
                    historic_df = self.extract_historic_from_s3(table)
                    latest_df = (
                        self.extract_latest_from_s3(table)
                        if table in self.s3_tables
                        else self.extract_latest_from_postgres(table)
                    )

                    # Transform
                    latest_tx = self.transform_data(latest_df, table)
                    historic_tx = self.transform_data(historic_df, table)

                    # Date window (optional filter like original)
                    date_col = self.find_valid_date_column(latest_tx, table)
                    if date_col:
                        try:
                            latest_tx[date_col] = pd.to_datetime(latest_tx[date_col], errors='coerce')
                            historic_tx[date_col] = pd.to_datetime(historic_tx[date_col], errors='coerce')
                            min_latest = latest_tx[date_col].min()
                            print(f"ℹ️ Latest data starts from: {min_latest}")
                            if date_col in historic_tx.columns:
                                historic_tx = historic_tx[historic_tx[date_col] < min_latest]
                        except Exception as e:
                            print(f"⚠️ Error processing date column {date_col}: {str(e)}")

                    # Combine
                    combined_df = pd.concat([historic_tx, latest_tx], ignore_index=True)
                    combined_df = self.convert_date_columns(combined_df, table)
                    # Save local CSV
                    combined_path = f"ingested_data/{table}_combined.csv"
                    combined_df.to_csv(combined_path, index=False)
                    print(f"💾 Saved combined data to {combined_path}")

                    # Metadata
                    metadata = {
                        'table_name': table,
                        'batch_id': self.batch_id,
                        'timestamp': str(self.batch_timestamp),
                        'historic_records': len(historic_tx),
                        'latest_records': len(latest_tx),
                        'total_records': len(combined_df),
                        'date_column_used': date_col,
                        'columns': combined_df.columns.tolist(),
                        'data_types': combined_df.dtypes.astype(str).to_dict(),
                        'mindate': str(combined_df[date_col].min()) if date_col in combined_df.columns else None
                    }

                    # Save to S3 + Load to ClickHouse
                    self.save_to_s3_historic(combined_df, table, metadata)
                    self.load_to_clickhouse(combined_df, table)

                    print(f"""
✅ Done {table}:
- Historic: {len(historic_tx)} rows
- Latest:   {len(latest_tx)} rows
- Total:    {len(combined_df)} rows
- Date col: {date_col if date_col else 'N/A'}
- Saved:    {combined_path}
- Loaded -> ClickHouse table: {table.upper()}
                    """)
                except Exception as e:
                    print(f"❌ Error processing {table}: {str(e)}")
                    continue
        except Exception as e:
            print(f"ETL process error: {str(e)}")
            raise
        finally:
            try:
                self.ch_client.close()
            except Exception:
                pass
            print("\n🏁 ETL process completed. ClickHouse connection closed.")

if __name__ == "__main__":
    etl = IncrementalETL()
    etl.run_etl()

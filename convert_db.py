import sqlite3
import pandas as pd
import os

db_path = 'data.db'

if not os.path.exists(db_path):
    print("❌ 错误：在仓库中没有找到 data.db 文件！")
    exit(1)

try:
    conn = sqlite3.connect(db_path)
    tables_df = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)
    tables = tables_df['name'].tolist()
    
    target_table = 'activities' if 'activities' in tables else tables[0]
    print(f"👉 准备转换表: '{target_table}'")
    
    df = pd.read_sql_query(f"SELECT * FROM {target_table}", conn)
    df.to_parquet('data.parquet', engine='pyarrow')
    print("🎉 成功生成 data.parquet！")
except Exception as e:
    print("❌ 转换失败：", e)
    exit(1)
finally:
    if 'conn' in locals():
        conn.close()

import os
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import time
from datetime import datetime

# 加载环境变量
load_dotenv()

# 数据库连接信息
DB_URL = "postgresql://neondb_owner:npg_jMw9JPsavIW3@ep-curly-king-a19vze1n-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require"

def connect_to_db():
    """连接到 Neon PostgreSQL 数据库"""
    try:
        conn = psycopg2.connect(DB_URL)
        print("数据库连接成功！")
        return conn
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return None

def create_test_table(conn):
    """创建测试表"""
    try:
        cursor = conn.cursor()
        
        # 创建一个简单的测试表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS test_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                value INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        print("测试表创建成功！")
    except Exception as e:
        conn.rollback()
        print(f"创建表失败: {e}")

def insert_test_data(conn, name, value):
    """插入测试数据"""
    try:
        cursor = conn.cursor()
        
        # 插入数据
        cursor.execute(
            "INSERT INTO test_table (name, value) VALUES (%s, %s) RETURNING id",
            (name, value)
        )
        
        # 获取插入的记录ID
        record_id = cursor.fetchone()[0]
        
        conn.commit()
        print(f"数据插入成功！ID: {record_id}")
        return record_id
    except Exception as e:
        conn.rollback()
        print(f"插入数据失败: {e}")
        return None

def query_test_data(conn, record_id=None):
    """查询测试数据"""
    try:
        cursor = conn.cursor()
        
        if record_id:
            # 查询特定ID的记录
            cursor.execute("SELECT * FROM test_table WHERE id = %s", (record_id,))
            print(f"查询ID为 {record_id} 的记录:")
        else:
            # 查询所有记录
            cursor.execute("SELECT * FROM test_table ORDER BY id DESC LIMIT 10")
            print("查询最近10条记录:")
        
        # 获取并打印结果
        records = cursor.fetchall()
        
        if not records:
            print("没有找到记录")
            return
        
        # 获取列名
        column_names = [desc[0] for desc in cursor.description]
        
        # 打印列名
        print(" | ".join(column_names))
        print("-" * 80)
        
        # 打印记录
        for record in records:
            print(" | ".join(str(value) for value in record))
        
        return records
    except Exception as e:
        print(f"查询数据失败: {e}")
        return None

def main():
    """主函数"""
    # 连接数据库
    conn = connect_to_db()
    if not conn:
        return
    
    try:
        # 创建测试表
        create_test_table(conn)
        
        # 生成测试数据
        test_name = f"测试记录-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        test_value = int(time.time()) % 1000
        
        print(f"\n准备插入数据: name={test_name}, value={test_value}")
        
        # 插入测试数据
        record_id = insert_test_data(conn, test_name, test_value)
        
        if record_id:
            print("\n插入数据成功，现在查询该记录:")
            # 查询刚插入的记录
            query_test_data(conn, record_id)
            
            print("\n查询最近的记录:")
            # 查询最近的记录
            query_test_data(conn)
    finally:
        # 关闭数据库连接
        conn.close()
        print("\n数据库连接已关闭")

if __name__ == "__main__":
    main() 
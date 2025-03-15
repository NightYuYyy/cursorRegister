import os
import psycopg2
from psycopg2 import sql, pool
from dotenv import load_dotenv
import time
from datetime import datetime
from loguru import logger

# 加载环境变量
load_dotenv()

class NeonDB:
    """Neon PostgreSQL 数据库操作类"""
    
    def __init__(self, db_url=None, min_conn=1, max_conn=5, max_retries=3):
        """
        初始化数据库连接池
        
        Args:
            db_url (str, optional): 数据库连接 URL。如果为 None，则从环境变量获取。
            min_conn (int, optional): 连接池中的最小连接数。默认为 1。
            max_conn (int, optional): 连接池中的最大连接数。默认为 5。
            max_retries (int, optional): 连接失败时的最大重试次数。默认为 3。
        """
        self.max_retries = max_retries
        
        # 获取数据库连接 URL
        self.db_url = db_url or os.getenv('DATABASE_URL')
        if not self.db_url:
            raise ValueError(
                "数据库连接 URL 未配置。请在 .env 文件中设置 DATABASE_URL，"
                "或者在创建 NeonDB 实例时提供 db_url 参数。"
                "\n示例 URL 格式：postgresql://username:password@host:port/dbname?sslmode=require"
            )
        
        # 创建连接池
        retry_count = 0
        last_error = None
        
        while retry_count < self.max_retries:
            try:
                self.pool = pool.ThreadedConnectionPool(min_conn, max_conn, self.db_url)
                logger.info("数据库连接池创建成功")
                return
            except Exception as e:
                last_error = e
                retry_count += 1
                if retry_count < self.max_retries:
                    logger.warning(f"创建数据库连接池失败，正在重试 ({retry_count}/{self.max_retries}): {str(e)}")
                    time.sleep(1)  # 等待1秒后重试
                
        logger.error(f"创建数据库连接池失败，已重试 {self.max_retries} 次: {str(last_error)}")
        raise last_error
    
    def get_connection(self):
        """
        从连接池获取连接
        
        Returns:
            connection: 数据库连接对象
        """
        try:
            conn = self.pool.getconn()
            return conn
        except Exception as e:
            logger.error(f"从连接池获取连接失败: {e}")
            raise
    
    def release_connection(self, conn):
        """
        将连接归还到连接池
        
        Args:
            conn: 要归还的数据库连接对象
        """
        self.pool.putconn(conn)
    
    def close_all(self):
        """关闭所有连接并销毁连接池"""
        self.pool.closeall()
        logger.info("所有数据库连接已关闭")
    
    def execute_query(self, query, params=None, fetch_one=False, fetch_all=False):
        """
        执行 SQL 查询
        
        Args:
            query (str): SQL 查询语句
            params (tuple, optional): 查询参数。默认为 None。
            fetch_one (bool, optional): 是否获取一条结果。默认为 False。
            fetch_all (bool, optional): 是否获取所有结果。默认为 False。
            
        Returns:
            结果取决于 fetch_one 和 fetch_all 参数:
            - 如果 fetch_one=True: 返回一条记录
            - 如果 fetch_all=True: 返回所有记录
            - 否则: 返回受影响的行数
        """
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(query, params)
            
            if fetch_one:
                result = cursor.fetchone()
            elif fetch_all:
                result = cursor.fetchall()
            else:
                result = cursor.rowcount
                
            conn.commit()
            return result
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"执行查询失败: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.release_connection(conn)
    
    def execute_many(self, query, params_list):
        """
        执行批量 SQL 操作
        
        Args:
            query (str): SQL 查询语句
            params_list (list): 参数列表，每个元素是一个参数元组
            
        Returns:
            int: 受影响的行数
        """
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.executemany(query, params_list)
            result = cursor.rowcount
                
            conn.commit()
            return result
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"执行批量操作失败: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.release_connection(conn)
    
    def create_table(self, table_name, columns):
        """
        创建表
        
        Args:
            table_name (str): 表名
            columns (list): 列定义列表，每个元素是一个字符串，表示一个列的定义
            
        Returns:
            bool: 是否成功创建表
        """
        query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(columns)}
            )
        """
        try:
            self.execute_query(query)
            logger.info(f"表 {table_name} 创建成功")
            return True
        except Exception as e:
            logger.error(f"创建表 {table_name} 失败: {e}")
            return False
    
    def insert(self, table_name, data, return_id=False):
        """
        插入数据
        
        Args:
            table_name (str): 表名
            data (dict): 要插入的数据，键是列名，值是列值
            return_id (bool, optional): 是否返回插入记录的 ID。默认为 False。
            
        Returns:
            如果 return_id=True: 返回插入记录的 ID
            否则: 返回是否成功插入
        """
        columns = list(data.keys())
        values = list(data.values())
        
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        
        if return_id:
            query += " RETURNING id"
            try:
                result = self.execute_query(query, tuple(values), fetch_one=True)
                logger.info(f"数据插入表 {table_name} 成功，ID: {result[0]}")
                return result[0]
            except Exception as e:
                logger.error(f"插入数据到表 {table_name} 失败: {e}")
                return None
        else:
            try:
                self.execute_query(query, tuple(values))
                logger.info(f"数据插入表 {table_name} 成功")
                return True
            except Exception as e:
                logger.error(f"插入数据到表 {table_name} 失败: {e}")
                return False
    
    def update(self, table_name, data, condition, condition_params):
        """
        更新数据
        
        Args:
            table_name (str): 表名
            data (dict): 要更新的数据，键是列名，值是列值
            condition (str): 更新条件，例如 "id = %s"
            condition_params (tuple): 条件参数
            
        Returns:
            int: 受影响的行数
        """
        set_clause = ', '.join([f"{key} = %s" for key in data.keys()])
        values = list(data.values()) + list(condition_params)
        
        query = f"UPDATE {table_name} SET {set_clause} WHERE {condition}"
        
        try:
            result = self.execute_query(query, tuple(values))
            logger.info(f"更新表 {table_name} 成功，影响 {result} 行")
            return result
        except Exception as e:
            logger.error(f"更新表 {table_name} 失败: {e}")
            return 0
    
    def delete(self, table_name, condition, condition_params):
        """
        删除数据
        
        Args:
            table_name (str): 表名
            condition (str): 删除条件，例如 "id = %s"
            condition_params (tuple): 条件参数
            
        Returns:
            int: 受影响的行数
        """
        query = f"DELETE FROM {table_name} WHERE {condition}"
        
        try:
            result = self.execute_query(query, condition_params)
            logger.info(f"从表 {table_name} 删除数据成功，影响 {result} 行")
            return result
        except Exception as e:
            logger.error(f"从表 {table_name} 删除数据失败: {e}")
            return 0
    
    def select(self, table_name, columns="*", condition=None, condition_params=None, 
               order_by=None, limit=None, offset=None, fetch_one=False, fetch_all=True):
        """
        查询数据
        
        Args:
            table_name (str): 表名
            columns (str, optional): 要查询的列，默认为 "*"
            condition (str, optional): 查询条件，例如 "id = %s"。默认为 None。
            condition_params (tuple, optional): 条件参数。默认为 None。
            order_by (str, optional): 排序条件，例如 "id DESC"。默认为 None。
            limit (int, optional): 限制返回的记录数。默认为 None。
            offset (int, optional): 跳过的记录数。默认为 None。
            fetch_one (bool, optional): 是否只获取一条记录。默认为 False。
            fetch_all (bool, optional): 是否获取所有记录。默认为 True。
            
        Returns:
            如果 fetch_one=True: 返回一条记录
            如果 fetch_all=True: 返回所有记录
            否则: 返回 None
        """
        query = f"SELECT {columns} FROM {table_name}"
        params = []
        
        if condition:
            query += f" WHERE {condition}"
            if condition_params:
                params.extend(condition_params)
        
        if order_by:
            query += f" ORDER BY {order_by}"
        
        if limit:
            query += f" LIMIT {limit}"
        
        if offset:
            query += f" OFFSET {offset}"
        
        try:
            if fetch_one:
                result = self.execute_query(query, tuple(params) if params else None, fetch_one=True)
            elif fetch_all:
                result = self.execute_query(query, tuple(params) if params else None, fetch_all=True)
            else:
                result = self.execute_query(query, tuple(params) if params else None)
            
            return result
        except Exception as e:
            logger.error(f"查询表 {table_name} 失败: {e}")
            return None
    
    def table_exists(self, table_name):
        """
        检查表是否存在
        
        Args:
            table_name (str): 表名
            
        Returns:
            bool: 表是否存在
        """
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            )
        """
        
        try:
            result = self.execute_query(query, (table_name,), fetch_one=True)
            return result[0]
        except Exception as e:
            logger.error(f"检查表 {table_name} 是否存在失败: {e}")
            return False
    
    def get_columns(self, table_name):
        """
        获取表的列信息
        
        Args:
            table_name (str): 表名
            
        Returns:
            list: 列信息列表，每个元素是一个字典，包含列名、数据类型等信息
        """
        query = """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
        """
        
        try:
            result = self.execute_query(query, (table_name,), fetch_all=True)
            columns = []
            
            for row in result:
                columns.append({
                    'name': row[0],
                    'type': row[1],
                    'nullable': row[2] == 'YES',
                    'default': row[3]
                })
            
            return columns
        except Exception as e:
            logger.error(f"获取表 {table_name} 的列信息失败: {e}")
            return []

    def create_accounts_table(self):
        """
        创建账号表 - 基于当前项目实际使用的字段
        """
        columns = [
            "id SERIAL PRIMARY KEY",
            "domain VARCHAR(255) NOT NULL",
            "email VARCHAR(255) NOT NULL UNIQUE",
            "password VARCHAR(255) NOT NULL",
            "cookies_str TEXT",  # 存储完整的 cookie 字符串
            "api_key VARCHAR(255)",
            "moe_mail_url TEXT",
            "quota VARCHAR(50)",  # 存储格式如 "10 / 100"
            "days_remaining VARCHAR(50)",
            "status VARCHAR(50) DEFAULT 'active'",
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
        ]
        return self.create_table('accounts', columns)

    def init_database(self):
        """
        初始化数据库，创建所有必要的表
        """
        try:
            self.create_accounts_table()
            logger.info("数据库初始化完成")
            return True
        except Exception as e:
            logger.error(f"数据库初始化失败: {e}")
            return False

    def add_account(self, account_data):
        """
        添加新账号
        
        Args:
            account_data (dict): 账号数据，包含以下字段：
                - domain: 域名
                - email: 邮箱
                - password: 密码
                - cookies_str: Cookie字符串
                - api_key: API密钥（可选）
                - moe_mail_url: 邮箱服务地址（可选）
                - quota: 额度信息（可选）
                - days_remaining: 剩余天数（可选）
            
        Returns:
            int: 新账号的 ID，如果失败则返回 None
        """
        try:
            # 确保必填字段存在
            required_fields = ['domain', 'email', 'password']
            for field in required_fields:
                if field not in account_data:
                    raise ValueError(f"缺少必填字段: {field}")
            
            # 设置更新时间
            account_data['updated_at'] = datetime.now()
            
            return self.insert('accounts', account_data, return_id=True)
        except Exception as e:
            logger.error(f"添加账号失败: {e}")
            return None

    def get_account_list(self, status=None, limit=None, offset=None):
        """
        获取账号列表
        
        Args:
            status (str): 可选的状态过滤
            limit (int): 限制返回的记录数
            offset (int): 分页偏移量
            
        Returns:
            list: 账号列表
        """
        try:
            condition = "status = %s" if status else None
            condition_params = (status,) if status else None
            
            columns = """
                id, domain, email, password, cookies_str, 
                api_key, moe_mail_url, quota, days_remaining, 
                status, created_at, updated_at
            """
            
            return self.select(
                'accounts',
                columns=columns,
                condition=condition,
                condition_params=condition_params,
                order_by="created_at DESC",
                limit=limit,
                offset=offset,
                fetch_all=True
            )
        except Exception as e:
            logger.error(f"获取账号列表失败: {e}")
            return []

    def get_account_by_email(self, email):
        """
        通过邮箱获取账号信息
        
        Args:
            email (str): 邮箱地址
            
        Returns:
            tuple: 账号信息，如果不存在则返回 None
        """
        try:
            return self.select(
                'accounts',
                condition="email = %s",
                condition_params=(email,),
                fetch_one=True
            )
        except Exception as e:
            logger.error(f"通过邮箱获取账号失败: {e}")
            return None

    def import_from_csv(self, csv_file_path):
        """
        从 CSV 文件导入账号数据
        
        Args:
            csv_file_path (str): CSV 文件路径
            
        Returns:
            bool: 是否导入成功
        """
        try:
            logger.info(f"开始从文件导入数据: {csv_file_path}")
            
            # 读取并解析 CSV/ENV 文件
            data = {}
            with open(csv_file_path, 'r', encoding='utf-8') as f:
                if csv_file_path.endswith('.env'):
                    # 解析 .env 文件
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#'):
                            try:
                                key, value = line.split('=', 1)
                                key = key.strip()
                                value = value.strip().strip("'").strip('"')
                                data[key] = value
                            except ValueError:
                                continue
                else:
                    # 解析 CSV 文件
                    import csv
                    reader = csv.DictReader(f)
                    for row in reader:
                        if 'variable' in row and 'value' in row:
                            data[row['variable']] = row['value']

            logger.debug(f"解析到的数据: {data}")
            
            # 构建账号数据
            account_data = {
                'domain': data.get('DOMAIN', ''),
                'email': data.get('EMAIL', ''),
                'password': data.get('PASSWORD', ''),
                'cookies_str': data.get('COOKIES_STR', ''),
                'api_key': data.get('API_KEY', ''),
                'moe_mail_url': data.get('MOE_MAIL_URL', ''),
                'quota': data.get('QUOTA', ''),
                'days_remaining': data.get('DAYS', '')
            }

            # 检查必要字段
            if not account_data['email'] or not account_data['password']:
                logger.warning(f"跳过导入：文件 {csv_file_path} 缺少必要字段 (email 或 password)")
                return False

            logger.info(f"准备导入账号: {account_data['email']}")

            # 检查邮箱是否已存在
            existing_account = self.get_account_by_email(account_data['email'])
            if existing_account:
                logger.info(f"更新已存在的账号: {account_data['email']}")
                return self.update_account(existing_account[0], account_data)
            else:
                logger.info(f"添加新账号: {account_data['email']}")
                result = self.add_account(account_data)
                if result:
                    logger.info(f"成功添加账号: {account_data['email']}")
                    return True
                else:
                    logger.error(f"添加账号失败: {account_data['email']}")
                    return False

        except Exception as e:
            logger.error(f"导入文件 {csv_file_path} 失败: {str(e)}")
            logger.exception("详细错误信息:")
            return False

    def export_to_csv(self, csv_file_path):
        """
        导出账号数据到 CSV 文件
        
        Args:
            csv_file_path (str): CSV 文件路径
            
        Returns:
            bool: 是否导出成功
        """
        try:
            accounts = self.get_account_list()
            if not accounts:
                return False
            
            import csv
            with open(csv_file_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['variable', 'value'])  # 保持原有格式
                
                for account in accounts:
                    # 使用原有的环境变量格式
                    writer.writerow(['DOMAIN', account[1]])  # domain
                    writer.writerow(['EMAIL', account[2]])   # email
                    writer.writerow(['PASSWORD', account[3]]) # password
                    if account[4]:  # cookies_str
                        writer.writerow(['COOKIES_STR', account[4]])
                    if account[5]:  # api_key
                        writer.writerow(['API_KEY', account[5]])
                    if account[6]:  # moe_mail_url
                        writer.writerow(['MOE_MAIL_URL', account[6]])
                    if account[7]:  # quota
                        writer.writerow(['QUOTA', account[7]])
                    if account[8]:  # days_remaining
                        writer.writerow(['DAYS', account[8]])
            return True
        except Exception as e:
            logger.error(f"导出账号到 CSV 失败: {e}")
            return False

    def update_account(self, account_id, update_data):
        """
        更新账号信息
        
        Args:
            account_id (int): 账号 ID
            update_data (dict): 要更新的数据，可以包含以下字段：
                - domain: 域名
                - email: 邮箱
                - password: 密码
                - cookies_str: Cookie字符串
                - api_key: API密钥
                - moe_mail_url: 邮箱服务地址
                - quota: 额度信息
                - days_remaining: 剩余天数
                - status: 状态
            
        Returns:
            bool: 是否更新成功
        """
        try:
            # 添加更新时间
            update_data['updated_at'] = datetime.now()
            
            # 构建 SQL 更新语句
            set_clauses = []
            values = []
            for key, value in update_data.items():
                set_clauses.append(f"{key} = %s")
                values.append(value)
            
            # 添加 WHERE 条件的参数
            values.append(account_id)
            
            # 执行更新
            sql = f"UPDATE accounts SET {', '.join(set_clauses)} WHERE id = %s"
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, values)
                    return cur.rowcount > 0
                    
        except Exception as e:
            logger.error(f"更新账号信息失败: {e}")
            return False


# 使用示例
def test_neon_db():
    """测试 NeonDB 类"""
    # 创建 NeonDB 实例
    db = NeonDB()
    
    try:
        # 创建测试表
        table_name = 'users'
        columns = [
            'id SERIAL PRIMARY KEY',
            'username VARCHAR(50) NOT NULL',
            'email VARCHAR(100) UNIQUE NOT NULL',
            'age INTEGER',
            'created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
        ]
        
        # 检查表是否存在
        if not db.table_exists(table_name):
            # 创建表
            db.create_table(table_name, columns)
            print(f"表 {table_name} 创建成功")
        else:
            print(f"表 {table_name} 已存在")
        
        # 获取表的列信息
        columns_info = db.get_columns(table_name)
        print(f"\n表 {table_name} 的列信息:")
        for col in columns_info:
            print(f"  - {col['name']} ({col['type']}), 可空: {col['nullable']}, 默认值: {col['default']}")
        
        # 插入数据
        user_data = {
            'username': f'user_{int(time.time())}',
            'email': f'user_{int(time.time())}@example.com',
            'age': 25 + (int(time.time()) % 10)
        }
        
        user_id = db.insert(table_name, user_data, return_id=True)
        print(f"\n插入用户数据成功，ID: {user_id}")
        
        # 查询刚插入的用户
        user = db.select(table_name, condition="id = %s", condition_params=(user_id,), fetch_one=True)
        print(f"\n查询用户 ID {user_id} 的结果:")
        print(f"  ID: {user[0]}")
        print(f"  用户名: {user[1]}")
        print(f"  邮箱: {user[2]}")
        print(f"  年龄: {user[3]}")
        print(f"  创建时间: {user[4]}")
        
        # 更新用户数据
        update_data = {
            'age': 30 + (int(time.time()) % 10)
        }
        
        affected_rows = db.update(table_name, update_data, "id = %s", (user_id,))
        print(f"\n更新用户 ID {user_id} 的年龄成功，影响 {affected_rows} 行")
        
        # 再次查询用户
        user = db.select(table_name, condition="id = %s", condition_params=(user_id,), fetch_one=True)
        print(f"\n更新后查询用户 ID {user_id} 的结果:")
        print(f"  ID: {user[0]}")
        print(f"  用户名: {user[1]}")
        print(f"  邮箱: {user[2]}")
        print(f"  年龄: {user[3]} (已更新)")
        print(f"  创建时间: {user[4]}")
        
        # 查询所有用户
        users = db.select(table_name, order_by="id DESC", limit=5)
        print(f"\n最近 5 个用户:")
        for user in users:
            print(f"  ID: {user[0]}, 用户名: {user[1]}, 邮箱: {user[2]}, 年龄: {user[3]}, 创建时间: {user[4]}")
        
        # 不实际删除用户，只是演示
        print(f"\n删除用户的示例代码 (未执行):")
        print(f"  db.delete('{table_name}', 'id = %s', ({user_id},))")
        
    finally:
        # 关闭所有连接
        db.close_all()


if __name__ == "__main__":
    test_neon_db() 
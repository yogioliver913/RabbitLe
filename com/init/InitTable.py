import sqlite3
import datetime
import sqlite3
from typing import List, Tuple, Union

DB_PATH = '/Users/xile/PycharmProjects/RabbitLe/com/init/mydb.db'
TABLE_NAME = 'CN_MAKET_BASIC_ALL'

def createEmptyTable():
    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.cursor()
        create = '''
        CREATE TABLE IF NOT EXISTS sensor_data(
        timestamp INTEGER NOT NULL,
        value1 REAL,
        value2 REAL,
        value3 REAL,
        value4 REAL,
        value5 REAL,
        id INTEGER PRIMARY KEY AUTOINCREMENT);
        '''
        cursor.execute(create)
        cursor.execute("CREATE INDEX idx_time ON sensor_data(timestamp);")
        conn.commit()
    finally:
        if 'conn' in locals() and conn:
            conn.close()  # 确保最终关闭


    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        create = '''
        CREATE TABLE IF NOT EXISTS sensor_data(
        timestamp INTEGER NOT NULL,
        value1 REAL,
        value2 REAL,
        value3 REAL,
        value4 REAL,
        value5 REAL,
        id INTEGER PRIMARY KEY AUTOINCREMENT);
        '''
        cursor.execute(create)
        cursor.execute("CREATE INDEX idx_time ON sensor_data(timestamp);")
        conn.commit()
        conn.close()


def batch_insert(data: List[Tuple]) -> bool:
    """
    执行SQLite批量插入操作

    参数:
        data: 待插入数据列表，每个元素为一行数据的元组
        table_name: 目标表名
        db_path: 数据库文件路径（默认'app.db'）

    返回:
        bool: True表示全部插入成功，False表示失败
    """
    if not data:
        print("警告：空数据集")
        return True  # 无数据视为成功

    conn = None
    try:
        # 1. 连接数据库（启用行工厂和连接追踪）
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")  # 启用外键约束[7]
        cursor = conn.cursor()

        # 2. 动态生成参数化查询（防止SQL注入）
        placeholders = ', '.join(['?'] * len(data[0]))
        sql = f"INSERT INTO %s (timestamp,maket_name,com_count,total_mv,float_mv,amount,pe) VALUES ({placeholders})" % TABLE_NAME

        # 3. 事务控制（显式事务提升性能）[2,5]
        conn.execute("BEGIN TRANSACTION")

        # 4. 批量执行插入[1,3]
        cursor.executemany(sql, data)

        # 5. 提交事务
        conn.commit()
        print(f"成功插入 {cursor.rowcount} 行数据")
        return True

    except sqlite3.IntegrityError as e:
        # 唯一性约束/外键约束违反[6,8]
        conn.rollback()
        print(f"数据约束冲突: {e}\n建议: 检查主键重复或外键引用")
        return False

    except sqlite3.OperationalError as e:
        # 表不存在/字段不匹配[7]
        conn.rollback()
        print(f"操作失败: {e}\n建议: 检查表结构或SQL语法")
        return False

    except sqlite3.DatabaseError as e:
        # 数据库文件损坏/磁盘满
        conn.rollback()
        print(f"数据库错误: {e}\n建议: 检查磁盘空间或数据库完整性")
        return False

    except Exception as e:
        # 捕获其他未知异常
        if conn:
            conn.rollback()
        print(f"未知错误: {type(e).__name__}: {e}")
        return False

    finally:
        # 确保连接关闭（资源清理）
        if conn:
            conn.close()


def selectOne(beginDateInt, endDateInt):
    beg_dt = datetime.datetime.strptime(str(beginDateInt), "%Y%m%d")
    end_dt = datetime.datetime.strptime(str(endDateInt), "%Y%m%d")
    beg_timestamp = int(beg_dt.timestamp())
    end_timestamp = int(end_dt.timestamp())

    sql_query = """
        SELECT timestamp,value1,value2,value3,value4,value5 FROM sensor_data 
        WHERE timestamp >= ? AND timestamp < ?
        LIMIT 1;
    """
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row  # 可选：结果按列名访问
        cursor = conn.cursor()
        cursor.execute(sql_query, (beg_timestamp, end_timestamp))
        one_row = cursor.fetchone()
        if one_row:
            row_tuple = tuple(one_row)  # 直接转换为元组
            print(row_tuple)  # 输出：(1, 'Alice')
    return row_tuple


def selectMany(beginDateInt, endDateInt):
    beg_dt = datetime.datetime.strptime(str(beginDateInt), "%Y%m%d")
    end_dt = datetime.datetime.strptime(str(endDateInt), "%Y%m%d")
    beg_timestamp = int(beg_dt.timestamp())
    end_timestamp = int(end_dt.timestamp())
    sql_query = """
        SELECT timestamp,value1,value2,value3,value4,value5 FROM sensor_data 
        WHERE timestamp >= ? AND timestamp < ?;
    """
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row  # 可选：结果按列名访问
        cursor = conn.cursor()
        cursor.execute(sql_query, (beg_timestamp, end_timestamp))
        all_rows = cursor.fetchall()
    for one_row in all_rows:
        row_tuple = tuple(one_row)  # 直接转换为元组
        print(row_tuple)
    return all_rows


def execute(sql):
    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
    except sqlite3.Error as e:
        print(f"数据库错误: {e}")
    finally:
        if 'conn' in locals() and conn:  # 检查conn是否存在且未关闭
            conn.close()


if __name__ == '__main__':
    createEmptyTable()
    # row = selectOne(20010101, 20250101)
    # rows = selectMany(20010101, 20250101)
    print("end")

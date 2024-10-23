import oracledb
from functools import wraps

def oracle_connection_manager(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        # 数据库配置
        config = {
            'user': 'agreement',
            'password': 'PiH37bZ0',
            'host': 'cnn-rds-rg-dsp-sit.cabcuuku89wx.rds.cn-north-1.amazonaws.com.cn',
            'port': '1521',
            'service_name': 'DSPSIT'
        }
        
        # 创建连接
        dsn = oracledb.makedsn(config['host'], config['port'], service_name=config['service_name'])
        connection = oracledb.connect(user=config['user'], password=config['password'], dsn=dsn)
        
        # 调用原始函数
        try:
            result = func(connection, *args, **kwargs)
            connection.commit()  # 确保更改被提交
        except Exception as e:
            connection.rollback()  # 发生错误时回滚事务
            raise e
        finally:
            # 关闭连接
            connection.close()
        
        return result
    
    return wrapper

# 使用装饰器的函数
@oracle_connection_manager
def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    columns = [col[0] for col in cursor.description]
    result = [dict(zip(columns, row)) for row in cursor.fetchall()]
    cursor.close()
    return result

@oracle_connection_manager
def execute_insert(connection, insert_query):
    cursor = connection.cursor()
    cursor.execute(insert_query)
    cursor.close()

@oracle_connection_manager
def execute_update(connection, update_query):
    cursor = connection.cursor()
    cursor.execute(update_query)
    cursor.close()

@oracle_connection_manager
def execute_delete(connection, delete_query):
    cursor = connection.cursor()
    cursor.execute(delete_query)
    cursor.close()


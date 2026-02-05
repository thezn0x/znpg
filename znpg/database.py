from psycopg import Error
from contextlib import contextmanager
from psycopg_pool import ConnectionPool
from typing import Optional, Any, Dict, List, Union
from psycopg.rows import dict_row
import json
from .query_builder import QueryBuilder
from znpg.utils.logger import get_logger

logger = get_logger(__name__)


class Database:
    def __init__(self, min_size:int = 1, max_size:int = 10, timeout:int = 30):
        self.pool: Optional[ConnectionPool] = None
        self.min_size = min_size
        self.max_size = max_size
        self.is_connected = False
        self.timeout = timeout

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.pool:
            self.pool.close()
    
    @contextmanager
    def get_connection(self):
        if not self.pool:
            raise ValueError("Connection string not set. Call url_connect() or manual_connect().")
        conn = None
        try:
            with self.pool.connection() as conn:
                yield conn
        except Error as e:
            logger.exception(f"Error while connecting to the database: {e}")
            raise

    @staticmethod
    def c_string(username, host, password, db_name, port):
        return f"postgresql://{username}:{password}@{host}:{port}/{db_name}"

    def url_connect(self, conn_string: str) -> None:
        try:
            self.pool = ConnectionPool(
                conn_string,
                min_size=self.min_size,
                max_size=self.max_size,
                timeout=self.timeout
            )
            self.is_connected = True
        except Exception:
            raise

    def manual_connect(self, username: str, host: str, password: str, db_name: str, port: int) -> None:
        conn_string = Database.c_string(username, host, password, db_name, port)
        try:
            self.pool = ConnectionPool(conn_string, min_size=self.min_size, max_size=self.max_size)
            self.is_connected = True
        except Exception:
            raise

    def query(self, sql: str, params:Optional[List[Any]]=None) -> List[dict[str,Any]]:
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql , params)
                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                return [dict(zip(columns,row)) for row in rows]

    def execute(self, sql: str, params: Optional[List[Any]] = None) -> int:
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql,params)
                conn.commit()
                return cur.rowcount

    def fetch_one(self,sql, params: Optional[List[Any]] = None) -> Optional[Dict[str, Any]]:
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                row = cursor.fetchone()
                if row is None:
                    return None
                columns = [desc[0] for desc in cursor.description]
                return dict(zip(columns, row))

    def _execute_fetch_all(self, sql: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            with conn.cursor(row_factory = dict_row) as cursor:
                cursor.execute(sql, params)
                rows = cursor.fetchall()
                return rows

    def select(self, table: str ,columns: Optional[List[str]]=None,where: Optional[Dict[str,Any]] = None, order_by: Optional[Union[str, List[str]]] = None, limit: Optional[int]= None) -> List[Dict[str, Any]]:
        sql, params = QueryBuilder.build_select_query(table, columns ,where, order_by, limit)
        try:
            result =  self._execute_fetch_all(sql, params)
            return result
        except Error as e:
            logger.error(f"Error while performing 'SELECT' operation : {e}")
            return []

    def insert(self, table: str, data: Dict[str,Any]) -> bool:
        sql_string,parameters = QueryBuilder.build_insert_query(table,data)
        try:
            self.execute(sql_string,parameters)
            return True
        except Error as e:
            logger.error(f"Error while performing 'INSERT' operation : {e}")
            return False

    def update(self, table: str, data: Dict[str, Any], conditions: Optional[Dict[str, Any]] = None, allow_all: bool = False) -> int:
        if not conditions and not allow_all:
            raise ValueError("UPDATE without WHERE is dangerous! Set allow_all=True if intentional.")
        sql_string,parameters = QueryBuilder.build_update_query(table,data,conditions)
        try:
            result = self.execute(sql_string,parameters)
            return result
        except Error as e:
            logger.error(f"Error while performing 'UPDATE' operation : {e}")
            return 0

    def delete(self,table: str, conditions: Optional[Dict[str, Any]] = None, allow_deleteall: bool = False) -> int:
        if not conditions and not allow_deleteall:
            raise ValueError("DELETE without WHERE is dangerous! Set allow_deleteall=True if intentional.")

        sql_string,parameters = QueryBuilder.build_delete_query(table,conditions)
        try:
            result = self.execute(sql_string,parameters)
            return result
        except Error as e:
            logger.error(f"Error while performing 'DELETE' operation : {e}")
            return 0
    
    def create_table(self, table: str, columns: Optional[Dict[str,str]] = None) -> bool:
        sql = QueryBuilder.build_createtable_query(table, columns)
        try:
            self.execute(sql)
            return True
        except Error as e:
            logger.error(f"Error while performing 'CREATE TABLE' operation : {e}")
            return False
    
    def drop_table(self, table: str, cascade: Optional[bool] = False, allow_action: Optional[bool] = False) -> bool:
        sql = QueryBuilder.build_droptable_query(table,cascade,allow_action)
        try:
            self.execute(sql)
            return True
        except Error as e:
            logger.error(f"Error while performing 'DROP TABLE' operation : {e}")
            return False
    
    def table_exists(self, table: str) -> Union[bool, None]:
        sql = QueryBuilder.build_findtable_query()
        try:
            result = self.query(sql,[table])
            if result and len(result) > 0:
                return True
            else:
                return False
        except Error as e:
            logger.error(f"Error while performing tables_exist() operation : {e}")
            return None

    def truncate(self, table: str) -> bool:
        sql = QueryBuilder.build_truncate_query(table)
        try:
            self.execute(sql)
            return True
        except Error as e:
            logger.error(f"Error while performing TRUNCATE operation : {e}")
            return False

    def bulk_insert(self, table: str, data: List[Dict[str,Any]], on_conflict: Optional[str] = None) -> Union[int,None]:
        if not data:
            return 0
        sql, params = QueryBuilder.build_bulk_insert(table, data, on_conflict)
        try:
            result = self.execute(sql,params)
            return result
        except Error as e:
            logger.error(f"Error while performing bulk_insert() operation : {e}")
            return 0

    def get_table_columns(self, table: str) -> Union[None,list[str]]:
        sql = QueryBuilder.build_allcolumns_query()
        try:
            result = self.query(sql,[table])
            columns = [row['column_name'] for row in result]
            return columns
        except Error as e:
            logger.error(f"Error while performing get_table_colums() operation : {e}")
            return None

    def get_by_id(self, table: str, id_name: str, id: Union[str,int]):
        sql = QueryBuilder.build_getby_id(table, id_name)
        param = [id]
        try:
            result = self.query(sql,param)
            return result
        except Error as e:
            logger.error(f"Error while performing get_by_id() operation : {e}")
            return None

    def count(self, table: str, where: Optional[Dict[str,Any]] = None) -> Union[int,None]:
        sql,params = QueryBuilder.build_count_query(table,where)
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute(sql,params)
                    result = cursor.fetchone()
                    return result[0] if result else None
                except Error as e:
                    logger.error(f"Error while performing count() operation : {e}")
                    return None

    def exists(self, table: str, where: Dict[str,Any]) -> Union[None,bool]:
        sql,params = QueryBuilder.build_exists_query(table,where)
        try:
            result = self._execute_fetch_all(sql,params)
            return bool(result) and result[0].get("exists", False)
        except Error as e:
            logger.error(f"Error while performing exists() operation : {e}")
            return None

    def create_index(self, table: str, columns: List[str], unique: bool = False):
        sql = QueryBuilder.build_create_index(table,columns,unique)
        return self.execute(sql)

    def vacuum(self, table: str = None, analyze: bool = True):
        sql = QueryBuilder.build_vacuum(table,analyze)
        return self.execute(sql)

    def is_healthy(self) -> bool:
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    return True
        except Exception:
            return False

    def stats(self) -> Dict:
        if self.pool:
            return {
                "size": self.pool.size,
                "available": self.pool.get_stats().get("available", 0),
                "used": self.pool.get_stats().get("used", 0)
            }
        return {}

    @staticmethod
    def export_to_json(file:str,data:dict, indent:int=4):
        try:
            with open(file,"w") as f:
                json.dump(data, f, indent=indent)
            return True
        except Exception as e:
            print(f"Error while exporting to json : {e}")
            return False


    @staticmethod
    def import_from_json(file:str):
        try:
            with open(file,"r") as f:
                data = json.load(f)
            return data
        except Exception as e:
            print(f"Error while exporting to json : {e}")
            return False
        
    @contextmanager
    def transaction(self):
        with self.get_connection() as conn:
            try:
                yield conn
                conn.commit()
            except:
                conn.rollback()
                raise
                
    def close(self) -> None:
        if self.pool:
            self.pool.close()
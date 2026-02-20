from psycopg import Error
from psycopg import Connection
from contextlib import contextmanager
from psycopg_pool import ConnectionPool
from typing import Optional, Any, Dict, List, Union
from psycopg.rows import dict_row
import json
from .query_builder import QueryBuilder
from znpg.utils.logger import get_logger

logger = get_logger(__name__)


class Database:
    def __init__(self, min_size: int = 1, max_size: int = 10, timeout: int = 30):
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

    # -- connection stuff --

    @contextmanager
    def get_connection(self):
        if not self.pool:
            raise ValueError("No pool found. Call url_connect() or manual_connect() first.")
        try:
            with self.pool.connection() as conn:
                yield conn
        except Error as e:
            logger.exception(f"Failed to get connection from pool: {e}")
            raise

    @contextmanager
    def transaction(self):
        # yields a conn so you can pass it into methods — they'll all run on the same connection
        # if anything blows up, everything rolls back. atomicity guaranteed.
        with self.get_connection() as conn:
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    @staticmethod
    def c_string(username, host, password, db_name, port) -> str:
        return f"postgresql://{username}:{password}@{host}:{port}/{db_name}"

    def url_connect(self, conn_string: str) -> None:
        self.pool = ConnectionPool(
            conn_string,
            min_size=self.min_size,
            max_size=self.max_size,
            timeout=self.timeout
        )
        self.is_connected = True

    def manual_connect(self, username: str, host: str, password: str, db_name: str, port: int) -> None:
        conn_string = Database.c_string(username, host, password, db_name, port)
        self.pool = ConnectionPool(conn_string, min_size=self.min_size, max_size=self.max_size)
        self.is_connected = True

    # -- the actual workhorse: two internal runners so every method stays DRY --

    def _run_query(self, sql: str, params=None, conn: Connection = None) -> List[Dict[str, Any]]:
        # returns rows as list of dicts. pass conn to run inside a transaction.
        def _fetch(c: Connection):
            with c.cursor() as cur:
                cur.execute(sql, params)
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

        if conn:
            return _fetch(conn)
        with self.get_connection() as c:
            return _fetch(c)

    def _run_execute(self, sql: str, params=None, conn: Connection = None) -> int:
        # for INSERT/UPDATE/DELETE — returns rowcount. pass conn to run inside a transaction.
        def _exec(c: Connection):
            with c.cursor() as cur:
                cur.execute(sql, params)
                return cur.rowcount

        if conn:
            return _exec(conn)
        with self.get_connection() as c:
            # auto-commit when no external transaction is managing this
            result = _exec(c)
            c.commit()
            return result

    # -- public query methods --

    def query(self, sql: str, params: Optional[List[Any]] = None, conn: Connection = None) -> List[Dict[str, Any]]:
        return self._run_query(sql, params, conn)

    def execute(self, sql: str, params: Optional[List[Any]] = None, conn: Connection = None) -> int:
        return self._run_execute(sql, params, conn)

    def fetch_one(self, sql: str, params: Optional[List[Any]] = None, conn: Connection = None) -> Optional[Dict[str, Any]]:
        rows = self._run_query(sql, params, conn)
        return rows[0] if rows else None

    # -- CRUD --

    def select(self, table: str, columns: Optional[List[str]] = None, where: Optional[Dict[str, Any]] = None,
               order_by: Optional[Union[str, List[str]]] = None, limit: Optional[int] = None,
               conn: Connection = None) -> List[Dict[str, Any]]:
        sql, params = QueryBuilder.build_select_query(table, columns, where, order_by, limit)
        try:
            return self._run_query(sql, params, conn)
        except Error as e:
            logger.error(f"SELECT failed on '{table}': {e}")
            return []

    def insert(self, table: str, data: Dict[str, Any], conn: Connection = None) -> bool:
        sql, params = QueryBuilder.build_insert_query(table, data)
        try:
            self._run_execute(sql, params, conn)
            return True
        except Error as e:
            logger.error(f"INSERT failed on '{table}': {e}")
            return False

    def update(self, table: str, data: Dict[str, Any], conditions: Optional[Dict[str, Any]] = None,
               allow_all: bool = False, conn: Connection = None) -> int:
        if not conditions and not allow_all:
            raise ValueError("UPDATE without WHERE is dangerous. Pass allow_all=True if you mean it.")
        sql, params = QueryBuilder.build_update_query(table, data, conditions)
        try:
            return self._run_execute(sql, params, conn)
        except Error as e:
            logger.error(f"UPDATE failed on '{table}': {e}")
            return 0

    def delete(self, table: str, conditions: Optional[Dict[str, Any]] = None,
               allow_deleteall: bool = False, conn: Connection = None) -> int:
        if not conditions and not allow_deleteall:
            raise ValueError("DELETE without WHERE is dangerous. Pass allow_deleteall=True if you mean it.")
        sql, params = QueryBuilder.build_delete_query(table, conditions)
        try:
            return self._run_execute(sql, params, conn)
        except Error as e:
            logger.error(f"DELETE failed on '{table}': {e}")
            return 0

    def bulk_insert(self, table: str, data: List[Dict[str, Any]], on_conflict: Optional[str] = None,
                    conn: Connection = None) -> int:
        if not data:
            return 0  # nothing to do
        sql, params = QueryBuilder.build_bulk_insert(table, data, on_conflict)
        try:
            return self._run_execute(sql, params, conn)
        except Error as e:
            logger.error(f"bulk_insert() failed on '{table}': {e}")
            return 0

    # -- DDL --

    def create_table(self, table: str, columns: Optional[Dict[str, str]] = None) -> bool:
        sql = QueryBuilder.build_createtable_query(table, columns)
        try:
            self._run_execute(sql)
            return True
        except Error as e:
            logger.error(f"CREATE TABLE '{table}' failed: {e}")
            return False

    def drop_table(self, table: str, cascade: bool = False, allow_action: bool = False) -> bool:
        sql = QueryBuilder.build_droptable_query(table, cascade, allow_action)
        try:
            self._run_execute(sql)
            return True
        except Error as e:
            logger.error(f"DROP TABLE '{table}' failed: {e}")
            return False

    def truncate(self, table: str) -> bool:
        sql = QueryBuilder.build_truncate_query(table)
        try:
            self._run_execute(sql)
            return True
        except Error as e:
            logger.error(f"TRUNCATE '{table}' failed: {e}")
            return False

    def table_exists(self, table: str) -> Optional[bool]:
        sql = QueryBuilder.build_findtable_query()
        try:
            result = self._run_query(sql, [table])
            return len(result) > 0
        except Error as e:
            logger.error(f"table_exists() check failed for '{table}': {e}")
            return None

    def create_index(self, table: str, columns: List[str], unique: bool = False) -> int:
        sql = QueryBuilder.build_create_index(table, columns, unique)
        return self._run_execute(sql)

    def vacuum(self, table: str = None, analyze: bool = True) -> int:
        sql = QueryBuilder.build_vacuum(table, analyze)
        return self._run_execute(sql)

    # -- utility reads --

    def get_table_columns(self, table: str) -> Optional[List[str]]:
        sql = QueryBuilder.build_allcolumns_query()
        try:
            result = self._run_query(sql, [table])
            return [row['column_name'] for row in result]
        except Error as e:
            logger.error(f"get_table_columns() failed for '{table}': {e}")
            return None

    def get_by_id(self, table: str, id_name: str, id: Union[str, int]) -> Optional[List[Dict]]:
        sql = QueryBuilder.build_getby_id(table, id_name)
        try:
            return self._run_query(sql, [id])
        except Error as e:
            logger.error(f"get_by_id() failed on '{table}': {e}")
            return None

    def count(self, table: str, where: Optional[Dict[str, Any]] = None) -> Optional[int]:
        sql, params = QueryBuilder.build_count_query(table, where)
        try:
            # COUNT returns a single value, not a dict — handle it directly
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    result = cur.fetchone()
                    return result[0] if result else None
        except Error as e:
            logger.error(f"count() failed on '{table}': {e}")
            return None

    def exists(self, table: str, where: Dict[str, Any]) -> Optional[bool]:
        sql, params = QueryBuilder.build_exists_query(table, where)
        try:
            result = self._run_query(sql, params)
            return bool(result) and result[0].get("exists", False)
        except Error as e:
            logger.error(f"exists() failed on '{table}': {e}")
            return None

    # -- health & stats --

    def is_healthy(self) -> bool:
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    return True
        except Exception:
            return False

    def stats(self) -> Dict:
        if not self.pool:
            return {}
        pool_stats = self.pool.get_stats()
        return {
            "size": self.pool.size,
            "available": pool_stats.get("available", 0),
            "used": pool_stats.get("used", 0)
        }

    def close(self) -> None:
        if self.pool:
            self.pool.close()

    # -- json helpers --

    @staticmethod
    def export_to_json(file: str, data: dict, indent: int = 4) -> bool:
        try:
            with open(file, "w") as f:
                json.dump(data, f, indent=indent)
            return True
        except Exception as e:
            logger.error(f"export_to_json() failed: {e}")
            return False

    @staticmethod
    def import_from_json(file: str) -> Union[dict, bool]:
        try:
            with open(file, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"import_from_json() failed: {e}")
            return False
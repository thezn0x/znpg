from typing import Any, Optional, Dict, List, Union
from exceptions import IncompleteArgumentsError
from .exceptions import AuthorizationError

class QueryBuilder:
    @staticmethod
    def build_select_query(table: str, columns: Optional[List[str]],where: Optional[Dict[str,Any]],order_by: Optional[str | List[str]] = None, limit: Optional[int]= None):
        params: List[Any] = []

        columns_list="*"
        if columns:
            columns_list = ", ".join(f'"{c}"' for c in columns)

        where_clause = ""
        if where:
            where_conditions=[]
            for key,value in where.items():
                where_conditions.append(f'"{key}" = %s')
                params.append(value)
            where_sql_conditions = " AND ".join(where_conditions)
            where_clause = "WHERE "+where_sql_conditions
        
        order_by_clause = ""
        if order_by:

            if isinstance(order_by, str):
                order_by = [order_by]
            order_by_clause = f"ORDER BY {', '.join(order_by)}"

        limit_clause = f"LIMIT {limit}" if limit is not None else ""

        sql_pt1 = f"SELECT {columns_list} FROM \"{table}\""
        sql_pt2 = f"{where_clause} {order_by_clause} {limit_clause}"

        sql = f"{sql_pt1} {sql_pt2}".strip()

        final_sql = " ".join(sql.split())

        return final_sql, params
    
    @staticmethod
    def build_insert_query(table: str, data: Dict[str,Any]) -> tuple[str, List[Any]]:
        columns = []
        params = []
        for keys, values in data.items():
            columns.append(f'"{keys}"')
            params.append(values)
        keys_pt = ", ".join(column for column in columns)
        values_pt = ", ".join(["%s"] * len(params))  
        full_insert_query = f'INSERT INTO "{table}"({keys_pt}) VALUES({values_pt})'
        return full_insert_query, params

    @staticmethod
    def build_update_query(table: str, data: Dict[str,Any], conditions: Optional[Dict[str,Any]] = None) -> tuple[str, List[Any]]:
        columns = []
        params = []
        for keys,values in data.items():
            columns.append(f'"{keys}" = %s')
            params.append(values)
        keys_pt = ", ".join(column for column in columns)

        if conditions:  
            condition_columns = []
            condition_params = []
            for keys,values in conditions.items():
                condition_columns.append(f'"{keys}" = %s')
                condition_params.append(values)
            condition_keys_pt = " AND ".join(condition_column for condition_column in condition_columns)
            full_update_query = f'UPDATE "{table}" SET {keys_pt} WHERE {condition_keys_pt}'
            all_params = params + condition_params
            return full_update_query,all_params
        else:
            full_update_query = f'UPDATE "{table}" SET {keys_pt}'
            return full_update_query, params

    @staticmethod
    def build_delete_query(table: str, conditions: Optional[Dict[str,Any]] = None) -> tuple[str, List[Any]]:
        columns = []
        params = []

        if conditions:  
            for keys,values in conditions.items():
                columns.append(f'"{keys}" = %s')
                params.append(values)
            keys_pt = " AND ".join(column for column in columns)
            full_delete_query = f'DELETE FROM "{table}" WHERE {keys_pt}'
            return full_delete_query, params        
        else:
            full_delete_query = f'DELETE FROM "{table}"'
            return full_delete_query, []
    
    @staticmethod
    def build_createtable_query(table: str, columns: Dict[str,str] = None)-> str:
        if not columns:
            raise IncompleteArgumentsError("Columns not defined")
        columns_definition = []
        for keys,values in columns.items():
            columns_definition.append(f'{keys} {values}')
        definition = ", ".join(columns_definition)
        query = f'CREATE TABLE IF NOT EXISTS "{table}" ({definition})'
        return query

    @staticmethod
    def build_droptable_query(table: str, cascade: Optional[bool] = False,allow_action: Optional[bool] = False) -> str:
        if not allow_action:
            raise AuthorizationError("Set allow_action to True to perform Operation")
        elif cascade and allow_action:
            query = f'DROP TABLE IF EXISTS "{table}" CASCADE'
            return query
        else:
            query = f'DROP TABLE IF EXISTS "{table}"'
            return query    
    
    @staticmethod
    def build_findtable_query() -> str:
        sql = 'SELECT table_name FROM information_schema.tables WHERE table_schema = \'public\' AND table_name = %s'
        return sql

    @staticmethod
    def build_truncate_query(table: str) -> str:
        sql = f'TRUNCATE TABLE "{table}"'
        return sql
    
    @staticmethod
    def build_allcolumns_query() -> str:
        sql = 'SELECT column_name FROM information_schema.columns WHERE table_name = %s'
        return sql

    @staticmethod
    def build_bulk_insert(table: str,data: List[Dict[str,Any]]) -> tuple[str,List[Any]]:
        keys = list(data[0].keys())
        keys_pt = ", ".join(f'"{k}"' for k in keys)

        placeholders = ", ".join(["%s"] * len(keys))
        placeholders_group = ", ".join([f"({placeholders})"] * len(data))

        values_list = []
        for item in data:
            for key in keys:
                values_list.append(item[key])

        sql = f'INSERT INTO "{table}" ({keys_pt}) VALUES {placeholders_group}'
        return sql,values_list

    @staticmethod
    def build_getby_id(table: str, id_column: str) -> str:
        sql = f'SELECT * FROM "{table}" WHERE "{id_column}" = %s'
        return sql

    @staticmethod
    def build_count_query(table: str, where: Optional[Dict[str,Any]] = None) -> Union[str,List[Any]]:
        if where:
            keys = []
            params = []
            for key,value in where.items():
                keys.append(key)
                params.append(value)
            keys_pt = " AND ".join(f'"{key}" = %s' for key in keys)
            where_sql = f"SELECT COUNT(*) FROM \"{table}\" WHERE {keys_pt}"
            return where_sql,params
        else:
            return f"SELECT COUNT(*) FROM \"{table}\"",[]

    @staticmethod
    def build_exists_query(table: str, where: Dict[str,Any]) -> Union[str,List[Any]]:
        keys = []
        params = []
        for key,value in where.items():
            keys.append(key)
            params.append(value)
        keys_pt = " AND ".join(f'"{key}" = %s' for key in keys)
        where_sql = f"SELECT EXISTS(SELECT 1 FROM \"{table}\" WHERE {keys_pt})"
        return where_sql,params

from typing import Any, Optional, Dict, List, Tuple, Union
import re

class QueryBuilder:
    # Prevent SQL injection in identifiers
    _SQL_KEYWORDS = {
        'select', 'insert', 'update', 'delete', 'drop', 'create',
        'table', 'where', 'from', 'into', 'values', 'set', 'and', 'or',
        'limit', 'offset', 'order', 'by', 'group', 'having', 'join'
    }
    
    @staticmethod
    def _sanitize_identifier(name: str) -> str:
        """Safely quote SQL identifiers"""
        if not name or not isinstance(name, str):
            raise ValueError(f"Invalid identifier: {name}")
        
        # Check for SQL keywords
        if name.lower() in QueryBuilder._SQL_KEYWORDS:
            raise ValueError(f"Cannot use SQL keyword as identifier: {name}")
        
        # Allow only alphanumeric and underscores
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
            raise ValueError(f"Invalid identifier format: {name}")
        
        return f'"{name}"'
    
    @staticmethod
    def _parse_where_value(value: Any) -> Tuple[str, List[Any]]:
        """Parse WHERE clause values with operators"""
        if isinstance(value, list) and len(value) == 2:
            operator, operand = value
            operator = operator.upper()
            
            if operator in ('IN', 'NOT IN'):
                if not isinstance(operand, (list, tuple)):
                    raise ValueError(f"{operator} requires a list/tuple")
                placeholders = ', '.join(['%s'] * len(operand))
                return f"{operator} ({placeholders})", list(operand)
            
            elif operator in ('=', '!=', '<', '>', '<=', '>=', 'LIKE', 'ILIKE'):
                return f"{operator} %s", [operand]
            
            elif operator == 'BETWEEN':
                if not isinstance(operand, (list, tuple)) or len(operand) != 2:
                    raise ValueError("BETWEEN requires two values")
                return "BETWEEN %s AND %s", list(operand)
            
            elif operator in ('IS NULL', 'IS NOT NULL'):
                return operator, []
            
            else:
                raise ValueError(f"Unsupported operator: {operator}")
        
        # Default to equality
        return "= %s", [value]
    
    @staticmethod
    def build_select_query(
        table: str,
        columns: Optional[List[str]] = None,
        where: Optional[Dict[str, Any]] = None,
        order_by: Optional[Union[str, List[str]]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> Tuple[str, List[Any]]:
        """Build SELECT query with safe parameterization"""
        params: List[Any] = []
        
        # Sanitize table name
        safe_table = QueryBuilder._sanitize_identifier(table)
        
        # Build columns
        if columns:
            safe_columns = [QueryBuilder._sanitize_identifier(col) for col in columns]
            columns_clause = ", ".join(safe_columns)
        else:
            columns_clause = "*"
        
        # Build WHERE clause
        where_clauses = []
        if where:
            for key, value in where.items():
                safe_key = QueryBuilder._sanitize_identifier(key)
                operator, value_params = QueryBuilder._parse_where_value(value)
                
                where_clauses.append(f"{safe_key} {operator}")
                params.extend(value_params)
        
        where_clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        
        # Build ORDER BY
        order_clause = ""
        if order_by:
            if isinstance(order_by, str):
                order_by = [order_by]
            
            safe_orders = []
            for item in order_by:
                # Handle "column DESC" format
                if ' ' in item:
                    col, direction = item.split(' ', 1)
                    safe_col = QueryBuilder._sanitize_identifier(col)
                    safe_orders.append(f"{safe_col} {direction.upper()}")
                else:
                    safe_orders.append(QueryBuilder._sanitize_identifier(item))
            
            order_clause = f"ORDER BY {', '.join(safe_orders)}"
        
        # Build LIMIT and OFFSET
        limit_clause = f"LIMIT {limit}" if limit is not None else ""
        offset_clause = f"OFFSET {offset}" if offset is not None else ""
        
        # Combine everything
        parts = [
            f"SELECT {columns_clause} FROM {safe_table}",
            where_clause,
            order_clause,
            limit_clause,
            offset_clause
        ]
        
        sql = " ".join(filter(None, parts))
        return sql.strip(), params
    
    @staticmethod
    def build_insert_query(table: str, data: Dict[str, Any]) -> Tuple[str, List[Any]]:
        """Build INSERT query with safe parameterization"""
        if not data:
            raise ValueError("No data provided for INSERT")
        
        safe_table = QueryBuilder._sanitize_identifier(table)
        columns = []
        params = []
        
        for key, value in data.items():
            columns.append(QueryBuilder._sanitize_identifier(key))
            params.append(value)
        
        columns_clause = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(params))
        
        sql = f"INSERT INTO {safe_table} ({columns_clause}) VALUES ({placeholders})"
        return sql, params
    
    @staticmethod
    def build_update_query(
        table: str,
        data: Dict[str, Any],
        conditions: Optional[Dict[str, Any]] = None
    ) -> Tuple[str, List[Any]]:
        """Build UPDATE query with required WHERE clause"""
        if not conditions:
            raise ValueError("UPDATE requires WHERE conditions for safety")
        
        if not data:
            raise ValueError("No data provided for UPDATE")
        
        safe_table = QueryBuilder._sanitize_identifier(table)
        params = []
        
        # Build SET clause
        set_items = []
        for key, value in data.items():
            safe_key = QueryBuilder._sanitize_identifier(key)
            set_items.append(f"{safe_key} = %s")
            params.append(value)
        
        set_clause = ", ".join(set_items)
        
        # Build WHERE clause
        where_items = []
        for key, value in conditions.items():
            safe_key = QueryBuilder._sanitize_identifier(key)
            operator, value_params = QueryBuilder._parse_where_value(value)
            
            where_items.append(f"{safe_key} {operator}")
            params.extend(value_params)
        
        where_clause = f"WHERE {' AND '.join(where_items)}"
        
        sql = f"UPDATE {safe_table} SET {set_clause} {where_clause}"
        return sql.strip(), params
    
    @staticmethod
    def build_delete_query(
        table: str,
        conditions: Optional[Dict[str, Any]] = None
    ) -> Tuple[str, List[Any]]:
        """Build DELETE query with required WHERE clause"""
        if not conditions:
            raise ValueError("DELETE requires WHERE conditions for safety")
        
        safe_table = QueryBuilder._sanitize_identifier(table)
        params = []
        
        # Build WHERE clause
        where_items = []
        for key, value in conditions.items():
            safe_key = QueryBuilder._sanitize_identifier(key)
            operator, value_params = QueryBuilder._parse_where_value(value)
            
            where_items.append(f"{safe_key} {operator}")
            params.extend(value_params)
        
        where_clause = f"WHERE {' AND '.join(where_items)}"
        
        sql = f"DELETE FROM {safe_table} {where_clause}"
        return sql.strip(), params
    
    @staticmethod
    def build_createtable_query(
        table: str,
        columns: Dict[str, str]
    ) -> str:
        """Build CREATE TABLE query"""
        if not columns:
            raise ValueError("Columns definition required")
        
        safe_table = QueryBuilder._sanitize_identifier(table)
        
        column_defs = []
        for name, definition in columns.items():
            safe_name = QueryBuilder._sanitize_identifier(name)
            # Note: definition is NOT sanitized - it's SQL syntax
            column_defs.append(f"{safe_name} {definition}")
        
        columns_clause = ", ".join(column_defs)
        sql = f"CREATE TABLE IF NOT EXISTS {safe_table} ({columns_clause})"
        return sql
    
    @staticmethod
    def build_droptable_query(
        table: str,
        cascade: bool = False,
        require_safety_flag: bool = False
    ) -> str:
        """Build DROP TABLE query with safety check"""
        if require_safety_flag:
            raise ValueError("Safety flag required. Call with require_safety_flag=False to confirm.")
        
        safe_table = QueryBuilder._sanitize_identifier(table)
        cascade_clause = " CASCADE" if cascade else ""
        
        sql = f"DROP TABLE IF EXISTS {safe_table}{cascade_clause}"
        return sql
    
    @staticmethod
    def build_bulk_insert(
        table: str,
        data: List[Dict[str, Any]],
        on_conflict: Optional[str] = None
    ) -> Tuple[str, List[Any]]:
        """Build bulk INSERT query"""
        if not data:
            return "", []
        
        # Validate all dicts have same keys
        first_keys = set(data[0].keys())
        for i, item in enumerate(data[1:], 1):
            if set(item.keys()) != first_keys:
                raise ValueError(f"Item {i} has different keys: {set(item.keys())} != {first_keys}")
        
        safe_table = QueryBuilder._sanitize_identifier(table)
        
        # Sanitize column names
        columns = [QueryBuilder._sanitize_identifier(k) for k in first_keys]
        columns_clause = ", ".join(columns)
        
        # Build placeholders
        row_placeholder = "(" + ", ".join(["%s"] * len(columns)) + ")"
        all_placeholders = ", ".join([row_placeholder] * len(data))
        
        # Flatten values
        params = []
        for item in data:
            for key in first_keys:
                params.append(item[key])
        
        # Build SQL
        sql = f"INSERT INTO {safe_table} ({columns_clause}) VALUES {all_placeholders}"
        
        if on_conflict:
            sql += f" ON CONFLICT {on_conflict}"
        
        return sql, params
    
    @staticmethod
    def build_count_query(
        table: str,
        where: Optional[Dict[str, Any]] = None
    ) -> Tuple[str, List[Any]]:
        """Build COUNT query"""
        return QueryBuilder.build_select_query(
            table=table,
            columns=["COUNT(*)"],
            where=where
        )
    
    @staticmethod
    def build_exists_query(
        table: str,
        where: Dict[str, Any]
    ) -> Tuple[str, List[Any]]:
        """Build EXISTS query"""
        # Build inner SELECT
        inner_sql, params = QueryBuilder.build_select_query(
            table=table,
            columns=["1"],
            where=where,
            limit=1
        )
        
        sql = f"SELECT EXISTS({inner_sql})"
        return sql, params
    
    # Other methods remain similar but should use _sanitize_identifier
    
    @staticmethod
    def build_create_index(
        table: str,
        columns: List[str],
        unique: bool = False
    ) -> str:
        """Build CREATE INDEX query"""
        safe_table = QueryBuilder._sanitize_identifier(table)
        safe_columns = [QueryBuilder._sanitize_identifier(col) for col in columns]
        
        # Generate index name (PostgreSQL will create if not specified)
        unique_str = "UNIQUE " if unique else ""
        columns_str = ", ".join(safe_columns)
        
        sql = f"CREATE {unique_str}INDEX ON {safe_table} ({columns_str})"
        return sql
    
    @staticmethod
    def build_vacuum(
        table: Optional[str] = None,
        analyze: bool = True
    ) -> str:
        """Build VACUUM query"""
        if table:
            safe_table = QueryBuilder._sanitize_identifier(table)
            analyze_str = " ANALYZE" if analyze else ""
            sql = f"VACUUM{analyze_str} {safe_table}"
        else:
            analyze_str = " ANALYZE" if analyze else ""
            sql = f"VACUUM{analyze_str}"
        return sql
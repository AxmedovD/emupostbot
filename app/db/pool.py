from typing import Optional, Dict, Any, List, Union
from contextlib import asynccontextmanager

from asyncpg import create_pool, Pool, Record, Connection
from orjson import loads, dumps

from app.core.config import settings
from app.core.logger import db_logger

SAFE_OPERATORS: Dict[str, str] = {
    "=": "=",
    ">=": ">=",
    "<=": "<=",
    "!=": "!=",
    ">": ">",
    "<": "<",
    "LIKE": "LIKE",
    "ILIKE": "ILIKE",
    "IN": "IN",
    "NOT IN": "NOT IN",
    "IS NULL": "IS NULL",
    "IS NOT NULL": "IS NOT NULL",
}

ALLOWED_TABLES: set = {
    "users",
    "clients",
    "orders",
    "payments",
    "notifications",
    "webhook_logs"
}

ALLOWED_ORDER_COLUMNS: Dict[str, set] = {
    "users": {"pk", "user_id", "create_at"},
    "clients": {"pk", "person_name", "person_id", "ball", "status", "create_at"},
    "orders": {"pk", "deal_id", "person_id", "total_amount", "status", "create_at"},
    "payments": {"pk", "amount", "person_id", "cashin_id", "create_at"},
}


class DatabaseSecurityError(Exception):
    pass


class Database:
    def __init__(
            self,
            user: str = None,
            password: str = None,
            host: str = None,
            database: str = None,
            port: int = None,
            min_size: int = 5,
            max_size: int = 20,
            command_timeout: float = 60.0,
            max_queries: int = 50000,
            max_inactive_connection_lifetime: float = 300.0,
    ):
        """
        Initialize database manager

        Args:
            user: Database user
            password: Database password
            host: Database host
            database: Database name
            port: Database port
            min_size: Minimum pool size
            max_size: Maximum pool size
            command_timeout: Query timeout in seconds
            max_queries: Max queries per connection before recycling
            max_inactive_connection_lifetime: Max inactive time in seconds
        """

        self.user = user or settings.DB_USER
        self.password = password or settings.DB_PASSWORD
        self.host = host or settings.DB_HOST
        self.database = database or settings.DB_NAME
        self.port = port or settings.DB_PORT

        self.min_size = min_size
        self.max_size = max_size
        self.command_timeout = command_timeout
        self.max_queries = max_queries
        self.max_inactive_connection_lifetime = max_inactive_connection_lifetime

        self.pool: Optional[Pool] = None

    @staticmethod
    async def _init_connection(conn: Connection):
        """Initialize connection with custom type codecs"""
        try:
            await conn.set_type_codec(
                typename='jsonb',
                encoder=lambda x: dumps(x).decode(),
                decoder=loads,
                schema='pg_catalog'
            )
        except Exception as e:
            db_logger.error(f"Failed to set type codec: {e}")
            raise

    @staticmethod
    def _validate_table_name(table: str) -> str:
        """
        Validate table name against whitelist

        Args:
            table: Table name to validate

        Returns:
            Validated table name

        Raises:
            DatabaseSecurityError: If table name is not allowed
        """
        if table not in ALLOWED_TABLES:
            db_logger.error(f"Security violation: Invalid table name '{table}'")
            raise DatabaseSecurityError(f"Table '{table}' is not allowed")
        return table

    @staticmethod
    def _validate_column_name(column: str) -> str:
        """
        Validate column name (prevent SQL injection via column names)

        Args:
            column: Column name to validate

        Returns:
            Validated column name

        Raises:
            DatabaseSecurityError: If column name contains suspicious characters
        """

        if not all(c.isalnum() or c in ('_', '.') for c in column):
            db_logger.error(f"Security violation: Invalid column name '{column}'")
            raise DatabaseSecurityError(f"Column name '{column}' contains invalid characters")
        return column

    @staticmethod
    def _validate_order_by(table: str, order_by: str) -> str:
        """
        Validate ORDER BY clause

        Args:
            table: Table name
            order_by: ORDER BY clause

        Returns:
            Validated ORDER BY clause

        Raises:
            DatabaseSecurityError: If ORDER BY is invalid
        """

        parts = order_by.split()
        column = parts[0].strip()

        if table in ALLOWED_ORDER_COLUMNS:
            if column not in ALLOWED_ORDER_COLUMNS[table]:
                db_logger.error(f"Security violation: ORDER BY column '{column}' not allowed for table '{table}'")
                raise DatabaseSecurityError(f"ORDER BY column '{column}' not allowed for table '{table}'")

        if len(parts) > 1:
            direction = parts[1].upper()
            if direction not in ("ASC", "DESC"):
                raise DatabaseSecurityError(f"Invalid ORDER BY direction: {direction}")

        return order_by

    async def create_session_pool(self):
        """Create connection pool with optimized settings"""
        try:
            self.pool = await create_pool(  # type: ignore[misc]
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                port=self.port,
                min_size=self.min_size,
                max_size=self.max_size,
                command_timeout=self.command_timeout,
                max_queries=self.max_queries,
                max_inactive_connection_lifetime=self.max_inactive_connection_lifetime,
                init=self._init_connection,
                server_settings={
                    'jit': 'off',
                    'application_name': 'emupostbot_advanced',
                    'statement_timeout': str(int(self.command_timeout * 1000)),
                }
            )
            db_logger.info(
                f"Database pool created: "
                f"min={self.min_size}, max={self.max_size}, "
                f"timeout={self.command_timeout}s"
            )
        except Exception as e:
            db_logger.error(f"Failed to create database pool: {e}")
            raise

    async def close(self) -> None:
        """Close database pool gracefully"""
        if self.pool:
            try:
                await self.pool.close()
                db_logger.info("Database pool closed successfully")
            except Exception as e:
                db_logger.error(f"Error closing database pool: {e}")

    @asynccontextmanager
    async def acquire(self):
        """
        Acquire connection from pool with automatic release

        Usage:
            async with db.acquire() as conn:
                await conn.execute(query)
        """
        if not self.pool:
            raise RuntimeError("Database pool not initialized. Call create_session_pool() first.")

        conn = await self.pool.acquire()  # type: ignore[misc]
        try:
            yield conn
        finally:
            await self.pool.release(conn)

    @asynccontextmanager
    async def transaction(self):
        """
        Transaction context manager

        Usage:
            async with db.transaction() as conn:
                await conn.execute(query1)
                await conn.execute(query2)
        """
        async with self.acquire() as conn:
            async with conn.transaction():
                yield conn

    def _build_where_clause(
            self,
            conditions: Dict[str, Any],
            use_or: bool = False,
            start_index: int = 1
    ) -> tuple[str, list]:
        """
        Build WHERE clause with security validation

        Args:
            conditions: Conditions dictionary
            use_or: Use OR instead of AND
            start_index: Starting parameter index

        Returns:
            Tuple of (where_clause, parameters)
        """
        if not conditions:
            return "TRUE", []

        where_parts: List[str] = []
        params: List[Any] = []
        current_index = start_index

        for key, value in conditions.items():
            self._validate_column_name(key)

            if value is None:
                where_parts.append(f"{key} IS NULL")
                continue

            if isinstance(value, (list, tuple)):
                if not value:
                    where_parts.append("FALSE")
                    continue
                placeholders = [f"${i}" for i in range(current_index, current_index + len(value))]
                where_parts.append(f"{key} IN ({','.join(placeholders)})")
                params.extend(value)
                current_index += len(value)
                continue

            if isinstance(value, dict) and "OR" in value:
                or_values = value["OR"]
                if not or_values:
                    continue
                or_parts = []
                for val in or_values:
                    or_parts.append(f"{key} = ${current_index}")
                    params.append(val)
                    current_index += 1
                where_parts.append(f"({' OR '.join(or_parts)})")
                continue

            operator_found = False
            for op, sql_op in SAFE_OPERATORS.items():
                if key.endswith(op):
                    clean_key = key[:-len(op)].strip()
                    self._validate_column_name(clean_key)
                    where_parts.append(f"{clean_key} {sql_op} ${current_index}")
                    params.append(value)
                    current_index += 1
                    operator_found = True
                    break

            if not operator_found:
                where_parts.append(f"{key} = ${current_index}")
                params.append(value)
                current_index += 1

        join_operator = ' OR ' if use_or else ' AND '
        where_clause = join_operator.join(where_parts) if where_parts else "TRUE"

        return where_clause, params

    def _build_query(
            self,
            table: str,
            conditions: Optional[Dict[str, Any]] = None,
            fields: Optional[List[str]] = None,
            order_by: Optional[str] = None,
            limit: Optional[int] = None,
            offset: Optional[int] = None,
            use_or: bool = False,
            start_index: int = 1,
    ) -> tuple[str, list]:
        """
        Build SELECT query with security validation

        Args:
            table: Table name
            conditions: WHERE conditions
            fields: Column names to select
            order_by: ORDER BY clause
            limit: LIMIT value
            offset: OFFSET value
            use_or: Use OR in WHERE
            start_index: Starting parameter index

        Returns:
            Tuple of (query, parameters)
        """
        table = self._validate_table_name(table)

        if fields:
            fields = [self._validate_column_name(f) for f in fields]
            fields_str = ', '.join(fields)
        else:
            fields_str = '*'

        query = f"SELECT {fields_str} FROM {table}"
        params = []

        if conditions:
            where_clause, where_params = self._build_where_clause(
                conditions, use_or=use_or, start_index=start_index
            )
            query += f" WHERE {where_clause}"
            params.extend(where_params)

        if order_by:
            order_by = self._validate_order_by(table, order_by)
            query += f" ORDER BY {order_by}"

        if limit is not None:
            if limit < 0:
                raise DatabaseSecurityError("LIMIT must be non-negative")
            query += f" LIMIT ${len(params) + 1}"
            params.append(limit)

        if offset is not None:
            if offset < 0:
                raise DatabaseSecurityError("OFFSET must be non-negative")
            query += f" OFFSET ${len(params) + 1}"
            params.append(offset)

        return query, params

    async def create(
            self,
            table: str,
            data: Dict[str, Any],
            returning: str = "pk"
    ) -> Optional[Any]:
        """
        Insert record into table

        Args:
            table: Table name
            data: Data dictionary
            returning: Column to return (default: pk)

        Returns:
            Value of returning column or None on error
        """
        try:
            table = self._validate_table_name(table)
            for col in data.keys():
                self._validate_column_name(col)

            if returning:
                self._validate_column_name(returning)

            columns = ', '.join(data.keys())
            placeholders = ', '.join(f'${i}' for i in range(1, len(data) + 1))

            query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
            if returning:
                query += f" RETURNING {returning}"

            db_logger.debug(f"CREATE query: {query}")

            async with self.transaction() as conn:
                if returning:
                    result = await conn.fetchval(query, *data.values())
                else:
                    result = await conn.execute(query, *data.values())

            db_logger.info(f"Created record in {table}: {returning}={result}")
            return result

        except DatabaseSecurityError:
            raise
        except Exception as e:
            db_logger.error(f"Error creating record in {table}: {e}")
            return None

    async def read(
            self,
            table: str,
            conditions: Optional[Dict[str, Any]] = None,
            fields: Optional[List[str]] = None,
            order_by: Optional[str] = None,
            limit: Optional[int] = None,
            offset: Optional[int] = None,
            result: Optional[int] = None,
            use_or: bool = False,
    ) -> Union[None, Record, List[Record], Any]:
        """
        Read records from table

        Args:
            table: Table name
            conditions: WHERE conditions
            fields: Columns to select
            order_by: ORDER BY clause
            limit: LIMIT value
            offset: OFFSET value
            result: 0=fetchval, 1=fetchrow, None=fetch
            use_or: Use OR in WHERE

        Returns:
            Query result or None on error
        """
        try:
            query, params = self._build_query(
                table, conditions, fields, order_by, limit, offset, use_or=use_or
            )

            db_logger.debug(f"READ query: {query} | params: {params}")

            if result == 1:
                return await self.pool.fetchrow(query, *params)
            elif result == 0:
                return await self.pool.fetchval(query, *params)
            else:
                return await self.pool.fetch(query, *params)

        except DatabaseSecurityError:
            raise
        except Exception as e:
            db_logger.error(f"Error reading from {table}: {e}")
            return None

    async def update(
            self,
            table: str,
            data: Dict[str, Any],
            conditions: Dict[str, Any]
    ) -> Optional[str]:
        """
        Update records in table

        Args:
            table: Table name
            data: Data to update
            conditions: WHERE conditions

        Returns:
            Result status or None on error
        """
        try:
            table = self._validate_table_name(table)
            for col in data.keys():
                self._validate_column_name(col)
            for col in conditions.keys():
                self._validate_column_name(col)

            set_clause = ', '.join(
                f"{k} = ${i}" for i, k in enumerate(data.keys(), start=1)
            )

            where_clause, where_params = self._build_where_clause(
                conditions, start_index=len(data) + 1
            )

            query = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
            params = list(data.values()) + where_params

            db_logger.debug(f"UPDATE query: {query} | params: {params}")

            async with self.transaction() as conn:
                result = await conn.execute(query, *params)

            db_logger.info(f"Updated records in {table}: {result}")
            return result

        except DatabaseSecurityError:
            raise
        except Exception as e:
            db_logger.error(f"Error updating {table}: {e}")
            return None

    async def delete(
            self,
            table: str,
            conditions: Dict[str, Any]
    ) -> Optional[str]:
        """
        Delete records from table

        Args:
            table: Table name
            conditions: WHERE conditions

        Returns:
            Result status or None on error
        """
        try:
            table = self._validate_table_name(table)
            for col in conditions.keys():
                self._validate_column_name(col)

            where_clause, params = self._build_where_clause(conditions)

            query = f"DELETE FROM {table} WHERE {where_clause}"

            db_logger.debug(f"DELETE query: {query} | params: {params}")

            async with self.transaction() as conn:
                result = await conn.execute(query, *params)

            db_logger.warning(f"Deleted records from {table}: {result}")
            return result

        except DatabaseSecurityError:
            raise
        except Exception as e:
            db_logger.error(f"Error deleting from {table}: {e}")
            return None

    async def count(
            self,
            table: str,
            conditions: Optional[Dict[str, Any]] = None
    ) -> Optional[int]:
        """
        Count records in table

        Args:
            table: Table name
            conditions: WHERE conditions

        Returns:
            Count or None on error
        """
        try:
            table = self._validate_table_name(table)

            query = f"SELECT COUNT(*) FROM {table}"
            params = []

            if conditions:
                for col in conditions.keys():
                    self._validate_column_name(col)
                where_clause, params = self._build_where_clause(conditions)
                query += f" WHERE {where_clause}"

            db_logger.debug(f"COUNT query: {query} | params: {params}")

            result = await self.pool.fetchval(query, *params)
            return result

        except DatabaseSecurityError:
            raise
        except Exception as e:
            db_logger.error(f"Error counting {table}: {e}")
            return None

    async def execute_raw(
            self,
            query: str,
            *params,
            timeout: Optional[float] = None
    ) -> Any:
        """
        Execute raw SQL query (use with caution!)

        Args:
            query: SQL query
            *params: Query parameters
            timeout: Query timeout

        Returns:
            Query result

        Warning:
            Only use this for complex queries that cannot be built with safe methods.
            Always use parameterized queries, never string interpolation!
        """
        db_logger.warning(f"Executing raw query: {query[:100]}...")

        try:
            async with self.acquire() as conn:
                return await conn.execute(query, *params, timeout=timeout)
        except Exception as e:
            db_logger.error(f"Error executing raw query: {e}")
            raise

    async def create_tables(self) -> None:
        """Create database tables (if not exist)"""
        tables = {}

        try:
            async with self.transaction() as conn:
                for table_name, query in tables.items():
                    await conn.execute(query)
                    db_logger.info(f"Table '{table_name}' created/verified")
        except Exception as e:
            db_logger.error(f"Error creating tables: {e}")
            raise


db = Database()

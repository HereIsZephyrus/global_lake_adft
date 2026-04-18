"""DuckDB 文件数据客户端 - 支持 Parquet 文件查询"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

log = logging.getLogger(__name__)


class DuckDBClient:
    """DuckDB 客户端，用于查询 Parquet 文件

    这个客户端替代 PostgreSQL 客户端，用于远端无数据库环境的计算。
    """

    def __init__(self, data_dir: Path | None = None) -> None:
        """初始化 DuckDB 客户端

        Args:
            data_dir: Parquet 文件所在目录。如果为 None，创建内存数据库。
        """
        self.data_dir = Path(data_dir) if data_dir else None
        self.con = duckdb.connect(database=":memory:")

        if self.data_dir:
            log.info(f"初始化 DuckDB 客户端，数据目录: {self.data_dir}")
            self.register_dir()

    def query(self, sql: str, parameters: dict[str, Any] | None = None) -> list[dict]:
        """执行 SQL 查询，返回字典列表

        Args:
            sql: SQL 查询语句
            parameters: 可选的查询参数

        Returns:
            查询结果列表，每个元素是一个字典
        """
        result = self.con.execute(sql, parameters=parameters).fetchdf()
        return result.to_dict(orient="records")

    def query_df(self, sql: str, parameters: dict[str, Any] | None = None) -> pd.DataFrame:
        """执行 SQL 查询，返回 DataFrame

        Args:
            sql: SQL 查询语句
            parameters: 可选的查询参数

        Returns:
            查询结果 DataFrame
        """
        return self.con.execute(sql, parameters=parameters).fetchdf()

    def register_parquet(self, name: str, path: Path) -> None:
        """注册 Parquet 文件为 DuckDB 视图

        注册后可以通过视图名直接查询文件内容。

        Args:
            name: 视图名（用于 SQL 查询）
            path: Parquet 文件路径
        """
        self.con.execute(f"CREATE VIEW {name} AS SELECT * FROM '{path}'")
        log.debug(f"注册视图 {name}: {path}")

    def register_dir(self) -> None:
        """注册数据目录中的所有 Parquet 文件

        自动扫描 data_dir 中的所有 .parquet 文件，
        并以文件名（不含扩展名）作为视图名注册。

        Raises:
            ValueError: 如果 data_dir 未设置
        """
        if not self.data_dir:
            raise ValueError("data_dir 未设置，无法注册目录")

        for parquet_file in sorted(self.data_dir.glob("*.parquet")):
            table_name = parquet_file.stem
            try:
                self.register_parquet(table_name, parquet_file)
                log.info(f"注册表 {table_name}: {parquet_file.name}")
            except Exception as e:
                log.warning(f"注册 {parquet_file} 失败: {e}")

    def list_registered_tables(self) -> list[str]:
        """列出所有已注册的视图名"""
        result = self.con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_type = 'VIEW'"
        )
        return [row[0] for row in result.fetchall()]

    def close(self) -> None:
        """关闭 DuckDB 连接"""
        self.con.close()

    def __enter__(self) -> "DuckDBClient":
        """上下文管理器入口"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """上下文管理器退出，自动关闭连接"""
        self.close()


def create_client(data_dir: str | Path | None = None) -> DuckDBClient:
    """创建 DuckDB 客户端的便捷函数

    Args:
        data_dir: Parquet 文件目录

    Returns:
        DuckDBClient 实例
    """
    return DuckDBClient(data_dir=Path(data_dir) if data_dir else None)

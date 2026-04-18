"""Tests for DuckDBClient"""

import os
from pathlib import Path

import pytest


class TestDuckDBClient:
    """Tests for DuckDBClient"""

    def test_import(self):
        """DuckDBClient 可以导入"""
        from lakeanalysis.dbconnect import DuckDBClient

        assert DuckDBClient is not None

    def test_create_client(self):
        """可以创建 DuckDB 客户端"""
        from lakeanalysis.dbconnect import DuckDBClient

        client = DuckDBClient()
        assert client is not None
        assert client.data_dir is None
        client.close()

    def test_query_simple(self):
        """可以执行简单查询"""
        from lakeanalysis.dbconnect import DuckDBClient

        client = DuckDBClient()
        result = client.query_df("SELECT 1 as num")
        assert result["num"].iloc[0] == 1
        client.close()

    def test_query_with_params(self):
        """可以执行带参数的查询"""
        from lakeanalysis.dbconnect import DuckDBClient

        client = DuckDBClient()
        result = client.query_df("SELECT $1 as value", parameters={"1": "test"})
        assert result["value"].iloc[0] == "test"
        client.close()

    def test_query_returns_dict_list(self):
        """query() 返回字典列表"""
        from lakeanalysis.dbconnect import DuckDBClient

        client = DuckDBClient()
        result = client.query("SELECT 42 as answer, 'hello' as text")
        assert len(result) == 1
        assert result[0]["answer"] == 42
        assert result[0]["text"] == "hello"
        client.close()

    def test_context_manager(self):
        """上下文管理器正常工作"""
        from lakeanalysis.dbconnect import DuckDBClient

        with DuckDBClient() as client:
            result = client.query_df("SELECT 1")
            assert len(result) == 1

    def test_register_parquet(self, tmp_path):
        """可以注册 Parquet 文件"""
        import pandas as pd

        from lakeanalysis.dbconnect import DuckDBClient

        # 创建测试数据
        test_file = tmp_path / "test.parquet"
        df = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        df.to_parquet(test_file)

        # 注册并查询
        client = DuckDBClient()
        client.register_parquet("test_table", test_file)
        result = client.query_df("SELECT * FROM test_table")

        assert len(result) == 3
        assert list(result.columns) == ["id", "value"]
        assert result["value"].tolist() == ["a", "b", "c"]
        client.close()

    def test_register_parquet_with_dates(self, tmp_path):
        """Parquet 日期列正确处理"""
        import pandas as pd

        from lakeanalysis.dbconnect import DuckDBClient

        # 创建包含日期的测试数据
        test_file = tmp_path / "dates.parquet"
        df = pd.DataFrame({
            "id": [1, 2],
            "date": pd.to_datetime(["2024-01-01", "2024-06-15"])
        })
        df.to_parquet(test_file)

        client = DuckDBClient()
        client.register_parquet("dates", test_file)
        result = client.query_df("SELECT * FROM dates")

        assert len(result) == 2
        # 检查是 datetime 类型即可
        assert pd.api.types.is_datetime64_any_dtype(result["date"])
        client.close()

    def test_register_dir(self, tmp_path):
        """可以注册目录中的所有 Parquet 文件"""
        import pandas as pd

        from lakeanalysis.dbconnect import DuckDBClient

        # 创建多个 Parquet 文件
        df1 = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        df2 = pd.DataFrame({"id": [3, 4], "name": ["c", "d"]})
        df1.to_parquet(tmp_path / "table1.parquet")
        df2.to_parquet(tmp_path / "table2.parquet")

        # 注册目录
        client = DuckDBClient(data_dir=tmp_path)
        tables = client.list_registered_tables()

        assert "table1" in tables
        assert "table2" in tables

        # 查询
        result1 = client.query_df("SELECT COUNT(*) as cnt FROM table1")
        assert result1["cnt"].iloc[0] == 2

        client.close()

    def test_register_dir_creates_views(self, tmp_path):
        """register_dir 自动创建视图"""
        import pandas as pd

        from lakeanalysis.dbconnect import DuckDBClient

        # 创建测试文件
        df = pd.DataFrame({"x": [1, 2, 3]})
        df.to_parquet(tmp_path / "myview.parquet")

        client = DuckDBClient(data_dir=tmp_path)

        # 验证视图已创建
        result = client.query_df("SELECT * FROM myview")
        assert len(result) == 3

        client.close()

    def test_query_lake_info_structure(self, tmp_path):
        """模拟 lake_info 表结构查询"""
        import pandas as pd

        from lakeanalysis.dbconnect import DuckDBClient

        # 创建模拟的 lake_info 数据
        df = pd.DataFrame({
            "hylak_id": [1, 2, 3],
            "country": ["China", "USA", "Brazil"],
            "lake_area": [1.5, 2.3, 0.8],
            "pour_long": [120.5, -95.0, -45.0],
            "pour_lat": [35.0, 40.0, -10.0],
        })
        df.to_parquet(tmp_path / "lake_info.parquet")

        client = DuckDBClient(data_dir=tmp_path)

        # 验证查询
        result = client.query_df("""
            SELECT hylak_id, country, lake_area
            FROM lake_info
            WHERE lake_area > 1.0
            ORDER BY lake_area DESC
        """)

        assert len(result) == 2
        assert result["country"].iloc[0] == "USA"  # 2.3 是最大值

        client.close()

    def test_create_client_function(self, tmp_path):
        """create_client 便捷函数正常工作"""
        import pandas as pd

        from lakeanalysis.dbconnect import create_client

        # 创建测试数据
        df = pd.DataFrame({"x": [1, 2]})
        df.to_parquet(tmp_path / "test.parquet")

        # 使用便捷函数
        client = create_client(tmp_path)
        result = client.query_df("SELECT COUNT(*) as cnt FROM test")
        assert result["cnt"].iloc[0] == 2

        client.close()


class TestDuckDBClientErrors:
    """测试错误处理"""

    def test_register_dir_without_data_dir(self):
        """register_dir 需要 data_dir"""
        from lakeanalysis.dbconnect import DuckDBClient

        client = DuckDBClient()  # 没有 data_dir
        with pytest.raises(ValueError, match="data_dir 未设置"):
            client.register_dir()
        client.close()

    def test_invalid_parquet_file(self, tmp_path):
        """无效的 Parquet 文件会报错"""
        from lakeanalysis.dbconnect import DuckDBClient

        # 创建一个无效的文件
        invalid_file = tmp_path / "invalid.parquet"
        invalid_file.write_text("not a parquet file")

        client = DuckDBClient()
        with pytest.raises(Exception):  # DuckDB 会抛出异常
            client.register_parquet("invalid", invalid_file)
        client.close()

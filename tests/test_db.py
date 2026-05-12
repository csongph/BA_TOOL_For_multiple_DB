"""
test_db.py
Unit tests for connection pool management (db.py)
"""

import os
import sys
from pathlib import Path

import pytest
from unittest.mock import MagicMock, patch, call

# Ensure the project root is on sys.path so db.py can be imported from tests.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


# ────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────

def _reset_pools():
    """ล้าง _pools dict ก่อนแต่ละ test"""
    import db as db_module
    db_module._pools.clear()


@pytest.fixture(autouse=True)
def clear_pools():
    _reset_pools()
    yield
    _reset_pools()


def make_mock_pool():
    p = MagicMock()
    p.getconn.return_value = MagicMock()
    return p


# ────────────────────────────────────────────
# _get_db_configs()
# ────────────────────────────────────────────

class TestGetDbConfigs:
    def test_reads_db_url(self, monkeypatch):
        monkeypatch.setenv("DB_URL", "postgres://localhost/test")
        from db import _get_db_configs
        configs = _get_db_configs()
        assert "default" in configs
        assert configs["default"] == "postgres://localhost/test"

    def test_reads_db_url_named(self, monkeypatch):
        monkeypatch.setenv("DB_URL_MYDB", "postgres://localhost/mydb")
        from db import _get_db_configs
        configs = _get_db_configs()
        assert "mydb" in configs

    def test_named_key_is_lowercase(self, monkeypatch):
        monkeypatch.setenv("DB_URL_PRODUCTION", "postgres://localhost/prod")
        from db import _get_db_configs
        configs = _get_db_configs()
        assert "production" in configs

    def test_empty_env(self, monkeypatch):
        # ลบ env vars ที่อาจมีอยู่
        monkeypatch.delenv("DB_URL", raising=False)
        for k in list(os.environ):
            if k.startswith("DB_URL_"):
                monkeypatch.delenv(k, raising=False)
        from db import _get_db_configs
        configs = _get_db_configs()
        assert configs == {}

    def test_ignores_empty_value(self, monkeypatch):
        monkeypatch.setenv("DB_URL_EMPTY", "")
        from db import _get_db_configs
        configs = _get_db_configs()
        assert "empty" not in configs


# ────────────────────────────────────────────
# init_db_pool()
# ────────────────────────────────────────────

class TestInitDbPool:
    def test_init_single_dynamic(self):
        import db as db_module
        mock_pool = make_mock_pool()

        with patch("db.pool.ThreadedConnectionPool", return_value=mock_pool):
            db_module.init_db_pool("testdb", "postgres://localhost/test")

        assert "testdb" in db_module._pools

    def test_skip_if_already_initialized(self):
        import db as db_module
        mock_pool = make_mock_pool()
        db_module._pools["testdb"] = mock_pool

        with patch("db.pool.ThreadedConnectionPool") as mock_cls:
            db_module.init_db_pool("testdb", "postgres://localhost/test")

        mock_cls.assert_not_called()

    def test_init_all_from_env(self, monkeypatch):
        monkeypatch.setenv("DB_URL", "postgres://localhost/default")
        monkeypatch.setenv("DB_URL_SECOND", "postgres://localhost/second")

        import db as db_module
        mock_pool = make_mock_pool()

        with patch("db.pool.ThreadedConnectionPool", return_value=mock_pool):
            db_module.init_db_pool()

        assert "default" in db_module._pools
        assert "second" in db_module._pools

    def test_raises_on_no_env(self, monkeypatch):
        monkeypatch.delenv("DB_URL", raising=False)
        for k in list(os.environ):
            if k.startswith("DB_URL_"):
                monkeypatch.delenv(k, raising=False)

        import db as db_module
        with pytest.raises(RuntimeError, match="No DB_URL"):
            db_module.init_db_pool()

    def test_raises_on_connection_error(self):
        from psycopg2 import OperationalError
        import db as db_module

        with patch("db.pool.ThreadedConnectionPool", side_effect=OperationalError("conn refused")):
            with pytest.raises(OperationalError):
                db_module.init_db_pool("bad", "postgres://bad/db")

        assert "bad" not in db_module._pools


# ────────────────────────────────────────────
# get_connection()
# ────────────────────────────────────────────

class TestGetConnection:
    def test_returns_connection(self):
        import db as db_module
        mock_pool = make_mock_pool()
        db_module._pools["default"] = mock_pool

        conn = db_module.get_connection("default")
        assert conn is not None
        mock_pool.getconn.assert_called_once()

    def test_raises_on_unknown_pool(self):
        import db as db_module
        with pytest.raises(RuntimeError, match="not initialized"):
            db_module.get_connection("nonexistent")

    def test_raises_on_none_conn(self):
        import db as db_module
        mock_pool = make_mock_pool()
        mock_pool.getconn.return_value = None
        db_module._pools["default"] = mock_pool

        with pytest.raises(RuntimeError, match="No available connections"):
            db_module.get_connection("default")

    def test_raises_on_pool_exhausted(self):
        from psycopg2 import pool as psycopg2_pool
        import db as db_module

        mock_pool = make_mock_pool()
        mock_pool.getconn.side_effect = psycopg2_pool.PoolError("exhausted")
        db_module._pools["default"] = mock_pool

        with pytest.raises(RuntimeError, match="exhausted"):
            db_module.get_connection("default")

    def test_custom_db_name(self):
        import db as db_module
        mock_pool = make_mock_pool()
        db_module._pools["analytics"] = mock_pool

        conn = db_module.get_connection("analytics")
        assert conn is not None


# ────────────────────────────────────────────
# release_connection()
# ────────────────────────────────────────────

class TestReleaseConnection:
    def test_returns_conn_to_pool(self):
        import db as db_module
        mock_pool = make_mock_pool()
        db_module._pools["default"] = mock_pool
        conn = MagicMock()

        db_module.release_connection(conn, "default")
        mock_pool.putconn.assert_called_once_with(conn)

    def test_closes_if_pool_not_found(self):
        import db as db_module
        conn = MagicMock()

        # ไม่มี pool → ปิด conn โดยตรง
        db_module.release_connection(conn, "ghost_pool")
        conn.close.assert_called_once()

    def test_closes_conn_on_putconn_error(self):
        import db as db_module
        mock_pool = make_mock_pool()
        mock_pool.putconn.side_effect = Exception("putconn failed")
        db_module._pools["default"] = mock_pool
        conn = MagicMock()

        # ไม่ raise exception ออกมา
        db_module.release_connection(conn, "default")
        conn.close.assert_called_once()


# ────────────────────────────────────────────
# close_db_pool()
# ────────────────────────────────────────────

class TestCloseDbPool:
    def test_close_specific_pool(self):
        import db as db_module
        mock_pool = make_mock_pool()
        db_module._pools["testdb"] = mock_pool

        db_module.close_db_pool("testdb")
        mock_pool.closeall.assert_called_once()
        assert "testdb" not in db_module._pools

    def test_close_all_pools(self):
        import db as db_module
        p1, p2 = make_mock_pool(), make_mock_pool()
        db_module._pools["a"] = p1
        db_module._pools["b"] = p2

        db_module.close_db_pool()
        p1.closeall.assert_called_once()
        p2.closeall.assert_called_once()
        assert db_module._pools == {}

    def test_close_nonexistent_pool_no_error(self):
        import db as db_module
        # ไม่มี pool ชื่อนี้ → ไม่ raise
        db_module.close_db_pool("ghost")


# ────────────────────────────────────────────
# get_db_names()
# ────────────────────────────────────────────

class TestGetDbNames:
    def test_returns_initialized_names(self):
        import db as db_module
        db_module._pools["alpha"] = MagicMock()
        db_module._pools["beta"] = MagicMock()

        names = db_module.get_db_names()
        assert set(names) == {"alpha", "beta"}

    def test_empty_when_no_pools(self):
        import db as db_module
        assert db_module.get_db_names() == []


        
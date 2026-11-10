"""Tests for Gate G0: SQLite Durability Pragmas on macOS APFS.

Validates:
- PRAGMA synchronous = EXTRA (3)
- PRAGMA fullfsync = ON (1)
- PRAGMA foreign_keys = ON (1)
- PRAGMA journal_mode = DELETE ('delete')
- Reset of connection-local pragmas on reconnect
- Foreign key constraint enforcement
- Zero-WAL rollback journal lifecycle
- BEGIN IMMEDIATE concurrency locking
"""

import os
from pathlib import Path
import sqlite3
import tempfile

try:
    import pytest
except ImportError:
    class _RaisesContext:
        def __init__(self, expected_exception, match=None):
            self.expected_exception = expected_exception
            self.match = match
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc_val, exc_tb):
            if exc_type is None:
                raise AssertionError(f"Expected {self.expected_exception} but no exception was raised")
            if not issubclass(exc_type, self.expected_exception):
                return False
            if self.match and self.match not in str(exc_val):
                raise AssertionError(f"Expected match '{self.match}' in '{exc_val}'")
            return True

    class _PytestShim:
        @staticmethod
        def raises(expected_exception, match=None):
            return _RaisesContext(expected_exception, match)

    pytest = _PytestShim()


def configure_sqlite_connection(con: sqlite3.Connection) -> dict[str, int | str]:
    """Configure SQLite connection with mandatory durability pragmas."""
    cur = con.cursor()
    cur.execute("PRAGMA foreign_keys = ON;")
    cur.execute("PRAGMA journal_mode = DELETE;")
    cur.execute("PRAGMA synchronous = EXTRA;")
    cur.execute("PRAGMA fullfsync = ON;")
    cur.execute("PRAGMA busy_timeout = 5000;")

    fk = cur.execute("PRAGMA foreign_keys;").fetchone()[0]
    jm = cur.execute("PRAGMA journal_mode;").fetchone()[0]
    sync = cur.execute("PRAGMA synchronous;").fetchone()[0]
    ffsync = cur.execute("PRAGMA fullfsync;").fetchone()[0]

    assert fk == 1, f"Expected foreign_keys=1, got {fk}"
    assert jm.lower() == "delete", f"Expected journal_mode='delete', got {jm}"
    assert sync == 3, f"Expected synchronous=3 (EXTRA), got {sync}"
    assert ffsync == 1, f"Expected fullfsync=1 (ON), got {ffsync}"

    return {
        "foreign_keys": fk,
        "journal_mode": jm.lower(),
        "synchronous": sync,
        "fullfsync": ffsync,
    }


def test_durability_pragmas_readback(tmp_path: Path):
    """Verify exact return values and types of required SQLite durability pragmas."""
    db_path = tmp_path / "test_pragmas.sqlite"
    con = sqlite3.connect(db_path)
    readbacks = configure_sqlite_connection(con)

    assert readbacks["synchronous"] == 3
    assert readbacks["fullfsync"] == 1
    assert readbacks["foreign_keys"] == 1
    assert readbacks["journal_mode"] == "delete"
    con.close()


def test_connection_reset_semantics(tmp_path: Path):
    """Verify that connection-local pragmas reset on reconnect and must be reapplied."""
    db_path = tmp_path / "test_reset.sqlite"
    con1 = sqlite3.connect(db_path)
    configure_sqlite_connection(con1)
    con1.execute("CREATE TABLE t (id INT PRIMARY KEY);")
    con1.commit()
    con1.close()

    # Reopen connection: check that pragmas reset
    con2 = sqlite3.connect(db_path)
    cur = con2.cursor()
    assert cur.execute("PRAGMA journal_mode;").fetchone()[0].lower() == "delete"
    assert cur.execute("PRAGMA synchronous;").fetchone()[0] == 2  # resets to FULL
    assert cur.execute("PRAGMA fullfsync;").fetchone()[0] == 0    # resets to OFF
    assert cur.execute("PRAGMA foreign_keys;").fetchone()[0] == 0 # resets to OFF

    # Reconfiguration restores them
    configure_sqlite_connection(con2)
    assert cur.execute("PRAGMA synchronous;").fetchone()[0] == 3
    assert cur.execute("PRAGMA fullfsync;").fetchone()[0] == 1
    assert cur.execute("PRAGMA foreign_keys;").fetchone()[0] == 1
    con2.close()


def test_foreign_key_enforcement(tmp_path: Path):
    """Verify that foreign keys are strictly enforced and raise sqlite3.IntegrityError."""
    db_path = tmp_path / "test_fk.sqlite"
    con = sqlite3.connect(db_path)
    configure_sqlite_connection(con)

    con.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY);")
    con.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id));")
    con.commit()

    # Valid insert
    con.execute("INSERT INTO parent (id) VALUES (1);")
    con.execute("INSERT INTO child (id, parent_id) VALUES (10, 1);")
    con.commit()

    # Invalid insert: foreign key violation
    with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY constraint failed"):
        con.execute("INSERT INTO child (id, parent_id) VALUES (20, 999);")
        con.commit()

    # Invalid parent delete: foreign key violation
    with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY constraint failed"):
        con.execute("DELETE FROM parent WHERE id = 1;")
        con.commit()

    con.close()


def test_rollback_journal_lifecycle(tmp_path: Path):
    """Verify rollback journal creation during mutation and deletion after commit/rollback without WAL."""
    db_path = tmp_path / "test_journal.sqlite"
    journal_path = tmp_path / "test_journal.sqlite-journal"
    wal_path = tmp_path / "test_journal.sqlite-wal"
    shm_path = tmp_path / "test_journal.sqlite-shm"

    con = sqlite3.connect(db_path)
    configure_sqlite_connection(con)
    con.execute("CREATE TABLE records (id INTEGER PRIMARY KEY, val TEXT);")
    con.commit()

    assert not journal_path.exists()
    assert not wal_path.exists()
    assert not shm_path.exists()

    # Active mutating transaction
    con.execute("BEGIN IMMEDIATE;")
    con.execute("INSERT INTO records VALUES (1, 'active');")

    assert journal_path.exists(), "Rollback journal must exist during active transaction"
    assert not wal_path.exists()
    assert not shm_path.exists()

    con.commit()
    assert not journal_path.exists(), "Rollback journal must be deleted after commit"

    # Active rollback transaction
    con.execute("BEGIN IMMEDIATE;")
    con.execute("INSERT INTO records VALUES (2, 'aborted');")
    assert journal_path.exists()
    con.rollback()
    assert not journal_path.exists(), "Rollback journal must be deleted after rollback"

    con.close()


def test_short_transaction_immediate_locking(tmp_path: Path):
    """Verify BEGIN IMMEDIATE concurrency locking blocks concurrent writers."""
    db_path = tmp_path / "test_locking.sqlite"
    con1 = sqlite3.connect(db_path)
    configure_sqlite_connection(con1)
    con1.execute("CREATE TABLE records (id INTEGER PRIMARY KEY);")
    con1.commit()

    con2 = sqlite3.connect(db_path)
    configure_sqlite_connection(con2)

    # Con1 takes write lock via BEGIN IMMEDIATE
    con1.execute("BEGIN IMMEDIATE;")

    # Con2 attempting BEGIN IMMEDIATE with timeout 0.1s fails with OperationalError
    con2.execute("PRAGMA busy_timeout = 100;")
    with pytest.raises(sqlite3.OperationalError, match="database is locked"):
        con2.execute("BEGIN IMMEDIATE;")

    con1.commit()
    con1.close()
    con2.close()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as td:
        tpath = Path(td)
        test_durability_pragmas_readback(tpath)
        test_connection_reset_semantics(tpath)
        test_foreign_key_enforcement(tpath)
        test_rollback_journal_lifecycle(tpath)
        test_short_transaction_immediate_locking(tpath)
    print("ALL 5 SQLITE DURABILITY TESTS PASSED.")

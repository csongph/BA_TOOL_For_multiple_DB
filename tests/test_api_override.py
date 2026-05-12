import unittest
import uuid
from datetime import datetime

from backend.api.main import OverrideRequest, override
from backend.core.cache_store import result_cache


class TestOverrideEndpoint(unittest.TestCase):
    def tearDown(self):
        result_cache.clear()

    def test_override_prunes_column_diagnostics_and_refreshes_fk_errors(self):
        session_id = str(uuid.uuid4())
        result_cache[session_id] = {
            "tables": {
                "users": [
                    {
                        "column_name": "profile_id",
                        "final_type": "UNKNOWN",
                        "fk": {"ref_table": "profiles", "ref_column": "id"},
                        "is_pk": False,
                    }
                ],
                "profiles": [
                    {
                        "column_name": "id",
                        "final_type": "INTEGER",
                        "fk": None,
                        "is_pk": True,
                    }
                ],
            },
            "unknown": {
                "users": [{"column": "profile_id", "type": "mystery"}],
            },
            "byte_anomalies": {
                "users": [{"column_name": "profile_id", "detail": "stale anomaly"}],
            },
            "fk_errors": [
                {"table": "users", "column": "profile_id", "error": "stale fk error"},
            ],
            "created_at": datetime.now(),
        }

        response = override(session_id, OverrideRequest(table="users", column="profile_id", new_type="VARCHAR(36)"))

        self.assertEqual(response["updated_column"]["final_type"], "VARCHAR(36)")
        self.assertEqual(result_cache[session_id]["unknown"], {})
        self.assertEqual(result_cache[session_id]["byte_anomalies"], {})
        self.assertEqual(result_cache[session_id]["fk_errors"], [])


if __name__ == "__main__":
    unittest.main()

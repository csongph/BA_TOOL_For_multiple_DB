import unittest

from backend.parser.sql_parser import parse_sql, validate_fk


class TestSQLParserMySQL(unittest.TestCase):
    def test_mysql_table_options_and_single_fk_parse_correctly(self):
        sql = """
        CREATE TABLE hrEmployee (
            EmployeeID CHAR(36) NOT NULL,
            FirstName VARCHAR(100) NOT NULL,
            PRIMARY KEY (EmployeeID)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

        CREATE TABLE hrPayroll_Details (
            PayrollID CHAR(36) NOT NULL DEFAULT (UUID()),
            EmployeeID CHAR(36) NOT NULL,
            PRIMARY KEY (PayrollID),
            CONSTRAINT fk_payroll_employee
                FOREIGN KEY (EmployeeID) REFERENCES hrEmployee(EmployeeID)
                ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """

        rows = parse_sql(sql)
        tables = {}
        for row in rows:
            tables.setdefault(row["table"], []).append({
                "column_name": row["column"],
                "fk": row.get("fk"),
                "is_pk": row.get("is_pk"),
            })

        self.assertEqual(len(rows), 4)
        self.assertIn("hremployee", tables)
        self.assertIn("hrpayroll_details", tables)
        self.assertTrue(next(c for c in tables["hremployee"] if c["column_name"] == "employeeid")["is_pk"])

        employee_fk = next(c for c in tables["hrpayroll_details"] if c["column_name"] == "employeeid")
        self.assertEqual(employee_fk["fk"], {"ref_table": "hremployee", "ref_column": "employeeid"})
        self.assertEqual(validate_fk(tables), [])

    def test_validate_fk_reports_missing_referenced_column(self):
        tables = {
            "parent": [
                {"column_name": "id", "fk": None, "is_pk": True},
            ],
            "child": [
                {"column_name": "parent_id", "fk": {"ref_table": "parent", "ref_column": "missing_col"}, "is_pk": False},
            ],
        }

        errors = validate_fk(tables)

        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]["table"], "child")
        self.assertEqual(errors[0]["column"], "parent_id")
        self.assertIn("parent.missing_col", errors[0]["error"])


if __name__ == "__main__":
    unittest.main()

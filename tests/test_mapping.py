import unittest

from backend.core.converter import DataTypeConverter
from backend.repository.mapping_repo import MappingRepository


class TestDataTypeConverter(unittest.TestCase):
    def test_dest_final_preferred_over_standard_final(self):
        mapping = {
            'int': {
                'raw': 'int',
                'logical': 'int',
                'final': 'INT',
                'dest_final': 'INTEGER'
            }
        }
        conv = DataTypeConverter(mapping)
        result = conv.convert('int')

        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['final'], 'INTEGER')
        self.assertEqual(result['raw'], 'int')
        self.assertEqual(result['logical'], 'int')

    def test_final_falls_back_to_standard_final_when_dest_missing(self):
        mapping = {
            'varchar': {
                'raw': 'string',
                'logical': 'string',
                'final': 'VARCHAR',
                'dest_final': None
            }
        }
        conv = DataTypeConverter(mapping)
        result = conv.convert('varchar(50)')

        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['final'], 'VARCHAR(50)')
        self.assertEqual(result['raw'], 'string')
        self.assertEqual(result['logical'], 'string')

    def test_unknown_type_returns_unknown_status(self):
        mapping = {'int': {'raw': 'int', 'logical': 'int', 'final': 'INT', 'dest_final': None}}
        conv = DataTypeConverter(mapping)
        result = conv.convert('decimal(10,2)')

        self.assertEqual(result['status'], 'unknown')
        self.assertIsNone(result['raw'])
        self.assertIsNone(result['logical'])
        self.assertIsNone(result['final'])
        self.assertIn("Type 'decimal' not found", result['reason'])

    def test_bytes_decimal_and_binary_are_not_byte_anomalies(self):
        mapping = {
            'decimal': {'raw': 'bytes', 'logical': 'decimal', 'final': 'DECIMAL', 'dest_final': None},
            'varbinary': {'raw': 'bytes', 'logical': 'bytes', 'final': 'VARBINARY', 'dest_final': None},
        }
        conv = DataTypeConverter(mapping)

        decimal_result = conv.convert('decimal(10,2)')
        binary_result = conv.convert('varbinary(20)')

        self.assertFalse(decimal_result['byte_anomaly'])
        self.assertIsNone(decimal_result['byte_anomaly_detail'])
        self.assertFalse(binary_result['byte_anomaly'])
        self.assertIsNone(binary_result['byte_anomaly_detail'])

    def test_bytes_with_unexpected_logical_type_is_byte_anomaly(self):
        mapping = {
            'weird': {'raw': 'bytes', 'logical': 'string', 'final': 'VARCHAR', 'dest_final': None},
        }
        conv = DataTypeConverter(mapping)
        result = conv.convert('weird')

        self.assertTrue(result['byte_anomaly'])
        self.assertIn("string", result['byte_anomaly_detail'])


class TestMappingRepositoryHelpers(unittest.TestCase):
    def test_rows_to_dict_pair_keeps_dest_final_if_present(self):
        rows = [
            ('varchar', 'string', 'string', 'VARCHAR', 'NVARCHAR', False, False, False),
            ('int', 'int', 'int', 'INT', None, False, False, False),
        ]
        mapping = MappingRepository._rows_to_dict_pair(rows)

        self.assertEqual(mapping['varchar']['final'], 'VARCHAR')
        self.assertEqual(mapping['varchar']['dest_final'], 'NVARCHAR')
        self.assertEqual(mapping['int']['final'], 'INT')
        self.assertIsNone(mapping['int']['dest_final'])

    def test_rows_to_dict_pair_handles_missing_dest_type(self):
        rows = [
            ('text', 'string', 'string', 'VARCHAR', None, False, False, False),
        ]
        mapping = MappingRepository._rows_to_dict_pair(rows)

        self.assertEqual(mapping['text']['final'], 'VARCHAR')
        self.assertIsNone(mapping['text']['dest_final'])

    def test_rows_to_dict_pair_ignores_none_sql_type(self):
        rows = [
            (None, 'string', 'string', 'VARCHAR', 'TEXT', False, False, False),
        ]
        mapping = MappingRepository._rows_to_dict_pair(rows)

        self.assertEqual(mapping, {})


if __name__ == '__main__':
    unittest.main()

import unittest
import os
from airflow.providers.postgres.hooks.postgres import PostgresHook

class TestCreateTable(unittest.TestCase):
    def test_create_table(self):
        postgres_hook = PostgresHook(postgres_conn_id=os.environ.get("ETL_CONN_ID"))
        table_exists = postgres_hook.get_records("""
            SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'users_derivation');
        
        """)
        self.assertIsNotNone(table_exists)  # [(True,)]
        self.assertIsNotNone(table_exists[0])  # (True,)
        self.assertTrue(table_exists[0][0])

    def test_insert_data(self):
        postgres_hook = PostgresHook(postgres_conn_id=os.environ.get("ETL_CONN_ID"))
        result = postgres_hook.get_records("SELECT COUNT(*) FROM users_derivation;")
        self.assertIsNotNone(result)  # [(10,)]
        self.assertIsNotNone(result[0])  # (10,)
        self.assertNotEqual(result[0][0], 0)  # 10

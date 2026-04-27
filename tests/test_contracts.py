import unittest

from adapter_ate.contracts import (
    DEFECT_FIELDS,
    PROCESSED_PRODUCT_FIELDS,
    RAW_PRODUCTS_FIELDS,
    RAW_STATION_EVENTS_FIELDS,
    RAW_TEST_ITEMS_FIELDS,
)


class ContractTests(unittest.TestCase):
    def test_raw_schemas_include_stable_identifiers(self):
        self.assertIn("run_id", RAW_PRODUCTS_FIELDS)
        self.assertIn("item_id", RAW_TEST_ITEMS_FIELDS)
        self.assertIn("event_id", RAW_STATION_EVENTS_FIELDS)

    def test_processed_schemas_include_traceability_and_result_fields(self):
        self.assertIn("sn", PROCESSED_PRODUCT_FIELDS)
        self.assertIn("final_result", PROCESSED_PRODUCT_FIELDS)
        self.assertIn("failure_codes", PROCESSED_PRODUCT_FIELDS)
        self.assertIn("item_id", DEFECT_FIELDS)
        self.assertIn("failure_code", DEFECT_FIELDS)


if __name__ == "__main__":
    unittest.main()

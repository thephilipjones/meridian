"""Tests for synthetic data generators.

Validates that generated data matches expected schemas, distributions,
and referential integrity constraints.
"""

import os
import tempfile

import pytest


class TestGenerateCRM:
    def test_generates_expected_volume(self):
        from src.data_gen.generate_crm import generate_accounts, generate_deals

        accounts = generate_accounts(50)
        deals = generate_deals(accounts, 200)

        assert len(accounts) == 50
        assert len(deals) == 200

    def test_deal_fields_present(self):
        from src.data_gen.generate_crm import generate_accounts, generate_deals

        accounts = generate_accounts(10)
        deals = generate_deals(accounts, 10)

        required_fields = [
            "deal_id", "account_name", "account_id", "deal_name",
            "stage", "amount", "close_date", "product_line",
        ]
        for deal in deals:
            for field in required_fields:
                assert field in deal, f"Missing field: {field}"

    def test_valid_stages(self):
        from src.data_gen.generate_crm import STAGES, generate_accounts, generate_deals

        accounts = generate_accounts(10)
        deals = generate_deals(accounts, 100)

        for deal in deals:
            assert deal["stage"] in STAGES

    def test_arr_only_for_closed_won(self):
        from src.data_gen.generate_crm import generate_accounts, generate_deals

        accounts = generate_accounts(50)
        deals = generate_deals(accounts, 500)

        for deal in deals:
            if deal["stage"] != "Closed Won":
                assert deal["arr"] == 0.0

    def test_csv_output(self):
        from src.data_gen.generate_crm import main

        with tempfile.TemporaryDirectory() as tmpdir:
            deals = main(output_path=tmpdir)
            csv_path = os.path.join(tmpdir, "crm_deals.csv")
            assert os.path.exists(csv_path)
            assert len(deals) > 0


class TestGenerateWebEvents:
    def test_generates_events(self):
        from src.data_gen.generate_web_events import generate_customers, generate_events

        customers = generate_customers(10)
        events = generate_events(customers, 500)

        assert len(events) > 0
        assert len(events) <= 500

    def test_event_fields_present(self):
        from src.data_gen.generate_web_events import generate_customers, generate_events

        customers = generate_customers(5)
        events = generate_events(customers, 50)

        required_fields = [
            "event_id", "event_type", "event_timestamp",
            "customer_id", "product", "status_code",
        ]
        for event in events:
            for field in required_fields:
                assert field in event, f"Missing field: {field}"

    def test_json_output(self):
        from src.data_gen.generate_web_events import main

        with tempfile.TemporaryDirectory() as tmpdir:
            events = main(output_path=tmpdir)
            files = [f for f in os.listdir(tmpdir) if f.endswith(".json")]
            assert len(files) > 0


class TestGenerateFinancials:
    def test_generates_rows(self):
        from src.data_gen.generate_financials import generate_financials

        rows = generate_financials()
        assert len(rows) > 0

    def test_row_fields_present(self):
        from src.data_gen.generate_financials import generate_financials

        rows = generate_financials()
        required_fields = [
            "fiscal_quarter", "fiscal_year", "product_line",
            "revenue", "cost_of_data", "gross_margin",
        ]
        for row in rows:
            for field in required_fields:
                assert field in row, f"Missing field: {field}"

    def test_margin_consistency(self):
        from src.data_gen.generate_financials import generate_financials

        rows = generate_financials()
        for row in rows:
            expected_margin = row["revenue"] - row["cost_of_data"]
            assert abs(row["gross_margin"] - expected_margin) < 0.01

    def test_csv_output(self):
        from src.data_gen.generate_financials import main

        with tempfile.TemporaryDirectory() as tmpdir:
            rows = main(output_path=tmpdir)
            csv_path = os.path.join(tmpdir, "financial_summaries.csv")
            assert os.path.exists(csv_path)

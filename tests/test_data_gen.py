"""Tests for synthetic data generators.

Validates that generated data matches expected schemas, distributions,
and referential integrity constraints.
"""

import os
import tempfile

import pytest


class TestGenerateCRM:
    def test_generates_deals_for_all_accounts(self):
        from src.data_gen.generate_crm import CURATED_ACCOUNTS, _generate_deals_for_account

        all_deals = []
        for account in CURATED_ACCOUNTS[:5]:
            deals = _generate_deals_for_account(account)
            assert len(deals) > 0
            all_deals.extend(deals)

        account_names = {d["account_name"] for d in all_deals}
        assert len(account_names) == 5

    def test_deal_fields_present(self):
        from src.data_gen.generate_crm import CURATED_ACCOUNTS, _generate_deals_for_account

        deals = _generate_deals_for_account(CURATED_ACCOUNTS[0])

        required_fields = [
            "deal_id", "account_name", "account_id", "deal_name",
            "stage", "amount", "close_date", "product_line",
        ]
        for deal in deals:
            for field in required_fields:
                assert field in deal, f"Missing field: {field}"

    def test_valid_stages(self):
        from src.data_gen.generate_crm import CURATED_ACCOUNTS, STAGES, _generate_deals_for_account

        for account in CURATED_ACCOUNTS[:10]:
            deals = _generate_deals_for_account(account)
            for deal in deals:
                assert deal["stage"] in STAGES

    def test_arr_only_for_closed_won(self):
        from src.data_gen.generate_crm import CURATED_ACCOUNTS, _generate_deals_for_account

        for account in CURATED_ACCOUNTS[:10]:
            deals = _generate_deals_for_account(account)
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
    def test_generates_events_per_account(self):
        from src.data_gen.generate_web_events import CURATED_ACCOUNTS, _generate_events_for_account

        for account in CURATED_ACCOUNTS[:5]:
            events = _generate_events_for_account(account)
            assert len(events) > 0
            for event in events:
                assert event["account_name"] == account["name"]

    def test_event_fields_present(self):
        from src.data_gen.generate_web_events import CURATED_ACCOUNTS, _generate_events_for_account

        events = _generate_events_for_account(CURATED_ACCOUNTS[0])

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


class TestGenerateSecFilings:
    def test_generates_expected_volume(self):
        from src.data_gen.generate_sec_filings import generate_filings

        filings = generate_filings(100)
        assert len(filings) == 100

    def test_filing_fields_present(self):
        from src.data_gen.generate_sec_filings import generate_filings

        filings = generate_filings(20)
        required_fields = [
            "filing_id", "cik", "company_name", "filing_type",
            "filing_date", "description", "sic_code", "state",
        ]
        for filing in filings:
            for field in required_fields:
                assert field in filing, f"Missing field: {field}"

    def test_valid_filing_types(self):
        from src.data_gen.generate_sec_filings import FILING_TYPES, generate_filings

        filings = generate_filings(200)
        for filing in filings:
            assert filing["filing_type"] in FILING_TYPES

    def test_json_output(self):
        from src.data_gen.generate_sec_filings import main

        with tempfile.TemporaryDirectory() as tmpdir:
            filings = main(output_path=tmpdir)
            files = [f for f in os.listdir(tmpdir) if f.endswith(".json")]
            assert len(files) > 0
            assert len(filings) > 0


class TestGenerateFdaActions:
    def test_generates_expected_volume(self):
        from src.data_gen.generate_fda_actions import generate_actions

        actions = generate_actions(100)
        assert len(actions) == 100

    def test_action_fields_present(self):
        from src.data_gen.generate_fda_actions import generate_actions

        actions = generate_actions(20)
        required_fields = [
            "action_id", "product_description", "reason_for_recall",
            "status", "classification", "recall_initiation_date",
            "company_name", "city", "state",
        ]
        for action in actions:
            for field in required_fields:
                assert field in action, f"Missing field: {field}"

    def test_valid_classifications(self):
        from src.data_gen.generate_fda_actions import CLASSIFICATIONS, generate_actions

        actions = generate_actions(200)
        for action in actions:
            assert action["classification"] in CLASSIFICATIONS

    def test_valid_statuses(self):
        from src.data_gen.generate_fda_actions import STATUSES, generate_actions

        actions = generate_actions(200)
        for action in actions:
            assert action["status"] in STATUSES

    def test_json_output(self):
        from src.data_gen.generate_fda_actions import main

        with tempfile.TemporaryDirectory() as tmpdir:
            actions = main(output_path=tmpdir)
            files = [f for f in os.listdir(tmpdir) if f.endswith(".json")]
            assert len(files) > 0
            assert len(actions) > 0


class TestGeneratePatents:
    def test_generates_expected_volume(self):
        from src.data_gen.generate_patents import generate_patents

        patents = generate_patents(100)
        assert len(patents) == 100

    def test_patent_fields_present(self):
        from src.data_gen.generate_patents import generate_patents

        patents = generate_patents(20)
        required_fields = [
            "patent_number", "title", "abstract", "assignee",
            "filing_date", "grant_date", "patent_type", "uspc_class",
        ]
        for patent in patents:
            for field in required_fields:
                assert field in patent, f"Missing field: {field}"

    def test_valid_patent_types(self):
        from src.data_gen.generate_patents import PATENT_TYPES, generate_patents

        patents = generate_patents(200)
        for patent in patents:
            assert patent["patent_type"] in PATENT_TYPES

    def test_grant_after_filing(self):
        from src.data_gen.generate_patents import generate_patents

        patents = generate_patents(100)
        for patent in patents:
            assert patent["grant_date"] >= patent["filing_date"]

    def test_json_output(self):
        from src.data_gen.generate_patents import main

        with tempfile.TemporaryDirectory() as tmpdir:
            patents = main(output_path=tmpdir)
            files = [f for f in os.listdir(tmpdir) if f.endswith(".json")]
            assert len(files) > 0
            assert len(patents) > 0

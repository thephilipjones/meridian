"""Tests validating that generated data would pass pipeline quality expectations.

These tests simulate the expectation checks that SDP silver pipelines apply,
ensuring data generators produce compliant data before deployment.
"""

import pytest


class TestCRMExpectations:
    """Validate CRM data against silver pipeline expectations."""

    def test_deal_id_not_null(self):
        from src.data_gen.generate_crm import generate_accounts, generate_deals

        accounts = generate_accounts(10)
        deals = generate_deals(accounts, 100)
        for deal in deals:
            assert deal["deal_id"] is not None
            assert len(deal["deal_id"]) > 0

    def test_amount_non_negative(self):
        from src.data_gen.generate_crm import generate_accounts, generate_deals

        accounts = generate_accounts(10)
        deals = generate_deals(accounts, 100)
        for deal in deals:
            assert deal["amount"] >= 0


class TestWebEventExpectations:
    """Validate web events against silver pipeline expectations."""

    def test_event_id_not_null(self):
        from src.data_gen.generate_web_events import generate_customers, generate_events

        customers = generate_customers(5)
        events = generate_events(customers, 100)
        for event in events:
            assert event["event_id"] is not None

    def test_timestamp_not_null(self):
        from src.data_gen.generate_web_events import generate_customers, generate_events

        customers = generate_customers(5)
        events = generate_events(customers, 100)
        for event in events:
            assert event["event_timestamp"] is not None


class TestFinancialExpectations:
    """Validate financial data against silver pipeline expectations."""

    def test_quarter_not_null(self):
        from src.data_gen.generate_financials import generate_financials

        rows = generate_financials()
        for row in rows:
            assert row["fiscal_quarter"] is not None

    def test_revenue_non_negative(self):
        from src.data_gen.generate_financials import generate_financials

        rows = generate_financials()
        for row in rows:
            assert row["revenue"] >= 0


class TestSecFilingExpectations:
    """Validate SEC filing data against silver pipeline expectations."""

    def test_filing_id_not_null(self):
        from src.data_gen.generate_sec_filings import generate_filings

        filings = generate_filings(100)
        for filing in filings:
            assert filing["filing_id"] is not None
            assert len(filing["filing_id"]) > 0

    def test_company_name_not_null(self):
        from src.data_gen.generate_sec_filings import generate_filings

        filings = generate_filings(100)
        for filing in filings:
            assert filing["company_name"] is not None
            assert len(filing["company_name"]) > 0

    def test_filing_date_format(self):
        from datetime import datetime

        from src.data_gen.generate_sec_filings import generate_filings

        filings = generate_filings(100)
        for filing in filings:
            datetime.strptime(filing["filing_date"], "%Y-%m-%d")


class TestFdaActionExpectations:
    """Validate FDA action data against silver pipeline expectations."""

    def test_action_id_not_null(self):
        from src.data_gen.generate_fda_actions import generate_actions

        actions = generate_actions(100)
        for action in actions:
            assert action["action_id"] is not None
            assert len(action["action_id"]) > 0

    def test_company_name_not_null(self):
        from src.data_gen.generate_fda_actions import generate_actions

        actions = generate_actions(100)
        for action in actions:
            assert action["company_name"] is not None

    def test_recall_date_format(self):
        from datetime import datetime

        from src.data_gen.generate_fda_actions import generate_actions

        actions = generate_actions(100)
        for action in actions:
            datetime.strptime(action["recall_initiation_date"], "%Y-%m-%d")


class TestPatentExpectations:
    """Validate patent data against silver pipeline expectations."""

    def test_patent_number_not_null(self):
        from src.data_gen.generate_patents import generate_patents

        patents = generate_patents(100)
        for patent in patents:
            assert patent["patent_number"] is not None
            assert patent["patent_number"].startswith("US")

    def test_title_not_null(self):
        from src.data_gen.generate_patents import generate_patents

        patents = generate_patents(100)
        for patent in patents:
            assert patent["title"] is not None
            assert len(patent["title"]) > 0

    def test_abstract_not_null(self):
        from src.data_gen.generate_patents import generate_patents

        patents = generate_patents(100)
        for patent in patents:
            assert patent["abstract"] is not None
            assert len(patent["abstract"]) > 0

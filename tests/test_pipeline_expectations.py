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

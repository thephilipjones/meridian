"""Delta Sharing info endpoints for the customer view.

Provides connection instructions and pre-built code snippets for
accessing Meridian data products via Delta Sharing across platforms.
"""

import os

from fastapi import APIRouter

router = APIRouter()

_catalog = os.environ.get("MERIDIAN_CATALOG", "serverless_stable_k2zkdm_catalog")
_hostname = os.environ.get("DATABRICKS_SERVER_HOSTNAME", "your-workspace.databricks.com")


@router.get("/connection-info")
def get_connection_info():
    """Return Delta Sharing connection details for the current customer."""
    return {
        "share_name": "meridian_regulatory_share",
        "provider": "Meridian Insights",
        "provider_endpoint": f"https://{_hostname}/api/2.0/delta-sharing",
        "recipient_name": "acme_bank",
        "shared_tables": [
            {
                "schema": "meridian_regulatory",
                "table": "regulatory_actions",
                "description": "Unified regulatory actions across SEC filings and FDA enforcement",
            },
            {
                "schema": "meridian_regulatory",
                "table": "company_entities",
                "description": "Master entity table — companies resolved across SEC, FDA, and patent sources",
            },
        ],
        "activation_status": "Active",
        "instructions": (
            "Use the credentials file provided by Meridian to connect from your "
            "preferred platform. Code snippets are available for Databricks, "
            "Snowflake, Pandas, and Power BI."
        ),
    }


@router.get("/code-snippets")
def get_code_snippets():
    """Return pre-built code snippets for various platforms."""
    share = "meridian_regulatory_share"
    schema = "meridian_regulatory"

    return {
        "databricks": {
            "label": "Databricks",
            "language": "python",
            "code": (
                f'# Read shared table in Databricks\n'
                f'df = spark.read.format("deltaSharing")\\\n'
                f'    .load("{share}.{schema}.regulatory_actions")\n'
                f'\n'
                f'display(df.limit(10))\n'
                f'\n'
                f'# Create a managed table from the share\n'
                f'df.write.saveAsTable("my_catalog.my_schema.regulatory_actions")'
            ),
        },
        "snowflake": {
            "label": "Snowflake",
            "language": "sql",
            "code": (
                f'-- Create an external table from Delta Sharing in Snowflake\n'
                f'CREATE OR REPLACE EXTERNAL TABLE regulatory_actions\n'
                f'  USING delta_sharing\n'
                f'  LOCATION = \'profile_file://{share}.{schema}.regulatory_actions\';\n'
                f'\n'
                f'SELECT * FROM regulatory_actions LIMIT 10;'
            ),
        },
        "pandas": {
            "label": "Pandas (Python)",
            "language": "python",
            "code": (
                f'import delta_sharing\n'
                f'\n'
                f'# Path to your credentials file from Meridian\n'
                f'profile_file = "meridian_credentials.share"\n'
                f'\n'
                f'# List available tables\n'
                f'client = delta_sharing.SharingClient(profile_file)\n'
                f'print(client.list_all_tables())\n'
                f'\n'
                f'# Load as a Pandas DataFrame\n'
                f'table_url = f"{{profile_file}}#{share}.{schema}.regulatory_actions"\n'
                f'df = delta_sharing.load_as_pandas(table_url)\n'
                f'print(df.head())'
            ),
        },
        "powerbi": {
            "label": "Power BI",
            "language": "text",
            "code": (
                f'1. Open Power BI Desktop\n'
                f'2. Get Data → More → Delta Sharing\n'
                f'3. Enter the sharing server URL:\n'
                f'   https://{_hostname}/api/2.0/delta-sharing\n'
                f'4. Authenticate with the bearer token from your credentials file\n'
                f'5. Select share: {share}\n'
                f'6. Select tables: regulatory_actions, company_entities\n'
                f'7. Click Load to import the data'
            ),
        },
    }

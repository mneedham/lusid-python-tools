import unittest
from pathlib import Path

# tag::imports[]
import lusid
from lusid import models

import pytz
import pandas as pd
import uuid
from datetime import datetime, timedelta
# end::imports[]


class InstrumentsMaster(unittest.TestCase):
    def write_to_test_output(self, df, file_name):
        df.to_csv(Path(__file__).parent.joinpath(f"data/test_transaction_portfolios/test_output/{file_name}"), index=False)

    def test_maintain_instruments_master(self) -> None:
        # tag::api-factory[]
        secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")
        api_factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=secrets_file
        )
        # end::api-factory[]

        scope = f"UK-Trading-{uuid.uuid4()}"

        # tag::create-apis[]
        transaction_portfolios_api = api_factory.build(lusid.api.TransactionPortfoliosApi)
        property_definitions_api = api_factory.build(lusid.api.PropertyDefinitionsApi)
        portfolios_api = api_factory.build(lusid.api.PortfoliosApi)
        # end::create-apis[]

        # tag::create-portfolio-default[]
        portfolio = transaction_portfolios_api.create_portfolio(
            scope=scope,
            create_transaction_portfolio_request=models.CreateTransactionPortfolioRequest(
                display_name="Portfolio UK",
                code=f"PortfolioUk-{uuid.uuid4()}",
                description="Portfolio for UK market",
                base_currency="GBP",
                created=datetime(2021, 3, 20, tzinfo=pytz.utc)
            )
        )
        portfolio_code = portfolio.id.code
        # end::create-portfolio-default[]
        self.assertIsNotNone(portfolio_code)

        # tag::create-property[]
        response = property_definitions_api.create_property_definition(
            create_property_definition_request=models.CreatePropertyDefinitionRequest(
                domain="Portfolio",
                scope=scope,
                code="portfolio_manager_name",
                value_required=False,
                display_name="portfolio_manager_name",
                data_type_id=models.ResourceId(scope="system", code="string")
            )
        )
        portfolio_manager_property = response.key
        # end::create-property[]
        self.assertIsNotNone(portfolio_manager_property)

        # tag::update-portfolio[]
        portfolios_api.upsert_portfolio_properties(
            scope=scope,
            code=portfolio_code,
            request_body={
                portfolio_manager_property: models.ModelProperty(
                    key=portfolio_manager_property,
                    value=models.PropertyValue(label_value="Matt Smith")
                )
            }
        )
        # end::update-portfolio[]

        # tag::get-portfolio[]
        portfolio = portfolios_api.get_portfolio(
            scope=scope,
            code=portfolio_code,
            property_keys=[portfolio_manager_property]
        )
        portfolio_df = pd.DataFrame([{
            "Code": portfolio.id.code,
            "Name": portfolio.display_name,
            "Description": portfolio.description,
            "Base Currency": portfolio.base_currency,
            "Manager Name": portfolio.properties[portfolio_manager_property].value.label_value,

        }])
        # end::get-portfolio[]
        self.write_to_test_output(portfolio_df, "get_portfolio.csv")
        self.assertEqual(portfolio.properties[portfolio_manager_property].value.label_value, "Matt Smith")

        # tag::create-portfolio-with-manager[]
        response = transaction_portfolios_api.create_portfolio(
            scope=scope,
            create_transaction_portfolio_request=models.CreateTransactionPortfolioRequest(
                display_name="Portfolio UK",
                code=f"PortfolioUk-{uuid.uuid4()}",
                description="Portfolio for UK market",
                base_currency="GBP",
                properties={
                    portfolio_manager_property: models.ModelProperty(
                        key=portfolio_manager_property,
                        value=models.PropertyValue(label_value="David Jones"))
                }
            )
        )
        portfolio_code = response.id.code
        # end::create-portfolio-with-manager[]

        # tag::get-updated-portfolio[]
        portfolio = portfolios_api.get_portfolio(
            scope=scope,
            code=portfolio_code,
            property_keys=[portfolio_manager_property]
        )
        portfolio_df = pd.DataFrame([{
            "Code": portfolio.id.code,
            "Name": portfolio.display_name,
            "Description": portfolio.description,
            "Base Currency": portfolio.base_currency,
            "Manager Name": portfolio.properties[portfolio_manager_property].value.label_value,
        }])
        # end::get-updated-portfolio[]
        self.write_to_test_output(portfolio_df, "get_new_portfolio.csv")
        self.assertEqual(portfolio.properties[portfolio_manager_property].value.label_value, "David Jones")





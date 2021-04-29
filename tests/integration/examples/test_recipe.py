import unittest
from pathlib import Path
import tests.integration.examples.lusid_utils as lusid_utils

# tag::imports[]
import lusid
from lusid import models

import uuid
import pytz
from datetime import datetime
# end::imports[]


class Recipe(unittest.TestCase):
    def write_to_test_output(self, df, file_name):
        df.to_csv(Path(__file__).parent.joinpath(f"data/test_recipe/test_output/{file_name}"), index=False)

    def test_valuation(self) -> None:
        api_factory = lusid_utils.api_factory

        # tag::apis[]
        transaction_portfolios_api = api_factory.build(lusid.api.TransactionPortfoliosApi)
        recipe_api = api_factory.build(lusid.api.ConfigurationRecipeApi)
        # end::apis[]

        # tag::create-portfolio[]
        scope = "Developer-Recipes-Tutorial"
        created_date = datetime(year=2019, month=1, day=1, tzinfo=pytz.UTC).isoformat()
        portfolio = transaction_portfolios_api.create_portfolio(
            scope=scope,
            create_transaction_portfolio_request=models.CreateTransactionPortfolioRequest(
                display_name="Developer Recipes Tutorial",
                code=f"Developer-Recipes-Tutorial-{uuid.uuid4()}",
                created=created_date,
                base_currency="USD"
            )
        )
        portfolio_code = portfolio.id.code
        # end::create-portfolio[]
        self.assertIsNotNone(portfolio_code)

        # tag::create-recipe[]
        response = recipe_api.upsert_configuration_recipe(
            upsert_recipe_request=models.UpsertRecipeRequest(
                configuration_recipe=models.ConfigurationRecipe(
                    scope=scope,
                    code=portfolio_code,
                    market=models.MarketContext(
                        options=models.MarketOptions(
                            default_supplier="Lusid",
                            default_instrument_code_type="LusidInstrumentId",
                            default_scope=scope)))))
        print(response)
        # end::create-recipe[]


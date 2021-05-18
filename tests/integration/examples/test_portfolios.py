import unittest
from pathlib import Path
import tests.integration.examples.lusid_utils as lusid_utils

# tag::imports-lusid[]
import lusid
# end::imports-lusid[]

# tag::imports-dates[]
import pytz
from datetime import datetime
# end::imports-dates[]

class Portfolios(unittest.TestCase):
    def write_to_test_output(self, df, file_name):
        df.to_csv(Path(__file__).parent.joinpath(f"data/test_portfolios/test_output/{file_name}"), index=False)

    def test_portfolios(self) -> None:
        api_factory = lusid_utils.api_factory

        # tag::create-portfolio-api[]
        transaction_portfolios_api = api_factory.build(lusid.api.TransactionPortfoliosApi)
        # end::create-portfolio-api[]

        # tag::derived-api[]
        derived_api = api_factory.build(lusid.api.DerivedTransactionPortfoliosApi)
        # end::derived-api[]

        # tag::portfolio-groups-api[]
        portfolio_groups_api = api_factory.build(lusid.api.PortfolioGroupsApi)
        # end::portfolio-groups-api[]

        # tag::scope[]
        scope = "Developer-Portfolios-Tutorial"
        # end::scope[]

        # tag::uk-portfolio-code[]
        uk_equities_portfolio_code = "uk-equities-trading"
        # end::uk-portfolio-code[]

        # tag::us-portfolio-code[]
        us_equities_portfolio_code = "us-equities-trading"
        # end::us-portfolio-code[]

        now = datetime.now().strftime('%Y-%m-%d-%H_%M_%S')
        scope = f"Developer-Portfolios-Tutorial-{now}"
        print(scope)

        # tag::create-portfolios[]
        created_date = datetime(year=2021, month=1, day=1, tzinfo=pytz.UTC).isoformat()
        portfolios = [(uk_equities_portfolio_code, "GBP"), (us_equities_portfolio_code, "USD")]
        for portfolio_code, currency in portfolios:
            transaction_portfolios_api.create_portfolio(
                scope=scope,
                create_transaction_portfolio_request=lusid.models.CreateTransactionPortfolioRequest(
                    display_name="Developer Portfolios Tutorial",
                    code=portfolio_code,
                    created=created_date,
                    base_currency=currency))
        # end::create-portfolios[]

        # tag::derived-portfolio-code[]
        derived_portfolio_code = f"{uk_equities_portfolio_code}-derived"
        # end::derived-portfolio-code[]

        # tag::create-derived-portfolio[]
        derived_api.create_derived_portfolio(
            scope=scope,
            create_derived_transaction_portfolio_request=lusid.models.CreateDerivedTransactionPortfolioRequest(
                display_name=f"Derived Portfolio of {uk_equities_portfolio_code}",
                code=derived_portfolio_code,
                parent_portfolio_id=lusid.models.ResourceId(scope=scope, code=uk_equities_portfolio_code),
                created=created_date
            ))
        # end::create-derived-portfolio[]

        # tag::global-portfolio-code[]
        global_portfolio_code = "global-equities-trading"
        # end::global-portfolio-code[]

        # tag::creation-date[]
        created_date = datetime(year=2021, month=1, day=1, tzinfo=pytz.UTC).isoformat()
        # end::creation-date[]

        # tag::create-portfolio-group[]
        portfolio_groups_api.create_portfolio_group(
            scope=scope,
            create_portfolio_group_request=lusid.models.CreatePortfolioGroupRequest(
                code=global_portfolio_code,
                display_name="Global Equities Trading",
                created=created_date
            ))
        # end::create-portfolio-group[]

        # tag::add-to-portfolio-group[]
        for portfolio_code in [uk_equities_portfolio_code, us_equities_portfolio_code]:
            portfolio_groups_api.add_portfolio_to_group(
                scope=scope,
                code=global_portfolio_code,
                effective_at=created_date,
                resource_id=lusid.models.ResourceId(scope=scope, code=portfolio_code))
        # end::add-to-portfolio-group[]

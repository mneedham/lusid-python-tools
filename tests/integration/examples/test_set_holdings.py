import unittest
from pathlib import Path
import tests.integration.examples.lusid_utils as lusid_utils

# tag::imports[]
import lusid
from lusid import models

import pytz
import pandas as pd
import uuid
from datetime import datetime
from dateutil.parser import parse


# end::imports[]


class Quotes(unittest.TestCase):
    def write_to_test_output(self, df, file_name):
        df.to_csv(Path(__file__).parent.joinpath(f"data/test_set_holdings/test_output/{file_name}"), index=False)

    def test_set_holdings(self) -> None:
        api_factory = lusid_utils.api_factory

        # tag::apis[]
        transaction_portfolios_api = api_factory.build(lusid.api.TransactionPortfoliosApi)
        instruments_api = api_factory.build(lusid.api.InstrumentsApi)
        # end::apis[]
        portfolios_api = api_factory.build(lusid.api.PortfoliosApi)

        # tag::create-portfolio[]
        now = datetime.now().strftime('%Y-%m-%d-%H_%M_%S')
        scope = portfolio_code = f"Developer-Set-Holdings-Tutorial-{now}"
        created_date = datetime(year=2019, month=1, day=1, tzinfo=pytz.UTC).isoformat()
        portfolio = transaction_portfolios_api.create_portfolio(
            scope=scope,
            create_transaction_portfolio_request=models.CreateTransactionPortfolioRequest(
                display_name="Developer Set Holdings Tutorial",
                code=portfolio_code,
                created=created_date,
                base_currency="USD"
            )
        )
        # end::create-portfolio[]
        self.assertIsNotNone(portfolio.id.code)

        # tag::holdings-file[]
        quotes_file = "data/test_quotes/holdings.csv"
        # end::holdings-file[]
        quotes_file = Path(__file__).parent.joinpath(quotes_file)

        # tag::load-holdings[]
        holdings = pd.read_csv(quotes_file)
        # end::load-holdings[]
        self.write_to_test_output(holdings, "holdings.csv")

        # tag::import-instruments[]
        response = instruments_api.upsert_instruments(request_body={
            holding["figi"]: models.InstrumentDefinition(
                name=holding["instrument_name"],
                identifiers={
                    "Figi": models.InstrumentIdValue(value=holding["figi"])
                },
            )
            for _, holding in holdings.iterrows()
        })

        figi_to_luid = {
            instrument.identifiers["Figi"]: instrument.lusid_instrument_id
            for _, instrument in response.values.items()
        }
        # end::import-instruments[]

        # tag::set-holdings[]
        holdings_adjustments = [
            models.HoldingAdjustment(
                instrument_identifiers={"Instrument/default/Figi": holding["figi"]},
                instrument_uid=figi_to_luid[holding["figi"]],
                tax_lots=[models.TargetTaxLot(units=holding["units"])])
            for _, holding in holdings.iterrows()
        ]
        transaction_portfolios_api.set_holdings(
            scope=scope,
            code=portfolio_code,
            effective_at=datetime(2020, 1, 1, tzinfo=pytz.UTC),
            adjust_holding_request=holdings_adjustments
        )
        # end::set-holdings[]

        portfolios_api.delete_portfolio(scope, portfolio_code)

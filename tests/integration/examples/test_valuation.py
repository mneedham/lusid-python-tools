import unittest
from pathlib import Path
import tests.integration.examples.lusid_utils as lusid_utils

# tag::imports[]
import lusid
from lusid import models

import pytz
import pandas as pd
from datetime import datetime
from dateutil.parser import parse
# end::imports[]

from itertools import groupby


class Valuation(unittest.TestCase):
    def write_to_test_output(self, df, file_name):
        df.to_csv(Path(__file__).parent.joinpath(f"data/test_valuation/test_output/{file_name}"), index=False)

    def test_valuation(self) -> None:
        api_factory = lusid_utils.api_factory

        # tag::apis[]
        instruments_api = api_factory.build(lusid.api.InstrumentsApi)
        transaction_portfolios_api = api_factory.build(lusid.api.TransactionPortfoliosApi)
        quotes_api = api_factory.build(lusid.api.QuotesApi)
        aggregation_api = api_factory.build(lusid.api.AggregationApi)
        # end::apis[]
        portfolios_api = api_factory.build(lusid.api.PortfoliosApi)

        # tag::scope-portfolio-code[]
        scope = portfolio_code = "Developer-Valuation-Tutorial"
        # end::scope-portfolio-code[]
        now = datetime.now().strftime('%Y-%m-%d-%H_%M_%S')
        scope = portfolio_code = f"Developer-Valuation-Tutorial-{now}"

        # tag::create-portfolio[]
        created_date = datetime(year=2019, month=1, day=1, tzinfo=pytz.UTC).isoformat()
        transaction_portfolios_api.create_portfolio(
            scope=scope,
            create_transaction_portfolio_request=models.CreateTransactionPortfolioRequest(
                display_name="Developer Valuation Tutorial",
                code=portfolio_code,
                created=created_date,
                base_currency="USD"))
        print(scope)
        # end::create-portfolio[]
        self.assertIsNotNone(portfolio_code)

        # tag::holdings-file[]
        quotes_file = "data/test_valuation/holdings.csv"
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

        # tag::quotes-file[]
        quotes_file = "data/test_valuation/quotes.csv"
        # end::quotes-file[]
        quotes_file = Path(__file__).parent.joinpath(quotes_file)

        # tag::load-quotes[]
        quotes = pd.read_csv(quotes_file)
        # end::load-quotes[]
        self.write_to_test_output(quotes, "quotes.csv")

        # tag::import-quotes[]
        quotes_request = {
            f"quote_request_{quote['instrument_name']}_{quote['date']}": models.UpsertQuoteRequest(
                quote_id=models.QuoteId(
                    quote_series_id=models.QuoteSeriesId(
                        provider="Lusid",
                        instrument_id=figi_to_luid[quote["figi"]],
                        instrument_id_type="LusidInstrumentId",
                        quote_type="Price",
                        field="mid",
                    ),
                    effective_at=pytz.UTC.localize(parse(quote['date'])).isoformat(),
                ),
                metric_value=models.MetricValue(value=quote['price'], unit="USD"),
            )
            for _, quote in quotes.iterrows()
        }

        quotes_api.upsert_quotes(scope=scope, request_body=quotes_request)
        # end::import-quotes[]

        # tag::compute-valuation[]
        def compute_valuation_with_default_recipe(scope, portfolio_code, effective_date, metrics, group_by):
            return aggregation_api.get_valuation(
                valuation_request=models.ValuationRequest(
                    recipe_id=models.ResourceId(scope=scope, code="default"),
                    metrics=[models.AggregateSpec(key, op) for key, op in metrics],
                    group_by=group_by,
                    valuation_schedule=models.ValuationSchedule(effective_at=effective_date),
                    portfolio_entity_ids=[models.PortfolioEntityId(
                        scope=scope,
                        code=portfolio_code,
                        portfolio_entity_type="SinglePortfolio"
                    )])).data
        # end::compute-valuation[]

        # tag::get-valuation-all[]
        metrics = [
            ("Analytic/default/ValuationDate", "Value"),
            ("Holding/default/PV", "Sum"),
        ]
        group_by = ["Analytic/default/ValuationDate"]
        # tag::get-valuation-all[]

        # tag::get-valuation-total[]
        effective_at = datetime(year=2021, month=4, day=21, tzinfo=pytz.UTC)
        response = compute_valuation_with_default_recipe(scope, portfolio_code, effective_at, metrics, group_by)
        valuation_all = pd.DataFrame(response)
        # end::get-valuation-total[]
        self.write_to_test_output(valuation_all, "valuation-all.csv")
        self.assertAlmostEqual(valuation_all["Sum(Holding/default/PV)"][0], 532212.0, 3)

        # tag::get-valuation-by-instrument[]
        metrics = [
            ("Instrument/default/Name", "Value"),
            ("Holding/default/Units", "Sum"),
            ("Holding/default/PV", "Sum"),
            ("Holding/default/PV", "Proportion")
        ]
        group_by = ["Instrument/default/Name"]
        # end::get-valuation-by-instrument[]

        # tag::get-valuation-20210421[]
        effective_at = datetime(year=2021, month=4, day=21, tzinfo=pytz.UTC)
        response = compute_valuation_with_default_recipe(scope, portfolio_code, effective_at, metrics, group_by)
        valuation = pd.DataFrame(response)
        # end::get-valuation-20210421[]
        self.write_to_test_output(valuation, "valuation-20210421.csv")
        self.assertAlmostEqual(valuation["Proportion(Holding/default/PV)"][0], 0.631707, 3)

        # tag::get-valuation-20210422[]
        effective_at = datetime(year=2021, month=4, day=22, tzinfo=pytz.UTC)
        response = compute_valuation_with_default_recipe(scope, portfolio_code, effective_at, metrics, group_by)
        valuation = pd.DataFrame(response)
        # end::get-valuation-20210422[]
        self.write_to_test_output(valuation, "valuation-20210422.csv")
        self.assertAlmostEqual(valuation["Proportion(Holding/default/PV)"][0], 0.6397, 3)

        portfolios_api.delete_portfolio(scope, portfolio_code)

import unittest
import uuid
from pathlib import Path
import tests.integration.examples.lusid_utils as lusid_utils

# tag::imports-lusid[]
import lusid
# end::imports-lusid[]

# tag::imports-pandas[]
import pandas as pd
# end::imports-pandas[]

# tag::imports-dates[]
import pytz
from datetime import datetime, timedelta
from dateutil.parser import parse
# end::imports-dates[]

# tag::imports[]

# end::imports[]


class WhatIf(unittest.TestCase):
    def write_to_test_output(self, df, file_name):
        df.to_csv(Path(__file__).parent.joinpath(f"data/test_what_if/test_output/{file_name}"), index=False)

    def test_ibor(self) -> None:
        api_factory = lusid_utils.api_factory

        transaction_portfolios_api = api_factory.build(lusid.api.TransactionPortfoliosApi)
        instruments_api = api_factory.build(lusid.api.InstrumentsApi)
        quotes_api = api_factory.build(lusid.api.QuotesApi)
        aggregation_api = api_factory.build(lusid.api.AggregationApi)
        derived_api = api_factory.build(lusid.api.DerivedTransactionPortfoliosApi)

        # tag::instruments-file[]
        instruments_file = "data/test_what_if/instruments.csv"
        # end::instruments-file[]
        instruments_file = Path(__file__).parent.joinpath(instruments_file)

        # tag::load-instruments[]
        instruments = pd.read_csv(instruments_file)
        # end::load-instruments[]

        # tag::import-instruments[]
        definitions = {}
        for _, instrument in instruments.iterrows():
            identifiers = {
                "ClientInternal": lusid.models.InstrumentIdValue(value=instrument["id"])
            }
            definitions[instrument['instrument_name']] = lusid.models.InstrumentDefinition(
                name=instrument['instrument_name'], identifiers=identifiers)

        instruments_api.upsert_instruments(request_body=definitions)

        now = datetime.now().strftime('%Y-%m-%d-%H_%M_%S')
        scope = portfolio_code = f"Developer-WhatIf-Tutorial-{now}"
        print("Portfolio Code", portfolio_code)

        # tag::create-portfolio[]
        created_date = datetime(year=2021, month=1, day=1, tzinfo=pytz.UTC)
        transaction_portfolios_api.create_portfolio(
            scope=scope,
            create_transaction_portfolio_request=lusid.models.CreateTransactionPortfolioRequest(
                display_name="Developer What If Tutorial",
                code=portfolio_code,
                created=created_date,
                base_currency="USD"))
        # end::create-portfolio[]

        # tag::transactions-file[]
        transactions_file = "data/test_what_if/transactions.csv"
        # end::transactions-file[]
        transactions_file = Path(__file__).parent.joinpath(transactions_file)

        # tag::load-transactions[]
        transactions = pd.read_csv(transactions_file)
        # end::load-transactions[]
        print("Transactions:")
        print(transactions)
        print("")

        # tag::import-transactions[]
        transactions_request = []
        for row, txn in transactions.iterrows():
            if txn["instrument_id"] == "cash":
                instrument_identifier = {"Instrument/default/Currency": txn["currency"]}
            else:
                instrument_identifier = {"Instrument/default/ClientInternal": txn["instrument_id"]}

            transactions_request.append(
                lusid.models.TransactionRequest(
                    transaction_id=txn["txn_id"],
                    type=txn["transaction_type"],
                    instrument_identifiers=instrument_identifier,
                    transaction_date=pytz.UTC.localize(parse(txn["trade_date"])),
                    settlement_date=pytz.UTC.localize(parse(txn["trade_date"])),
                    units=txn["quantity"],
                    transaction_price=lusid.models.TransactionPrice(price=txn["price"], type="Price"),
                    total_consideration=lusid.models.CurrencyAndAmount(
                        amount=txn["net_money"], currency=txn["currency"])))

        transaction_portfolios_api.upsert_transactions(
            scope=scope, code=portfolio_code, transaction_request=transactions_request)
        # end::import-transactions[]

        # tag::quotes-file[]
        quotes_file = "data/test_what_if/quotes.csv"
        # end::quotes-file[]
        quotes_file = Path(__file__).parent.joinpath(quotes_file)

        # tag::load-quotes[]
        quotes = pd.read_csv(quotes_file)
        # end::load-quotes[]
        print("Quotes:")
        print(quotes)
        print("")

        # tag::load-quotes-get-luid[]
        response = instruments_api.get_instruments(
            identifier_type='ClientInternal',
            request_body=quotes["id"].values.tolist())
        instruments = pd.DataFrame([{
            "id": instrument.identifiers["ClientInternal"],
            "luid": instrument.lusid_instrument_id}
            for _, instrument in response.values.items()
        ])
        quotes_with_luid = pd.merge(quotes, instruments, on=["id"])
        # end::load-quotes-get-luid[]

        # tag::import-quotes[]
        quotes_request = {
            f"quote_request_{quote['instrument_name']}_{quote['date']}": lusid.models.UpsertQuoteRequest(
                quote_id=lusid.models.QuoteId(
                    quote_series_id=lusid.models.QuoteSeriesId(
                        provider="Lusid",
                        instrument_id=quote["luid"],
                        instrument_id_type="LusidInstrumentId",
                        quote_type="Price",
                        field="mid",
                    ),
                    effective_at=pytz.UTC.localize(parse(quote['date'])).isoformat(),
                ),
                metric_value=lusid.models.MetricValue(value=quote['price'], unit="USD"),
            )
            for _, quote in quotes_with_luid.iterrows()
        }

        quotes_api.upsert_quotes(scope=scope, request_body=quotes_request)

        # tag::compute-valuation[]
        def compute_valuation_with_default_recipe(portfolio_code, from_date, to_date, metrics, group_by):
            return aggregation_api.get_valuation(
                valuation_request=lusid.models.ValuationRequest(
                    recipe_id=lusid.models.ResourceId(scope=scope, code="default"),
                    metrics=[lusid.models.AggregateSpec(key, op) for key, op in metrics],
                    group_by=group_by,
                    valuation_schedule=lusid.models.ValuationSchedule(effective_from=from_date, effective_at=to_date),
                    portfolio_entity_ids=[lusid.models.PortfolioEntityId(
                        scope=scope,
                        code=portfolio_code,
                        portfolio_entity_type="SinglePortfolio"
                    )])).data

        # end::compute-valuation[]

        effective_at = datetime(year=2021, month=5, day=5, tzinfo=pytz.UTC)
        response = compute_valuation_with_default_recipe(
            portfolio_code,
            effective_at,
            effective_at,
            metrics=[
                ("Analytic/default/ValuationDate", "Value"),
                ("Holding/default/PV", "Sum"),
            ],
            group_by=["Analytic/default/ValuationDate"]
        )
        valuation_all = pd.DataFrame(response)
        print("Valuation overall")
        print(valuation_all)
        print("")

        effective_at = datetime(year=2021, month=5, day=5, tzinfo=pytz.UTC)
        response = compute_valuation_with_default_recipe(
            portfolio_code,
            effective_at,
            effective_at,
            metrics=[
                ("Instrument/default/Name", "Value"),
                ("Holding/default/Cost", "Value"),
                ("Holding/default/Units", "Sum"),
                ("Holding/default/PV", "Sum"),
                ("Holding/default/PV", "Proportion")
            ],
            group_by=["Instrument/default/Name"]
        )
        valuation = pd.DataFrame(response)
        print("Valuation per instrument")
        print(valuation)
        print("")

        derived_portfolio_code = f"Developer-WhatIf-Tutorial-Derived-{now}"

        derived_request = lusid.models.CreateDerivedTransactionPortfolioRequest(
            display_name=f"Derived Portfolio of {portfolio_code}",
            code=derived_portfolio_code,
            parent_portfolio_id=lusid.models.ResourceId(scope=scope, code=portfolio_code),
            description="What if we didn't sell DOGE?",
            created=created_date
        )

        derived_api.create_derived_portfolio(
            scope=scope,
            create_derived_transaction_portfolio_request=derived_request
        )

        # Undo transactions
        transaction_portfolios_api.cancel_transactions(
            scope=scope,
            code=derived_portfolio_code,
            transaction_ids=["tx_00003", "tx_00004"]
        )
        print("Cancelled Transactions on 2nd February 2021")

        for code in [portfolio_code, derived_portfolio_code]:
            effective_at = datetime(year=2021, month=5, day=5, tzinfo=pytz.UTC)
            response = compute_valuation_with_default_recipe(
                code,
                effective_at,
                effective_at,
                metrics=[
                    ("Analytic/default/ValuationDate", "Value"),
                    ("Holding/default/PV", "Sum"),
                ],
                group_by=["Analytic/default/ValuationDate"]
            )
            valuation_all = pd.DataFrame(response)
            print(f"Valuation for {code}")
            print(valuation_all)
            print("")

            response = compute_valuation_with_default_recipe(
                code,
                effective_at,
                effective_at,
                metrics=[
                    ("Instrument/default/Name", "Value"),
                    ("Holding/default/Cost", "Value"),
                    ("Holding/default/Units", "Sum"),
                    ("Holding/default/PV", "Sum"),
                    ("Holding/default/PV", "Proportion")
                ],
                group_by=["Instrument/default/Name"]
            )
            valuation = pd.DataFrame(response)
            print(f"Valuation per instrument for {code}")
            print(valuation)
            print("")

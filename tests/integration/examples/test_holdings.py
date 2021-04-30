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


class Holdings(unittest.TestCase):
    def write_to_test_output(self, df, file_name):
        df.to_csv(Path(__file__).parent.joinpath(f"data/test_holdings/test_output/{file_name}"), index=False)

    def test_transactions(self) -> None:
        api_factory = lusid_utils.api_factory

        # tag::apis[]
        instruments_api = api_factory.build(lusid.api.InstrumentsApi)
        transaction_portfolios_api = api_factory.build(lusid.api.TransactionPortfoliosApi)
        # end::apis[]
        portfolios_api = api_factory.build(lusid.api.PortfoliosApi)

        # tag::create-portfolio[]
        now = datetime.now().strftime('%Y-%m-%d-%H_%M_%S')
        scope = portfolio_code = f"Developer-Holdings-Tutorial-{now}"
        created_date = datetime(year=2010, month=1, day=5, tzinfo=pytz.UTC).isoformat()
        portfolio = transaction_portfolios_api.create_portfolio(
            scope=scope,
            create_transaction_portfolio_request=models.CreateTransactionPortfolioRequest(
                display_name="Developer Holdings Tutorial",
                code=portfolio_code,
                created=created_date,
                base_currency="GBP"))
        # end::create-portfolio[]
        self.assertIsNotNone(portfolio.id.code)

        # tag::transactions-file[]
        transactions_file = "data/test_holdings/transactions.csv"
        # end::transactions-file[]
        transactions_file = Path(__file__).parent.joinpath(transactions_file)

        # tag::load-transactions[]
        transactions = pd.read_csv(transactions_file)
        # end::load-transactions[]
        self.write_to_test_output(transactions, "transactions.csv")

        # tag::import-instruments[]
        instruments = transactions[transactions["client_internal"] != "cash"]
        definitions = {
            instrument["instrument_desc"]: models.InstrumentDefinition(
                name=instrument["instrument_desc"],
                identifiers={"ClientInternal": models.InstrumentIdValue(value=instrument["client_internal"])},
            )
            for _, instrument in instruments.iterrows()
        }

        instruments_api.upsert_instruments(request_body=definitions)
        # end::import-instruments[]

        # tag::import-transactions[]
        transactions_request = []
        for row, txn in transactions.iterrows():
            if txn["client_internal"] == "cash":
                instrument_identifier = {"Instrument/default/Currency": txn["instrument_currency"]}
            else:
                instrument_identifier = {"Instrument/default/ClientInternal": txn["client_internal"]}

            transactions_request.append(
                models.TransactionRequest(
                    transaction_id=txn["txn_id"],
                    type=txn["transaction_type"],
                    instrument_identifiers=instrument_identifier,
                    transaction_date=pytz.UTC.localize(parse(txn["trade_date"])).isoformat(),
                    settlement_date=pytz.UTC.localize(parse(txn["trade_date"])).isoformat(),
                    units=txn["quantity"],
                    transaction_price=models.TransactionPrice(price=txn["price"], type="Price"),
                    total_consideration=models.CurrencyAndAmount(
                        amount=txn["net_money"], currency=txn["instrument_currency"]
                    )))

        transaction_portfolios_api.upsert_transactions(
            scope=scope, code=portfolio_code, transaction_request=transactions_request)
        # end::import-transactions[]

        # tag::format-holdings[]
        def display_holdings_summary(response):
            return pd.DataFrame([{
                "Instrument": value.properties["Instrument/default/Name"].value.label_value,
                "Amount": value.cost.amount,
                "Units": value.units,
                "Type": value.holding_type
            } for value in response.values])
        # end::format-holdings[]

        # tag::get-holdings-funds-loaded[]
        holding_response = transaction_portfolios_api.get_holdings(
            scope=scope,
            code=portfolio_code,
            property_keys=["Instrument/default/Name"],
            effective_at=datetime(year=2020, month=1, day=1, hour=1, tzinfo=pytz.UTC).isoformat(),
        )
        holdings = display_holdings_summary(holding_response)
        # end::get-holdings-funds-loaded[]
        self.write_to_test_output(holdings, "holdings_funds_loaded.csv")
        self.assertEqual(holdings.shape[0], 1)

        # tag::get-holdings-first-day-trading[]
        holding_response = transaction_portfolios_api.get_holdings(
            scope=scope,
            code=portfolio_code,
            property_keys=["Instrument/default/Name"],
            effective_at=datetime(year=2020, month=1, day=2, hour=1, tzinfo=pytz.UTC).isoformat(),
        )
        holdings = display_holdings_summary(holding_response)
        # end::get-holdings-first-day-trading[]
        self.write_to_test_output(holdings, "holdings_first_day_trading.csv")
        self.assertEqual(holdings.shape[0], 2)

        # tag::get-holdings-second-day-trading[]
        holding_response = transaction_portfolios_api.get_holdings(
            scope=scope,
            code=portfolio_code,
            property_keys=["Instrument/default/Name"],
            effective_at=datetime(year=2020, month=1, day=3, hour=1, tzinfo=pytz.UTC).isoformat(),
        )
        holdings = display_holdings_summary(holding_response)
        # end::get-holdings-second-day-trading[]
        self.write_to_test_output(holdings, "holdings_second_day_trading.csv")
        self.assertEqual(holdings.shape[0], 4)

        # tag::get-holdings-today[]
        holding_response = transaction_portfolios_api.get_holdings(
            scope=scope,
            code=portfolio_code,
            property_keys=["Instrument/default/Name"]
        )
        holdings = display_holdings_summary(holding_response)
        # end::get-holdings-today[]
        self.write_to_test_output(holdings, "holdings.csv")
        self.assertEqual(holdings.shape[0], 4)

        # tag::get-holdings-positions[]
        holding_response = transaction_portfolios_api.get_holdings(
            scope=scope,
            code=portfolio_code,
            filter="holdingType eq 'P'",
            property_keys=["Instrument/default/Name"]
        )
        holdings = display_holdings_summary(holding_response)
        # end::get-holdings-positions[]
        self.write_to_test_output(holdings, "holdings_positions.csv")
        self.assertEqual(holdings.shape[0], 3)

        portfolios_api.delete_portfolio(scope, portfolio_code)
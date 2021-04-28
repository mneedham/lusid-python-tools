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


class Transactions(unittest.TestCase):
    def write_to_test_output(self, df, file_name):
        df.to_csv(Path(__file__).parent.joinpath(f"data/test_transactions/test_output/{file_name}"), index=False)

    def test_transactions(self) -> None:
        api_factory = lusid_utils.api_factory

        # tag::apis[]
        instruments_api = api_factory.build(lusid.api.InstrumentsApi)
        transaction_portfolios_api = api_factory.build(lusid.api.TransactionPortfoliosApi)
        # end::apis[]

        # tag::create-portfolio[]
        scope = f"UK-Trading-{uuid.uuid4()}"
        created_date = datetime(year=2010, month=1, day=5, tzinfo=pytz.UTC).isoformat()
        portfolio = transaction_portfolios_api.create_portfolio(
            scope=scope,
            create_transaction_portfolio_request=models.CreateTransactionPortfolioRequest(
                display_name="Portfolio UK",
                code=f"PortfolioUk-{uuid.uuid4()}",
                created=created_date,
                base_currency="GBP"))
        portfolio_code = portfolio.id.code
        # end::create-portfolio[]
        self.assertIsNotNone(portfolio_code)

        # tag::transactions-file[]
        transactions_file = "data/test_transactions/transactions.csv"
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
                        amount=txn["net_money"], currency=txn["instrument_currency"])))

            transaction_portfolios_api.upsert_transactions(
                scope=scope, code=portfolio_code, transaction_request=transactions_request)
        # end::import-transactions[]

        # tag::format-transactions[]
        def display_transactions_summary(response):
            return pd.DataFrame([{
                "Transaction ID": value.transaction_id,
                "Instrument": value.properties["Instrument/default/Name"].value.label_value,
                "Entry Date": value.entry_date_time,
                "Amount": value.total_consideration.amount,
                "Units": value.units,
            } for value in response.values
            ])
        # end::format-transactions[]

        # tag::get-transactions[]
        response = transaction_portfolios_api.get_transactions(
            scope=scope, code=portfolio_code, property_keys=["Instrument/default/Name"],)
        tx_response = display_transactions_summary(response)
        # end::get-transactions[]
        self.write_to_test_output(tx_response, "transactions_response.csv")

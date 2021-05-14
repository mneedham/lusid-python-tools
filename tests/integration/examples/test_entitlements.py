import unittest
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
from datetime import datetime
from dateutil.parser import parse
# end::imports-dates[]


class Entitlements(unittest.TestCase):
    def write_to_test_output(self, df, file_name):
        df.to_csv(Path(__file__).parent.joinpath(f"data/test_what_if/test_output/{file_name}"), index=False)

    def test_entitlements(self) -> None:
        api_factory = lusid_utils.api_factory

        transaction_portfolios_api = api_factory.build(lusid.api.TransactionPortfoliosApi)
        instruments_api = api_factory.build(lusid.api.InstrumentsApi)
        portfolio_groups_api = api_factory.build(lusid.api.PortfolioGroupsApi)

        now = datetime.now().strftime('%Y-%m-%d-%H_%M_%S')
        scope = f"Developer-Entitlements-Tutorial-{now}"
        print("Scope", scope)

        # scope = "Developer-Entitlements-Tutorial-2021-05-12-11_39_33"
        uk_portfolio_code = "uk-equities-trading"
        us_portfolio_code = "us-equities-trading"

        # tag::create-portfolio[]
        created_date = datetime(year=2021, month=1, day=1, tzinfo=pytz.UTC)

        transaction_portfolios_api.create_portfolio(
            scope=scope,
            create_transaction_portfolio_request=lusid.models.CreateTransactionPortfolioRequest(
                display_name="UK Equities Trading",
                code=uk_portfolio_code,
                created=created_date,
                base_currency="GBP"))

        transaction_portfolios_api.create_portfolio(
            scope=scope,
            create_transaction_portfolio_request=lusid.models.CreateTransactionPortfolioRequest(
                display_name="US Equities Trading",
                code=us_portfolio_code,
                created=created_date,
                base_currency="USD"))
        # end::create-portfolio[]

        # tag::instruments-file[]
        instruments_file = "data/test_entitlements/instruments.csv"
        # end::instruments-file[]
        instruments_file = Path(__file__).parent.joinpath(instruments_file)

        # tag::load-instruments[]
        instruments = pd.read_csv(instruments_file)
        # end::load-instruments[]
        self.write_to_test_output(instruments, "instruments.csv")

        # tag::import-instruments[]
        identifier_columns = [('figi', 'Figi'), ('client_internal', 'ClientInternal')]
        definitions = {}
        for _, instrument in instruments.iterrows():
            identifiers = {
                identifier[1]: lusid.models.InstrumentIdValue(value=instrument[identifier[0]])
                for identifier in identifier_columns
            }
            definitions[instrument['instrument_name']] = lusid.models.InstrumentDefinition(
                name=instrument['instrument_name'], identifiers=identifiers)

        instruments_api.upsert_instruments(request_body=definitions)

        transactions_file = "data/test_entitlements/us_transactions.csv"
        transactions_file = Path(__file__).parent.joinpath(transactions_file)
        us_transactions = pd.read_csv(transactions_file)

        transactions_request = []
        for row, txn in us_transactions.iterrows():
            if txn["figi"] == "cash":
                instrument_identifier = {"Instrument/default/Currency": txn["currency"]}
            else:
                instrument_identifier = {"Instrument/default/Figi": txn["figi"]}

            transactions_request.append(
                lusid.models.TransactionRequest(
                    transaction_id=txn["txn_id"],
                    type=txn["transaction_type"],
                    instrument_identifiers=instrument_identifier,
                    transaction_date=pytz.UTC.localize(parse(txn["trade_date"])).isoformat(),
                    settlement_date=pytz.UTC.localize(parse(txn["trade_date"])).isoformat(),
                    units=txn["quantity"],
                    transaction_price=lusid.models.TransactionPrice(price=txn["price"], type="Price"),
                    total_consideration=lusid.models.CurrencyAndAmount(
                        amount=txn["net_money"], currency=txn["currency"])))

        transaction_portfolios_api.upsert_transactions(
            scope=scope, code=us_portfolio_code, transaction_request=transactions_request)

        transactions_file = "data/test_entitlements/uk_transactions.csv"
        transactions_file = Path(__file__).parent.joinpath(transactions_file)
        uk_transactions = pd.read_csv(transactions_file)

        transactions_request = []
        for row, txn in uk_transactions.iterrows():
            if txn["figi"] == "cash":
                instrument_identifier = {"Instrument/default/Currency": txn["currency"]}
            else:
                instrument_identifier = {"Instrument/default/Figi": txn["figi"]}

            transactions_request.append(
                lusid.models.TransactionRequest(
                    transaction_id=txn["txn_id"],
                    type=txn["transaction_type"],
                    instrument_identifiers=instrument_identifier,
                    transaction_date=pytz.UTC.localize(parse(txn["trade_date"])).isoformat(),
                    settlement_date=pytz.UTC.localize(parse(txn["trade_date"])).isoformat(),
                    units=txn["quantity"],
                    transaction_price=lusid.models.TransactionPrice(price=txn["price"], type="Price"),
                    total_consideration=lusid.models.CurrencyAndAmount(
                        amount=txn["net_money"], currency=txn["currency"])))

        transaction_portfolios_api.upsert_transactions(
            scope=scope, code=uk_portfolio_code, transaction_request=transactions_request)

        global_portfolio_code = "global-equities-trading"
        portfolio_groups_api.create_portfolio_group(
            scope=scope,
            create_portfolio_group_request=lusid.models.CreatePortfolioGroupRequest(
                code=global_portfolio_code,
                display_name="Global Equities Trading",
                created=created_date
            ))

        portfolio_groups_api.add_portfolio_to_group(
            scope=scope,
            code=global_portfolio_code,
            effective_at=created_date,
            resource_id=lusid.models.ResourceId(scope=scope, code=uk_portfolio_code))
        portfolio_groups_api.add_portfolio_to_group(
            scope=scope,
            code=global_portfolio_code,
            effective_at=created_date,
            resource_id=lusid.models.ResourceId(scope=scope, code=us_portfolio_code))



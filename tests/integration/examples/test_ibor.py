import json
import unittest
import uuid
from pathlib import Path
import tests.integration.examples.lusid_utils as lusid_utils
import tests.integration.examples.holdings as hold

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


class IBOR(unittest.TestCase):
    def write_to_test_output(self, df, file_name):
        df.to_csv(Path(__file__).parent.joinpath(f"data/test_ibor/test_output/{file_name}"), index=False)

    def test_ibor(self) -> None:
        api_factory = lusid_utils.api_factory

        # tag::apis[]
        aggregation_api = api_factory.build(lusid.api.AggregationApi)
        # end::apis[]

        # tag::quotes-api[]
        quotes_api = api_factory.build(lusid.api.QuotesApi)
        # end::quotes-api[]

        portfolios_api = api_factory.build(lusid.api.PortfoliosApi)

        # tag::instruments-api[]
        instruments_api = api_factory.build(lusid.api.InstrumentsApi)
        # end::instruments-api[]

        # tag::property-api[]
        property_definitions_api = api_factory.build(lusid.api.PropertyDefinitionsApi)
        # end::property-api[]

        ##################
        # INSTRUMENTS MASTER
        ##################

        # tag::instruments-file[]
        instruments_file = "data/test_ibor/instruments.csv"
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

        response = instruments_api.upsert_instruments(request_body=definitions)
        items = response.values.items()

        luids = pd.DataFrame([
            {"Figi": instrument.identifiers["Figi"],
             "Instrument": instrument.name,
             "LUID": instrument.lusid_instrument_id}
            for _, instrument in items
        ])
        # end::import-instruments[]
        self.write_to_test_output(luids, "luids.csv")
        self.assertEqual(luids.shape[0], 5)

        # tag::get-instrument[]
        response = instruments_api.get_instrument(identifier_type='Figi', identifier='BBG000B9XVV8')
        instrument_df = pd.DataFrame([{
            "Figi": response.identifiers["Figi"],
            "Instrument": response.name,
            "ClientInternal": response.identifiers["ClientInternal"],
            "LUID": response.lusid_instrument_id}
        ])
        # end::get-instrument[]
        self.write_to_test_output(instrument_df, "get_instrument.csv")
        self.assertEqual(instrument_df.shape[0], 1)
        self.assertEqual(instrument_df["Instrument"].values[0], "Apple_Nasdaq_AAPL")

        # tag::get-instrument-client-internal[]
        response = instruments_api.get_instrument(identifier_type='ClientInternal', identifier='4ce0ee48')

        instrument_df = pd.DataFrame([{
            "Figi": response.identifiers["Figi"],
            "Instrument": response.name,
            "ClientInternal": response.identifiers["ClientInternal"],
            "LUID": response.lusid_instrument_id}
        ])
        # end::get-instrument-client-internal[]
        self.write_to_test_output(instrument_df.head(10), "get_instrument_client_internal.csv")
        self.assertEqual(instrument_df.shape[0], 1)
        self.assertEqual(instrument_df["Instrument"].values[0], "Apple_Nasdaq_AAPL")

        # tag::get-instruments[]
        response = instruments_api.get_instruments(
            identifier_type='Figi',
            request_body=['BBG000BVPXP1', 'BBG00ZGF7HS6'])

        instruments_df = pd.DataFrame([{
            "Figi": instrument.identifiers["Figi"],
            "Instrument": instrument.name,
            "ClientInternal": instrument.identifiers["ClientInternal"],
            "LUID": instrument.lusid_instrument_id}
            for _, instrument in response.values.items()
        ])
        # end::get-instruments[]
        self.write_to_test_output(instruments_df, "get_instruments.csv")
        self.assertEqual(instruments_df.shape[0], 2)
        self.assertCountEqual(instruments_df["Instrument"].values, ["Amazon_Nasdaq_AMZN", "CoinBase_Nasdaq_COIN"])

        # tag::create-property[]
        properties_scope = f"custom_properties_{uuid.uuid4()}"
        property_request = lusid.models.CreatePropertyDefinitionRequest(
            domain='Instrument',
            scope=properties_scope,
            code='asset_class',
            value_required=True,
            display_name='Asset Class',
            life_time='TimeVariant',
            data_type_id=lusid.models.ResourceId(scope='system', code='string'))

        response = property_definitions_api.create_property_definition(
            create_property_definition_request=property_request)
        asset_class_property_key = response.key
        # end::create-property[]
        self.assertIsNotNone(asset_class_property_key)

        # tag::upsert-properties[]
        requests = []
        for row in instruments.iterrows():
            instrument = row[1]
            asset_class_property = lusid.models.ModelProperty(
                key=asset_class_property_key,
                value=lusid.models.PropertyValue(label_value=instrument['asset_class'])
            )
            requests.append(lusid.models.UpsertInstrumentPropertyRequest(
                identifier_type='Figi',
                identifier=instrument['figi'],
                properties=[asset_class_property]))

        instruments_api.upsert_instruments_properties(upsert_instrument_property_request=requests)
        # end::upsert-properties[]

        # tag::get-instrument-properties[]
        response = instruments_api.get_instruments(
            identifier_type='Figi',
            request_body=['BBG000BVPXP1', 'BBG000DQQNJ8'],
            property_keys=[asset_class_property_key])

        instrument_properties_df = pd.DataFrame([{
            "Figi": instrument.identifiers["Figi"],
            "Instrument": instrument.name,
            "ClientInternal": instrument.identifiers["ClientInternal"],
            "LUID": instrument.lusid_instrument_id,
            "Asset Class": instrument.properties[0].value.label_value}
            for _, instrument in response.values.items()])
        # end::get-instrument-properties[]
        self.write_to_test_output(instrument_properties_df, "get_instruments_properties.csv")
        self.assertCountEqual(instrument_properties_df["Asset Class"].values, ["equity", "govt"])

        # tag::search-instrument[]
        search_api = api_factory.build(lusid.api.SearchApi)

        search_request = lusid.models.InstrumentSearchProperty(key=asset_class_property_key, value='equity')
        response = search_api.instruments_search(instrument_search_property=[search_request], mastered_only=True)

        search_instruments_df = pd.DataFrame([{
            "Figi": instrument.identifiers["Figi"].value,
            "Instrument": instrument.name,
            "ClientInternal": instrument.identifiers["ClientInternal"].value,
            "LUID": instrument.identifiers['LusidInstrumentId'].value}
            for instrument in response[0].mastered_instruments])
        # end::search-instrument[]
        self.write_to_test_output(search_instruments_df, "search_instruments.csv")
        self.assertEqual(search_instruments_df.shape[0], 3)

        # tag::update-instrument-identifier[]
        instruments_api.update_instrument_identifier(
            identifier_type='Figi',
            identifier='BBG000BVPXP1',
            update_instrument_identifier_request=(lusid.models.UpdateInstrumentIdentifierRequest(
                type='ClientInternal', value='5deae335',
                effective_at=pytz.UTC.localize(parse("2021-05-06")).isoformat()
            )))
        # end::update-instrument-identifier[]

        # tag::get-instruments-now[]
        response = instruments_api.get_instruments(
            identifier_type='Figi',
            effective_at=pytz.UTC.localize(parse("2021-05-05")).isoformat(),
            request_body=['BBG000BVPXP1'])

        instruments_df_now = pd.DataFrame([{
            "Figi": instrument.identifiers["Figi"],
            "Instrument": instrument.name,
            "ClientInternal": instrument.identifiers["ClientInternal"],
            "LUID": instrument.lusid_instrument_id}
            for _, instrument in response.values.items()
        ])
        # end::get-instruments-now[]
        self.write_to_test_output(instruments_df_now, "get_instruments_now.csv")
        self.assertEqual(instruments_df_now["ClientInternal"].values[0], "21536181")

        # tag::get-instruments-later[]
        response = instruments_api.get_instruments(
            identifier_type='Figi',
            effective_at=pytz.UTC.localize(parse("2021-05-06")).isoformat(),
            request_body=['BBG000BVPXP1'])

        instruments_df_later = pd.DataFrame([{
            "Figi": instrument.identifiers["Figi"],
            "Instrument": instrument.name,
            "ClientInternal": instrument.identifiers["ClientInternal"],
            "LUID": instrument.lusid_instrument_id}
            for _, instrument in response.values.items()
        ])
        # end::get-instruments-later[]
        self.write_to_test_output(instruments_df_later, "get_instruments_later.csv")
        self.assertEqual(instruments_df_later["ClientInternal"].values[0], "5deae335")

        # tag::delete-instrument-properties[]
        instruments_api.delete_instrument_properties(
            identifier_type='Figi',
            identifier="BBG000BVPXP1",
            request_body=[asset_class_property_key],
            effective_at=datetime.now(pytz.UTC).isoformat()
        )
        # end::delete-instrument-properties[]

        ##################
        # CREATE PORTFOLIO
        ##################

        # tag::scope[]
        scope = "Developer-IBOR-Tutorial"
        # end::scope[]
        initial_scope = scope
        now = datetime.now().strftime('%Y-%m-%d-%H_%M_%S')
        scope = f"Developer-IBOR-Tutorial-{now}"

        # tag::portfolio-code[]
        portfolio_code = "Developer-IBOR-Tutorial"
        # end::portfolio-code[]
        initial_portfolio_code = portfolio_code

        print("Scope", scope)
        print("Portfolio Code", portfolio_code)

        # tag::create-portfolio-api[]
        transaction_portfolios_api = api_factory.build(lusid.api.TransactionPortfoliosApi)
        # end::create-portfolio-api[]

        # tag::create-portfolio[]
        created_date = datetime(year=2019, month=1, day=1, tzinfo=pytz.UTC).isoformat()
        transaction_portfolios_api.create_portfolio(
            scope=scope,
            create_transaction_portfolio_request=lusid.models.CreateTransactionPortfolioRequest(
                display_name="Developer IBOR Tutorial",
                code=portfolio_code,
                created=created_date,
                base_currency="USD"))
        # end::create-portfolio[]
        self.assertIsNotNone(portfolio_code)

        # tag::create-portfolio-property[]
        response = property_definitions_api.create_property_definition(
            create_property_definition_request=lusid.models.CreatePropertyDefinitionRequest(
                domain="Portfolio",
                scope=scope,
                code="portfolio_manager_name",
                value_required=False,
                display_name="portfolio_manager_name",
                data_type_id=lusid.models.ResourceId(scope="system", code="string")
            )
        )
        portfolio_manager_property = response.key
        # end::create-portfolio-property[]
        self.assertIsNotNone(portfolio_manager_property)

        # tag::update-portfolio[]
        portfolios_api.upsert_portfolio_properties(
            scope=scope,
            code=portfolio_code,
            request_body={
                portfolio_manager_property: lusid.models.ModelProperty(
                    key=portfolio_manager_property,
                    value=lusid.models.PropertyValue(label_value="Matt Smith")
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
            "Base Currency": portfolio.base_currency,
            "Manager Name": portfolio.properties[portfolio_manager_property].value.label_value
        }])
        # end::get-portfolio[]
        portfolio_df.loc[:, "Code"] = initial_portfolio_code
        self.write_to_test_output(portfolio_df, "get_portfolio.csv")
        self.assertEqual(portfolio.properties[portfolio_manager_property].value.label_value, "Matt Smith")

        # tag::new-portfolio-code[]
        new_portfolio_code = "Developer-IBOR-With-Manager-Tutorial"
        # end::new-portfolio-code[]
        initial_new_portfolio_code = new_portfolio_code
        new_portfolio_code = f"Developer-IBOR-With-Manager-Tutorial-{now}"

        # tag::create-portfolio-with-manager[]
        transaction_portfolios_api.create_portfolio(
            scope=scope,
            create_transaction_portfolio_request=lusid.models.CreateTransactionPortfolioRequest(
                display_name="Developer IBOR With Manager Tutorial",
                code=new_portfolio_code,
                created=created_date,
                base_currency="USD",
                properties={
                    portfolio_manager_property: lusid.models.ModelProperty(
                        key=portfolio_manager_property,
                        value=lusid.models.PropertyValue(label_value="David Jones"))
                }
            )
        )
        # end::create-portfolio-with-manager[]

        # tag::sub-holding-key-property[]
        code = "strategy"
        # end::sub-holding-key-property[]

        # tag::create-sub-holding-key-property[]
        response = property_definitions_api.create_property_definition(
            create_property_definition_request=lusid.models.CreatePropertyDefinitionRequest(
                domain="Transaction",
                scope=scope,
                code=code,
                display_name="Investment strategy",
                data_type_id=lusid.ResourceId(scope="system", code="string"),
            ))
        # end::create-sub-holding-key-property[]
        print(response.key)
        self.assertIsNotNone(response.key)

        # tag::portfolio-code-shk[]
        portfolio_code_with_shk = "Developer-IBOR-SHK-Tutorial"
        # end::portfolio-code-shk[]

        # tag::portfolio-with-shk-property-key[]
        strategy_property_key = f"Transaction/{scope}/{code}"
        # end::portfolio-with-shk-property-key[]

        # tag::create-portfolio-with-shk[]
        created_date = datetime(year=2019, month=1, day=1, tzinfo=pytz.UTC).isoformat()

        response = transaction_portfolios_api.create_portfolio(
            scope=scope,
            create_transaction_portfolio_request=lusid.models.CreateTransactionPortfolioRequest(
                display_name="Developer IBOR SHK Tutorial",
                code=portfolio_code_with_shk,
                created=created_date,
                sub_holding_keys=[strategy_property_key], # <1>
                base_currency="USD"))
        # end::create-portfolio-with-shk[]
        print(response.id.code)
        self.assertIsNotNone(response.id.code)

        # tag::get-updated-portfolio[]
        portfolio = portfolios_api.get_portfolio(
            scope=scope,
            code=new_portfolio_code,
            property_keys=[portfolio_manager_property]
        )
        portfolio_df = pd.DataFrame([{
            "Code": portfolio.id.code,
            "Name": portfolio.display_name,
            "Base Currency": portfolio.base_currency,
            "Manager Name": portfolio.properties[portfolio_manager_property].value.label_value,
        }])
        # end::get-updated-portfolio[]
        portfolio_df.loc[:, "Code"] = initial_new_portfolio_code
        self.write_to_test_output(portfolio_df, "get_new_portfolio.csv")
        self.assertEqual(portfolio.properties[portfolio_manager_property].value.label_value, "David Jones")

        ##################
        # TRANSACTIONS
        ##################

        # tag::transactions-file[]
        transactions_file = "data/test_ibor/transactions.csv"
        # end::transactions-file[]
        transactions_file = Path(__file__).parent.joinpath(transactions_file)

        # tag::load-transactions[]
        transactions = pd.read_csv(transactions_file)
        # end::load-transactions[]
        self.write_to_test_output(transactions, "transactions.csv")

        # tag::import-transactions[]
        transactions_request = []
        for row, txn in transactions.iterrows():
            if txn["figi"] == "cash":
                instrument_identifier = {"Instrument/default/Currency": txn["currency"]}
            else:
                instrument_identifier = {"Instrument/default/Figi": txn["figi"]}

            transactions_request.append(
                lusid.models.TransactionRequest(
                    transaction_id=txn["txn_id"],
                    type=txn["tx_type"],
                    instrument_identifiers=instrument_identifier,
                    transaction_date=pytz.UTC.localize(parse(txn["date"])).isoformat(),
                    settlement_date=pytz.UTC.localize(parse(txn["date"])).isoformat(),
                    units=txn["quantity"],
                    transaction_price=lusid.models.TransactionPrice(price=txn["price"], type="Price"),
                    total_consideration=lusid.models.CurrencyAndAmount(
                        amount=txn["net_money"], currency=txn["currency"]),
                    ))

        transaction_portfolios_api.upsert_transactions(
            scope=scope, code=portfolio_code, transaction_request=transactions_request)
        # end::import-transactions[]

        # tag::import-transactions-shk[]
        transactions_request = []
        for row, txn in transactions.iterrows():
            if txn["figi"] == "cash":
                instrument_identifier = {"Instrument/default/Currency": txn["currency"]}
            else:
                instrument_identifier = {"Instrument/default/Figi": txn["figi"]}

            transactions_request.append(
                lusid.models.TransactionRequest(
                    transaction_id=txn["txn_id"],
                    type=txn["tx_type"],
                    instrument_identifiers=instrument_identifier,
                    transaction_date=pytz.UTC.localize(parse(txn["date"])).isoformat(),
                    settlement_date=pytz.UTC.localize(parse(txn["date"])).isoformat(),
                    units=txn["quantity"],
                    transaction_price=lusid.models.TransactionPrice(price=txn["price"], type="Price"),
                    total_consideration=lusid.models.CurrencyAndAmount(
                        amount=txn["net_money"], currency=txn["currency"]),
                    properties={  # <1>
                        strategy_property_key: lusid.PerpetualProperty(
                            key=strategy_property_key,
                            value=lusid.PropertyValue(label_value=txn["strategy"]))
                    }
                ))

        transaction_portfolios_api.upsert_transactions(
            scope=scope, code=portfolio_code_with_shk, transaction_request=transactions_request)
        # end::import-transactions-shk[]

        # tag::format-transactions[]
        def display_transactions_summary(response):
            return pd.DataFrame([{
                "Transaction ID": value.transaction_id,
                "Instrument": value.properties["Instrument/default/Name"].value.label_value,
                "Amount": value.total_consideration.amount,
                "Units": value.units,
                "Type": value.type,
            } for value in response.values])

        # end::format-transactions[]

        # tag::get-transactions[]
        response = transaction_portfolios_api.get_transactions(
            scope=scope,
            code=portfolio_code,
            property_keys=["Instrument/default/Name"])
        tx_response = display_transactions_summary(response)
        # end::get-transactions[]
        self.write_to_test_output(tx_response, "transactions_response.csv")

        # tag::get-transactions-filter[]
        response = transaction_portfolios_api.get_transactions(
            scope=scope,
            code=portfolio_code,
            property_keys=["Instrument/default/Name"],
            filter="type eq 'Buy'")
        tx_response = display_transactions_summary(response)
        # end::get-transactions-filter[]
        self.write_to_test_output(tx_response, "transactions_filter_response.csv")

        ##################
        # HOLDINGS
        ##################

        # tag::format-holdings[]
        def display_holdings_summary(response):
            return pd.DataFrame([{
                "Instrument": value.properties["Instrument/default/Name"].value.label_value,
                "Amount": value.cost.amount,
                "Units": value.units,
                "Type": value.holding_type
            } for value in response.values])
        # end::format-holdings[]

        holding_response = hold.funds_loaded(transaction_portfolios_api, scope, portfolio_code)
        # tag::get-holdings-funds-loaded[]
        holdings = display_holdings_summary(holding_response)
        # end::get-holdings-funds-loaded[]
        self.write_to_test_output(holdings, "holdings_funds_loaded.csv")
        self.assertEqual(holdings.shape[0], 1)

        holding_response = hold.day1_trading(transaction_portfolios_api, scope, portfolio_code)
        # tag::get-holdings-first-day-trading[]
        holdings = display_holdings_summary(holding_response)
        # end::get-holdings-first-day-trading[]
        self.write_to_test_output(holdings, "holdings_first_day_trading.csv")
        self.assertEqual(holdings.shape[0], 2)

        holding_response = hold.day2_trading(transaction_portfolios_api, scope, portfolio_code)
        # tag::get-holdings-second-day-trading[]
        holdings = display_holdings_summary(holding_response)
        # end::get-holdings-second-day-trading[]
        self.write_to_test_output(holdings, "holdings_second_day_trading.csv")
        self.assertEqual(holdings.shape[0], 4)

        holding_response = hold.latest_day_trading(transaction_portfolios_api, scope, portfolio_code)
        # tag::get-holdings-today[]
        holdings = display_holdings_summary(holding_response)
        # end::get-holdings-today[]
        self.write_to_test_output(holdings, "holdings_today.csv")
        self.assertEqual(holdings.shape[0], 4)

        holding_response = hold.positions(transaction_portfolios_api, scope, portfolio_code)
        # tag::get-holdings-positions[]
        holdings = display_holdings_summary(holding_response)
        # end::get-holdings-positions[]
        self.write_to_test_output(holdings, "holdings_positions.csv")
        self.assertEqual(holdings.shape[0], 3)
        self.assertAlmostEqual(holdings[holdings["Instrument"] == "Amazon_Nasdaq_AMZN"]["Units"].values[0], 100.0, 3)

        # tag::format-holdings-shk[]
        def display_holdings_shk_summary(response):
            return pd.DataFrame([{
                "Instrument": value.properties["Instrument/default/Name"].value.label_value,
                "SHK": list(value.sub_holding_keys.values())[0].value.label_value,
                "Amount": value.cost.amount,
                "Units": value.units,
                "Type": value.holding_type
            } for value in response.values])
        # end::format-holdings-shk[]

        holding_response = hold.funds_loaded(transaction_portfolios_api, scope, portfolio_code_with_shk)
        # tag::get-holdings-funds-loaded-shk[]
        holdings = display_holdings_shk_summary(holding_response)
        # end::get-holdings-funds-loaded-shk[]
        self.write_to_test_output(holdings, "holdings_funds_loaded_shk.csv")
        self.assertEqual(holdings.shape[0], 2)

        holding_response = hold.day1_trading(transaction_portfolios_api, scope, portfolio_code_with_shk)
        # tag::get-holdings-first-day-trading-shk[]
        holdings = display_holdings_shk_summary(holding_response)
        # end::get-holdings-first-day-trading-shk[]
        self.write_to_test_output(holdings, "holdings_first_day_trading_shk.csv")
        self.assertEqual(holdings.shape[0], 4)

        holding_response = hold.day2_trading(transaction_portfolios_api, scope, portfolio_code_with_shk)
        # tag::get-holdings-second-day-trading-shk[]
        holdings = display_holdings_shk_summary(holding_response)
        # end::get-holdings-second-day-trading-shk[]
        self.write_to_test_output(holdings, "holdings_second_day_trading_shk.csv")
        self.assertEqual(holdings.shape[0], 8)

        holding_response = hold.latest_day_trading(transaction_portfolios_api, scope, portfolio_code_with_shk)
        # tag::get-holdings-today-shk[]
        holdings = display_holdings_shk_summary(holding_response)
        # end::get-holdings-today-shk[]
        self.write_to_test_output(holdings, "holdings_today_shk.csv")
        self.assertEqual(holdings.shape[0], 8)

        holding_response = hold.positions(transaction_portfolios_api, scope, portfolio_code_with_shk)
        # tag::get-holdings-positions-shk[]
        holdings = display_holdings_shk_summary(holding_response)
        # end::get-holdings-positions-shk[]
        self.write_to_test_output(holdings, "holdings_positions_shk.csv")
        self.assertEqual(holdings.shape[0], 6)
        amazon = holdings[(holdings["Instrument"] == "Amazon_Nasdaq_AMZN") & (holdings["SHK"] == "tech")]
        self.assertAlmostEqual(amazon["Units"].values[0], 75.0, 3)

        # Bit of hackery so that we use portfolio_code for both portfolios in the docs
        # tag::portfolio-code-shk-override[]
        portfolio_code = "Developer-IBOR-SHK-Tutorial"
        # end::portfolio-code-shk-override[]
        portfolio_code = initial_portfolio_code

        ##################
        # QUOTES
        ##################

        # tag::quotes-file[]
        quotes_file = "data/test_ibor/quotes.csv"
        # end::quotes-file[]
        quotes_file = Path(__file__).parent.joinpath(quotes_file)

        # tag::load-quotes[]
        quotes = pd.read_csv(quotes_file)
        # end::load-quotes[]
        self.write_to_test_output(quotes, "quotes.csv")

        # tag::load-quotes-get-luid[]
        response = instruments_api.get_instruments(
            identifier_type='Figi',
            request_body=quotes["figi"].values.tolist())
        instruments = pd.DataFrame([{
            "figi": instrument.identifiers["Figi"],
            "luid": instrument.lusid_instrument_id}
            for _, instrument in response.values.items()
        ])
        quotes_with_luid = pd.merge(quotes, instruments, on=["figi"])

        # end::load-quotes-get-luid[]
        self.write_to_test_output(quotes_with_luid, "quotes_with_luid.csv")
        self.assertIsNotNone(quotes_with_luid["luid"][0])

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

        # end::import-quotes[]

        # tag::compute-valuation[]
        def compute_valuation_with_default_recipe(from_date, to_date, metrics, group_by):
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

        # tag::get-valuation-all[]
        metrics = [
            ("Analytic/default/ValuationDate", "Value"),
            ("Holding/default/PV", "Sum"),
        ]
        group_by = ["Analytic/default/ValuationDate"]
        # end::get-valuation-all[]

        # tag::get-valuation-total[]
        effective_at = datetime(year=2021, month=4, day=21, tzinfo=pytz.UTC)
        response = compute_valuation_with_default_recipe(effective_at, effective_at, metrics, group_by)
        valuation_all = pd.DataFrame(response)
        # end::get-valuation-total[]
        self.write_to_test_output(valuation_all, "valuation-all.csv")
        self.assertAlmostEqual(valuation_all["Sum(Holding/default/PV)"].values[0], 1156135, 3)

        # tag::get-valuation-total-multiple-days[]
        date_from = datetime(year=2021, month=4, day=21, tzinfo=pytz.UTC)
        date_to = datetime(year=2021, month=4, day=23, tzinfo=pytz.UTC)
        response = compute_valuation_with_default_recipe(date_from, date_to, metrics, group_by)
        valuation_multiple_days = pd.DataFrame(response).sort_values(["Analytic/default/ValuationDate"])
        # end::get-valuation-total-multiple-days[]
        self.write_to_test_output(valuation_multiple_days, "valuation-all-multiple-days.csv")
        self.assertAlmostEqual(valuation_multiple_days["Sum(Holding/default/PV)"].values[1], 1141134.0, 3)

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
        response = compute_valuation_with_default_recipe(effective_at, effective_at, metrics, group_by)
        valuation = pd.DataFrame(response)
        # end::get-valuation-20210421[]
        self.write_to_test_output(valuation, "valuation-20210421.csv")
        self.assertAlmostEqual(valuation["Proportion(Holding/default/PV)"][0], 0.53966, 3)

        # tag::get-valuation-20210422[]
        effective_at = datetime(year=2021, month=4, day=22, tzinfo=pytz.UTC)
        response = compute_valuation_with_default_recipe(effective_at, effective_at, metrics, group_by)
        valuation = pd.DataFrame(response)
        # end::get-valuation-20210422[]
        self.write_to_test_output(valuation, "valuation-20210422.csv")
        self.assertAlmostEqual(valuation["Proportion(Holding/default/PV)"][0], 0.5467, 3)

        portfolio_code = portfolio_code_with_shk
        # tag::get-valuation-total-shk[]
        metrics = [
            (strategy_property_key, "Value"),
            ("Holding/default/PV", "Sum"),
            ("Holding/default/PV", "Proportion")
        ]
        group_by = [strategy_property_key]

        effective_at = datetime(year=2021, month=4, day=21, tzinfo=pytz.UTC)
        response = compute_valuation_with_default_recipe(effective_at, effective_at, metrics, group_by)
        valuation = pd.DataFrame(response)
        # end::get-valuation-total-shk[]
        valuation = valuation.rename({strategy_property_key: f"Transaction/{initial_scope}/{code}"}, axis=1)
        self.write_to_test_output(valuation, "valuation-shk.csv")

        portfolio_code = initial_portfolio_code

        # tag::corporate-action-source-code[]
        corporate_action_source_code = "Corporate-Actions-Source"
        # end::corporate-action-source-code[]

        # tag::corporate-action-source-api[]
        corporate_action_sources_api = api_factory.build(lusid.api.CorporateActionSourcesApi)
        # end::corporate-action-source-api[]

        # tag::create-corporate-action-source[]
        source_request = lusid.models.CreateCorporateActionSourceRequest(
            scope=scope,
            code=corporate_action_source_code,
            display_name="Corporate Actions",
        )
        corporate_action_sources_api.create_corporate_action_source(
            create_corporate_action_source_request=source_request
        )
        # end::create-corporate-action-source[]

        # tag::corporate-action-created-at[]
        created_date = datetime(year=2019, month=1, day=1, tzinfo=pytz.UTC).isoformat()
        # end::corporate-action-created-at[]

        # tag::add-corporate-action-portfolio[]
        transaction_portfolios_api.upsert_portfolio_details(
            scope=scope,
            code=portfolio_code,
            effective_at=created_date,
            create_portfolio_details=lusid.models.CreatePortfolioDetails(
                corporate_action_source_id=lusid.ResourceId(
                    scope=scope,
                    code=corporate_action_source_code)
            ),
        )
        # end::add-corporate-action-portfolio[]

        # tag::create-transition[]
        request = lusid.models.CorporateActionTransitionRequest(
            input_transition=lusid.models.CorporateActionTransitionComponentRequest(
                instrument_identifiers={
                    "Instrument/default/Figi": "BBG00ZGF7HS6"
                },
                units_factor=1,
                cost_factor=1,
            ),
            output_transitions=[lusid.models.CorporateActionTransitionComponentRequest(
                instrument_identifiers={
                    "Instrument/default/Currency": "USD"
                },
                units_factor=10,
                cost_factor=1,
            )]
        )
        # end::create-transition[]

        # tag::upsert-transition[]
        announcement_date = datetime(year=2020, month=2, day=1, tzinfo=pytz.UTC).isoformat()
        ex_date = datetime(year=2020, month=2, day=2, tzinfo=pytz.UTC).isoformat()
        record_date = datetime(year=2020, month=2, day=3, tzinfo=pytz.UTC).isoformat()
        payment_date = datetime(year=2020, month=2, day=4, tzinfo=pytz.UTC).isoformat()

        dividend_coinbase = lusid.models.UpsertCorporateActionRequest(
                corporate_action_code="ca001",
                announcement_date=announcement_date,
                ex_date=ex_date,
                record_date=record_date,
                payment_date=payment_date,
                transitions=[request],
            )

        corporate_action_sources_api.batch_upsert_corporate_actions(
            scope=scope, code=corporate_action_source_code,
            upsert_corporate_action_request=[dividend_coinbase]
        )
        # end::upsert-transition[]

        holding_response = transaction_portfolios_api.get_holdings(
            scope=scope,
            code=portfolio_code,
            property_keys=["Instrument/default/Name"],
            effective_at=datetime(year=2020, month=1, day=4, hour=1, tzinfo=pytz.UTC),
        )
        print(display_holdings_summary(holding_response))

        holding_response = transaction_portfolios_api.get_holdings(
            scope=scope,
            code=portfolio_code,
            property_keys=["Instrument/default/Name"],
            effective_at=datetime(year=2020, month=2, day=5, hour=1, tzinfo=pytz.UTC),
        )
        print(display_holdings_summary(holding_response))

        # Explicitly set holdings

        # tag::holdings-file[]
        holdings_file = "data/test_ibor/holdings.csv"
        # end::holdings-file[]
        holdings_file = Path(__file__).parent.joinpath(holdings_file)

        # tag::load-holdings[]
        holdings = pd.read_csv(holdings_file)
        # end::load-holdings[]
        self.write_to_test_output(holdings, "holdings.csv")

        # tag::set-holdings-api[]
        transaction_portfolios_api = api_factory.build(lusid.api.TransactionPortfoliosApi)
        # end::set-holdings-api[]

        # tag::adjust-holdings[]
        holdings_adjustments = [
            lusid.models.HoldingAdjustment(
                instrument_identifiers={"Instrument/default/Figi": holding["figi"]},
                instrument_uid=holding["figi"],
                sub_holding_keys={
                    strategy_property_key: lusid.PerpetualProperty(
                        key=strategy_property_key,
                        value=lusid.PropertyValue(label_value=holding["strategy"]))
                },
                tax_lots=[lusid.models.TargetTaxLot(units=holding["units"])])
            for _, holding in holdings.iterrows()
        ]
        transaction_portfolios_api.adjust_holdings(
            scope=scope,
            code=portfolio_code,
            effective_at=datetime(2020, 1, 1, tzinfo=pytz.UTC),
            adjust_holding_request=holdings_adjustments
        )
        # end::adjust-holdings[]

        # tag::set-holdings[]
        holdings_adjustments = [
            lusid.models.HoldingAdjustment(
                instrument_identifiers={"Instrument/default/Figi": holding["figi"]},
                instrument_uid=holding["figi"],
                sub_holding_keys={
                    strategy_property_key: lusid.PerpetualProperty(
                        key=strategy_property_key,
                        value=lusid.PropertyValue(label_value=holding["strategy"]))
                },
                tax_lots=[lusid.models.TargetTaxLot(units=holding["units"])])
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

        # tag::delete-instruments[]
        for figi in instruments.loc[:, 'figi'].values:
            instruments_api.delete_instrument(identifier_type='Figi', identifier=figi)
        # end::delete-instruments[]

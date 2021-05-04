import unittest
import uuid
from pathlib import Path
import tests.integration.examples.lusid_utils as lusid_utils

# tag::imports[]
import lusid
from lusid import models

import pytz
import pandas as pd
from datetime import datetime, timedelta
from dateutil.parser import parse
# end::imports[]

from itertools import groupby


class IBOR(unittest.TestCase):
    def write_to_test_output(self, df, file_name):
        df.to_csv(Path(__file__).parent.joinpath(f"data/test_ibor/test_output/{file_name}"), index=False)

    def test_ibor(self) -> None:
        api_factory = lusid_utils.api_factory

        # tag::apis[]
        instruments_api = api_factory.build(lusid.api.InstrumentsApi)
        quotes_api = api_factory.build(lusid.api.QuotesApi)
        aggregation_api = api_factory.build(lusid.api.AggregationApi)
        # end::apis[]
        portfolios_api = api_factory.build(lusid.api.PortfoliosApi)

        # tag::instruments-file[]
        instruments_file = "data/test_ibor/instruments.csv"
        # end::instruments-file[]
        instruments_file = Path(__file__).parent.joinpath(instruments_file)

        # tag::load-instruments[]
        instruments = pd.read_csv(instruments_file)
        # end::load-instruments[]
        self.write_to_test_output(instruments, "instruments.csv")

        # tag::instruments-api[]
        instruments_api = api_factory.build(lusid.api.InstrumentsApi)
        # end::instruments-api[]

        # tag::identifiers[]
        response = instruments_api.get_instrument_identifier_types()
        identifiers = pd.DataFrame(response.to_dict()["values"])
        unique_identifiers = identifiers.loc[identifiers["is_unique_identifier_type"]]
        unique_identifiers = unique_identifiers.drop(["is_unique_identifier_type"], axis=1)
        # end::identifiers[]

        self.write_to_test_output(unique_identifiers, "identifiers.csv")
        self.assertGreater(unique_identifiers.shape[0], 0)

        # tag::import-instruments[]
        identifier_columns = [('figi', 'Figi'), ('client_internal', 'ClientInternal')]
        definitions = {}
        for _, instrument in instruments.iterrows():
            identifiers = {
                identifier[1]: models.InstrumentIdValue(value=instrument[identifier[0]])
                for identifier in identifier_columns
            }
            definitions[instrument['instrument_name']] = models.InstrumentDefinition(
                name=instrument['instrument_name'], identifiers=identifiers)

        response = instruments_api.upsert_instruments(request_body=definitions)
        items = response.values.items()
        figi_to_luid = {
            instrument.identifiers["Figi"]: instrument.lusid_instrument_id
            for _, instrument in items
        }

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
        response = instruments_api.get_instrument(identifier_type='Figi', identifier='BBG000BVPXP1')
        instrument_df = pd.DataFrame([{
            "Figi": response.identifiers["Figi"],
            "Instrument": response.name,
            "ClientInternal": response.identifiers["ClientInternal"],
            "LUID": response.lusid_instrument_id}
        ])
        # end::get-instrument[]
        self.write_to_test_output(instrument_df, "get_instrument.csv")
        self.assertEqual(instrument_df.shape[0], 1)
        self.assertEqual(instrument_df["Instrument"].values[0], "Amazon_Nasdaq_AMZN")

        # tag::get-instrument-client-internal[]
        response = instruments_api.get_instrument(identifier_type='ClientInternal', identifier='21536181')

        instrument_df = pd.DataFrame([{
            "Figi": response.identifiers["Figi"],
            "Instrument": response.name,
            "ClientInternal": response.identifiers["ClientInternal"],
            "LUID": response.lusid_instrument_id}
        ])
        # end::get-instrument-client-internal[]
        self.write_to_test_output(instrument_df.head(10), "get_instrument_client_internal.csv")
        self.assertEqual(instrument_df.shape[0], 1)
        self.assertEqual(instrument_df["Instrument"].values[0], "Amazon_Nasdaq_AMZN")

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
        property_definitions_api = api_factory.build(lusid.api.PropertyDefinitionsApi)

        properties_scope = f"custom_properties_{uuid.uuid4()}"
        property_request = models.CreatePropertyDefinitionRequest(
            domain='Instrument',
            scope=properties_scope,
            code='asset_class',
            value_required=True,
            display_name='asset_class',
            life_time='TimeVariant',
            data_type_id=models.ResourceId(scope='system', code='string'))

        response = property_definitions_api.create_property_definition(
            create_property_definition_request=property_request)
        asset_class_property_key = response.key
        # end::create-property[]
        self.assertIsNotNone(asset_class_property_key)

        # tag::upsert-properties[]
        requests = []
        for row in instruments.iterrows():
            instrument = row[1]
            asset_class_property = models.ModelProperty(
                key=asset_class_property_key,
                value=models.PropertyValue(label_value=instrument['asset_class'])
            )
            requests.append(models.UpsertInstrumentPropertyRequest(
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

        search_request = models.InstrumentSearchProperty(key=asset_class_property_key, value='equity')
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
            update_instrument_identifier_request=(models.UpdateInstrumentIdentifierRequest(
                type='ClientInternal', value='5deae335',
                effective_at=(datetime.now(pytz.UTC) + timedelta(minutes=10)).isoformat()
            )))
        # end::update-instrument-identifier[]

        # tag::get-instruments-now[]
        response = instruments_api.get_instruments(
            identifier_type='Figi',
            effective_at=datetime.now(pytz.UTC).isoformat(),
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
            effective_at=(datetime.now(pytz.UTC) + timedelta(minutes=11)).isoformat(),
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

        # tag::scope-portfolio-code[]
        scope = portfolio_code = "Developer-IBOR-Tutorial"
        # end::scope-portfolio-code[]
        now = datetime.now().strftime('%Y-%m-%d-%H_%M_%S')
        scope = portfolio_code = f"Developer-IBOR-Tutorial-{now}"

        # tag::create-portfolio-api[]
        transaction_portfolios_api = api_factory.build(lusid.api.TransactionPortfoliosApi)
        # end::create-portfolio-api[]

        # tag::create-portfolio[]
        created_date = datetime(year=2019, month=1, day=1, tzinfo=pytz.UTC).isoformat()
        transaction_portfolios_api.create_portfolio(
            scope=scope,
            create_transaction_portfolio_request=models.CreateTransactionPortfolioRequest(
                display_name="Developer IBOR Tutorial",
                code=portfolio_code,
                created=created_date,
                base_currency="USD"))
        # end::create-portfolio[]
        self.assertIsNotNone(portfolio_code)

        # tag::holdings-file[]
        quotes_file = "data/test_ibor/holdings.csv"
        # end::holdings-file[]
        quotes_file = Path(__file__).parent.joinpath(quotes_file)

        # tag::load-holdings[]
        holdings = pd.read_csv(quotes_file)
        # end::load-holdings[]
        self.write_to_test_output(holdings, "holdings.csv")

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
        quotes_file = "data/test_ibor/quotes.csv"
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
        def compute_valuation_with_default_recipe(from_date, to_date, metrics, group_by):
            return aggregation_api.get_valuation(
                valuation_request=models.ValuationRequest(
                    recipe_id=models.ResourceId(scope=scope, code="default"),
                    metrics=[models.AggregateSpec(key, op) for key, op in metrics],
                    group_by=group_by,
                    valuation_schedule=models.ValuationSchedule(effective_from=from_date, effective_at=to_date),
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
        # end::get-valuation-all[]

        # tag::get-valuation-total[]
        effective_at = datetime(year=2021, month=4, day=21, tzinfo=pytz.UTC)
        response = compute_valuation_with_default_recipe(effective_at, effective_at, metrics, group_by)
        valuation_all = pd.DataFrame(response)
        # end::get-valuation-total[]
        self.write_to_test_output(valuation_all, "valuation-all.csv")
        self.assertAlmostEqual(valuation_all["Sum(Holding/default/PV)"].values[0], 532212.0, 3)

        # tag::get-valuation-total-multiple-days[]
        date_from = datetime(year=2021, month=4, day=21, tzinfo=pytz.UTC)
        date_to = datetime(year=2021, month=4, day=23, tzinfo=pytz.UTC)
        response = compute_valuation_with_default_recipe(date_from, date_to, metrics, group_by)
        valuation_multiple_days = pd.DataFrame(response).sort_values(["Analytic/default/ValuationDate"])
        # end::get-valuation-total-multiple-days[]
        self.write_to_test_output(valuation_multiple_days, "valuation-all-multiple-days.csv")
        self.assertAlmostEqual(valuation_multiple_days["Sum(Holding/default/PV)"].values[0], 532212.0, 3)

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
        self.assertAlmostEqual(valuation["Proportion(Holding/default/PV)"][0], 0.631707, 3)

        # tag::get-valuation-20210422[]
        effective_at = datetime(year=2021, month=4, day=22, tzinfo=pytz.UTC)
        response = compute_valuation_with_default_recipe(effective_at, effective_at, metrics, group_by)
        valuation = pd.DataFrame(response)
        # end::get-valuation-20210422[]
        self.write_to_test_output(valuation, "valuation-20210422.csv")
        self.assertAlmostEqual(valuation["Proportion(Holding/default/PV)"][0], 0.6397, 3)

        portfolios_api.delete_portfolio(scope, portfolio_code)

        # tag::delete-instruments[]
        for figi in instruments.loc[:, 'figi'].values:
            instruments_api.delete_instrument(identifier_type='Figi', identifier=figi)
        # end::delete-instruments[]

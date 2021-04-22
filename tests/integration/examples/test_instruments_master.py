import unittest
from pathlib import Path

# tag::imports[]
import lusid
from lusid import models

import pytz
import pandas as pd
import uuid
from datetime import datetime
# end::imports[]


class InstrumentsMaster(unittest.TestCase):
    def write_to_test_output(self, df, file_name):
        df.to_csv(Path(__file__).parent.joinpath(f"data/test_output/{file_name}"), index=False)

    def test_maintain_instruments_master(self) -> None:
        # tag::api-factory[]
        secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")
        api_factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=secrets_file
        )
        # end::api-factory[]

        # tag::instruments-file[]
        instruments_file = "data/instruments.csv"
        # end::instruments-file[]
        instruments_file = Path(__file__).parent.joinpath(instruments_file)

        # tag::load-instruments[]
        instruments = pd.read_csv(instruments_file)
        instruments.head(10)
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

        luids = pd.DataFrame([
            {"Figi": instrument.identifiers["Figi"],
             "Instrument": instrument.name,
             "LUID": instrument.lusid_instrument_id}
            for _, instrument in response.values.items()
        ])
        # end::import-instruments[]
        self.write_to_test_output(luids.head(10), "luids.csv")

        # tag::get-instrument[]
        response = instruments_api.get_instrument(
            identifier_type='Figi',
            identifier='BBG000C05BD1')
        instrument_df = pd.DataFrame([
            {"Figi": response.identifiers["Figi"],
             "Instrument": response.name,
             "ClientInternal": response.identifiers["ClientInternal"],
             "LUID": response.lusid_instrument_id}
        ])
        # end::get-instrument[]
        self.write_to_test_output(instrument_df.head(10), "get_instrument.csv")

        # tag::get-instrument-client-internal[]
        response = instruments_api.get_instrument(
            identifier_type='ClientInternal',
            identifier='imd_43535553')
        instrument_df = pd.DataFrame([
            {"Figi": response.identifiers["Figi"],
             "Instrument": response.name,
             "ClientInternal": response.identifiers["ClientInternal"],
             "LUID": response.lusid_instrument_id}
        ])
        # end::get-instrument-client-internal[]
        self.write_to_test_output(instrument_df.head(10), "get_instrument_client_internal.csv")

        # tag::get-instruments[]
        response = instruments_api.get_instruments(
            identifier_type='Figi',
            request_body=['BBG000C05BD1', 'BBG000DQQNJ8'])
        instruments_df = pd.DataFrame([
            {"Figi": instrument.identifiers["Figi"],
             "Instrument": instrument.name,
             "ClientInternal": instrument.identifiers["ClientInternal"],
             "LUID": instrument.lusid_instrument_id}
            for _, instrument in response.values.items()
        ])
        # end::get-instruments[]
        self.write_to_test_output(instruments_df.head(10), "get_instruments.csv")

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

        property_response = property_definitions_api.create_property_definition(
            create_property_definition_request=property_request)
        asset_class_property_key = property_response.key
        # end::create-property[]

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
            request_body=['BBG000C05BD1', 'BBG000DQQNJ8'],
            property_keys=[asset_class_property_key]
        )
        instrument_properties_df = pd.DataFrame([
            {"Figi": instrument.identifiers["Figi"],
             "Instrument": instrument.name,
             "ClientInternal": instrument.identifiers["ClientInternal"],
             "LUID": instrument.lusid_instrument_id,
             "Asset Class": instrument.properties[0].value.label_value}
            for _, instrument in response.values.items()])
        # end::get-instrument-properties[]
        self.write_to_test_output(instrument_properties_df.head(10), "get_instruments_properties.csv")

        # tag::delete-instrument-properties[]
        instruments_api.delete_instrument_properties(
            identifier_type='Figi',
            identifier="BBG000C05BD1",
            request_body=[asset_class_property_key],
            effective_at=datetime.now(pytz.UTC).isoformat()
        )
        # end::delete-instrument-properties[]

        # tag::search-instrument[]
        search_api = api_factory.build(lusid.api.SearchApi)

        search_request = models.InstrumentSearchProperty(key=asset_class_property_key, value='equity')
        response = search_api.instruments_search(instrument_search_property=[search_request], mastered_only=True)

        search_instruments_df = pd.DataFrame([
            {"Figi": instrument.identifiers["Figi"].value,
             "Instrument": instrument.name,
             "ClientInternal": instrument.identifiers["ClientInternal"].value,
             "LUID": instrument.identifiers['LusidInstrumentId'].value}
            for instrument in response[0].mastered_instruments])
        # end::search-instrument[]
        self.write_to_test_output(search_instruments_df.head(10), "search_instruments.csv")

        # tag::update-instrument-identifier[]
        request = models.UpdateInstrumentIdentifierRequest(type='ClientInternal', value='imd_43535554')
        response = instruments_api.update_instrument_identifier(
            identifier_type='Figi',
            identifier='BBG000C05BD1',
            update_instrument_identifier_request=request)
        updated_identifiers_df = pd.DataFrame([
            {"Figi": response.identifiers["Figi"],
             "Instrument": response.name,
             "ClientInternal": response.identifiers["ClientInternal"],
             "LUID": response.lusid_instrument_id}])
        # end::update-instrument-identifier[]
        self.write_to_test_output(updated_identifiers_df, "updated_identifiers.csv")

        # tag::delete-instruments[]
        for figi in instruments.loc[:, 'figi'].values:
            instruments_api.delete_instrument(identifier_type='Figi', identifier=figi)
        # end::delete-instruments[]

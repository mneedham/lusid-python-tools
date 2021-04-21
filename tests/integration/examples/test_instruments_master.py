import unittest
from pathlib import Path

import pandas as pd

import lusid
from lusid import models


class InstrumentsMaster(unittest.TestCase):
    def test_maintain_instruments_master(self) -> None:
        secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")
        api_factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=secrets_file
        )

        response = api_factory.build(lusid.api.InstrumentsApi).get_instrument_identifier_types()
        identifiers = pd.DataFrame(response.to_dict()["values"])

        identifiers.to_csv(Path(__file__).parent.joinpath("data/test_output/identifiers.csv"), index=False)

        # tag::instruments-file[]
        instruments_file = "data/instruments.csv"
        # end::instruments-file[]
        instruments_file = Path(__file__).parent.joinpath(instruments_file)

        # tag:load-instruments[]
        instruments = pd.read_csv(instruments_file)
        instruments.head(n=10)
        # end:load-instruments[]
        instruments.head(n=10).to_csv(Path(__file__).parent.joinpath("data/test_output/instruments.csv"), index=False)

        identifier_columns = [
            ('isin', 'Isin'),
            ('figi', 'Figi'),
            ('ticker', 'Ticker'),
            ('client_internal', 'ClientInternal')
        ]
        definitions = {}
        for _, instrument in instruments.iterrows():
            identifiers = {
                identifier[1]: models.InstrumentIdValue(value=instrument[identifier[0]])
                for identifier in identifier_columns
            }
            definitions[instrument['instrument_name']] = models.InstrumentDefinition(
                name=instrument['instrument_name'], identifiers=identifiers)

        response = api_factory.build(lusid.api.InstrumentsApi).upsert_instruments(request_body=definitions)

        # Print each the definition of each instrument returned in the response
        for figi, instrument in response.values.items():
            print('Figi: ' + figi + '\n',
                  'Instrument Name: ' + instrument.name + '\n',
                  'LUSID Instrument ID: ' + instrument.lusid_instrument_id + '\n' + '\n')

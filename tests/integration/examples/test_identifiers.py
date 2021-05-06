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


class Identifiers(unittest.TestCase):
    def write_to_test_output(self, df, file_name):
        df.to_csv(Path(__file__).parent.joinpath(f"data/test_identifiers/test_output/{file_name}"), index=False)

    def test_ibor(self) -> None:
        api_factory = lusid_utils.api_factory

        # tag::instruments-api[]
        instruments_api = api_factory.build(lusid.api.InstrumentsApi)
        # end::instruments-api[]

        # tag::identifiers[]
        response = instruments_api.get_instrument_identifier_types()
        identifiers = pd.DataFrame(response.to_dict()["values"])
        # end::identifiers[]

        # tag::unique-identifiers[]
        unique_identifiers = identifiers.loc[identifiers["is_unique_identifier_type"]]
        unique_identifiers = unique_identifiers.drop(["is_unique_identifier_type"], axis=1)
        # end::unique-identifiers[]

        self.write_to_test_output(unique_identifiers, "identifiers.csv")
        self.assertGreater(unique_identifiers.shape[0], 0)

        # tag::other-identifiers[]
        other_identifiers = identifiers.loc[identifiers["is_unique_identifier_type"] == False]
        other_identifiers = other_identifiers.drop(["is_unique_identifier_type"], axis=1)
        # end::other-identifiers[]

        self.write_to_test_output(other_identifiers, "other_identifiers.csv")
        self.assertGreater(other_identifiers.shape[0], 0)

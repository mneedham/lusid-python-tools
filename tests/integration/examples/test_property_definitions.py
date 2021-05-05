import unittest
from pathlib import Path

from lusid import PropertyDefinitionSearchResult

import tests.integration.examples.lusid_utils as lusid_utils

# tag::imports[]
import lusid
import pandas as pd


# end::imports[]


class PropertyDefinitions(unittest.TestCase):
    def write_to_test_output(self, df, file_name):
        df.to_csv(Path(__file__).parent.joinpath(f"data/test_property_definitions/test_output/{file_name}"),
                  index=False)

    def test_property_definitions(self) -> None:
        api_factory = lusid_utils.api_factory

        # tag::apis[]
        search_api = api_factory.build(lusid.api.SearchApi)
        # end::apis[]

        print(list(PropertyDefinitionSearchResult().attribute_map.values()))

        properties = pd.DataFrame([value.to_dict() for value in search_api.search_properties(
            filter="domain in 'Holding','Instrument','Portfolio'"
        ).values])
        self.write_to_test_output(properties, "properties.csv")

        print(pd.DataFrame([value.to_dict() for value in search_api.search_properties(
            filter="displayName eq 'portfolio_manager_name'"
        ).values]))

        # property_definitions_api = api_factory.build(lusid.api.PropertyDefinitionsApi)
        #
        # for _, property in properties.iterrows():
        #     property_definitions_api.delete_property_definition(
        #         domain = property["domain"],
        #         scope = property["scope"],
        #         code = property["code"]
        #     )

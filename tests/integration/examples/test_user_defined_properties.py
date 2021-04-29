import unittest
from pathlib import Path
import tests.integration.examples.lusid_utils as lusid_utils

# tag::imports[]
import lusid
from lusid import models
import uuid

import pandas as pd


# end::imports[]


class UserDefinedProperties(unittest.TestCase):
    def write_to_test_output(self, df, file_name):
        df.to_csv(Path(__file__).parent.joinpath(f"data/test_user_defined_properties/test_output/{file_name}"),
                  index=False)

    def test_user_defined_properties(self) -> None:
        api_factory = lusid_utils.api_factory

        # tag::create-apis[]
        property_definitions_api = api_factory.build(lusid.api.PropertyDefinitionsApi)
        # end::create-apis[]

        # tag::create-property[]
        scope = f"UK-Trading-{uuid.uuid4()}"
        response = property_definitions_api.create_property_definition(
            create_property_definition_request=models.CreatePropertyDefinitionRequest(
                domain="Portfolio",
                scope=scope,
                code="portfolio_manager_name",
                value_required=False,
                display_name="portfolio_manager_name",
                data_type_id=models.ResourceId(scope="system", code="string")
            )
        )
        portfolio_manager_property = response.key
        print(portfolio_manager_property)
        # end::create-property[]
        self.assertIsNotNone(portfolio_manager_property)
        with open(Path(__file__).parent.joinpath(
                "data/test_user_defined_properties/test_output/portfolio_manager_property.txt"), "w") as file:
            file.write(portfolio_manager_property)

        # tag::get-property[]
        response = property_definitions_api.get_property_definition(
            domain="Portfolio",
            scope=scope,
            code="portfolio_manager_name",
        )
        property_df = pd.DataFrame([{
            "Code": response.code,
            "Domain": response.domain,
            "Life Time": response.life_time,
            "Data Type": response.data_type_id
        }])

        # end::get-property[]
        self.write_to_test_output(property_df, "get_property.csv")
        self.assertEqual(response.code, "portfolio_manager_name")

        response = property_definitions_api.get_property_definition(
            domain="Holding",
            scope="default",
            code="PV",
        )
        print(response)

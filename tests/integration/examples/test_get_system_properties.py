import unittest
from itertools import groupby
from pathlib import Path

import tests.integration.examples.lusid_utils as lusid_utils

# tag::imports[]
import lusid

# end::imports[]


class GetSystemProperties(unittest.TestCase):
    def write_to_test_output(self, df, file_name):
        df.to_csv(Path(__file__).parent.joinpath(f"data/test_valuation/test_output/{file_name}"), index=False)

    def test_get_system_properties(self) -> None:
        api_factory = lusid_utils.api_factory
        aggregation_api = api_factory.build(lusid.api.AggregationApi)

        # response = api_factory.build(lusid.api.SearchApi).search_properties(
        #     filter="domain in 'Holding','Instrument','Portfolio'"
        # )
        # for value in response.values:
        #     print(value)

        response = aggregation_api.get_queryable_keys(
            # filter="addressKey startswith 'Holding'"
        )

        system_properties = sorted(
            [{"key": value.address_key, "name": value.display_name} for value in response.values],
            key=lambda x: x["key"]
        )
        grouped_by_domain = groupby(system_properties, key=lambda x: x["key"].split("/")[0])
        for domain, items in grouped_by_domain:
            print(f"{domain}:")
            for item in items:
                print(item)

        # for value in response.values:
        #     print(f"Key:{value.address_key}, Display: {value.display_name}, Supported Ops: {value.supported_operations}")

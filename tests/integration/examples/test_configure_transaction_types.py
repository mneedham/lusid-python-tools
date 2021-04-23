import unittest
from pathlib import Path
import tests.integration.examples.lusid_utils as lusid_utils

# tag::imports[]
import lusid
import pandas as pd
# end::imports[]


class ConfigureTransactionTypes(unittest.TestCase):
    def write_to_test_output(self, df, file_name):
        df.to_csv(Path(__file__).parent.joinpath(f"data/test_configure_transaction_types/test_output/{file_name}"),
                  index=False)

    def test_configure_transaction_types(self) -> None:
        api_factory = lusid_utils.api_factory

        # tag::create-apis[]
        system_configuration_api = api_factory.build(lusid.api.SystemConfigurationApi)
        # end::create-apis[]

        # tag::get-transaction-types[]
        response = system_configuration_api.list_configuration_transaction_types()

        transaction_types = response.transaction_configs
        aliases = [txn_type.aliases for txn_type in transaction_types]
        list_aliases = pd.DataFrame([alias.to_dict() for sublist in aliases for alias in sublist])
        # end::get-transaction-types[]
        self.write_to_test_output(list_aliases, "transaction_types.csv")


        # system_configuration_api.create_configuration_transaction_type(
        #     transaction_configuration_data_request=models.TransactionConfigurationDataRequest(
        #
        #     )
        # )

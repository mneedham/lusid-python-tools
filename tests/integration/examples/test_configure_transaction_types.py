import unittest
from pathlib import Path
import tests.integration.examples.lusid_utils as lusid_utils

# tag::imports[]
import lusid
from lusid import models
import pandas as pd
from lusidtools.cocoon.transaction_type_upload import upsert_transaction_type_alias, \
    create_transaction_type_configuration


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
        print(response)
        transaction_types = response.transaction_configs
        aliases = [txn_type.aliases for txn_type in transaction_types]
        list_aliases = pd.DataFrame([alias.to_dict() for sublist in aliases for alias in sublist])
        # end::get-transaction-types[]
        self.write_to_test_output(list_aliases, "transaction_types.csv")

        # x = [{"aliases": txn_type.aliases, "movements": txn_type.movements} for txn_type in transaction_types]
        # [(alias.to_dict(), movement.to_dict()) for sublist in x for alias in sublist["aliases"] for movement in sublist["movements"]]
        # [alias for sublist in x for alias in sublist]
        #
        # x1 = [{k: v for k, v in m.to_dict().items() if k in ["side", "direction", "movement_types"]} for m in
        #       x[0]['movements']]

        # keys_to_keep = ["type", "description", "transaction_class",  "side", "direction", "movement_types", "movement_types"]
        # [{k:v for k,v in {**alias.to_dict(), **movement.to_dict()}.items() if k in keys_to_keep}
        # for sublist in x for alias in sublist["aliases"] for movement in sublist["movements"]]

        upsert_transaction_type_alias(api_factory, [
            models.TransactionConfigurationDataRequest(
                aliases=[
                    models.TransactionConfigurationTypeAlias(
                        type='Buy',
                        description='Purchase',
                        transaction_class='Basic',
                        transaction_group='default',
                        transaction_roles='LongLonger'),
                    models.TransactionConfigurationTypeAlias(
                        type='BY',
                        description='PURCHASE',
                        transaction_class='Basic',
                        transaction_group='default',
                        transaction_roles='LongLonger'),
                ],
                movements=[
                    models.TransactionConfigurationMovementDataRequest(
                        movement_types='CashCommitment',
                        side='Side2',
                        direction=-1,
                    ),
                    models.TransactionConfigurationMovementDataRequest(
                        movement_types='StockMovement',
                        side='Side1',
                        direction=1,
                    )
                ])])

import unittest
from pathlib import Path
import pandas as pd
from lusidfeature import lusid_feature

from lusidtools import cocoon as cocoon
from parameterized import parameterized
import lusid
from lusidtools import logger
from lusidtools.cocoon.utilities import create_scope_id


class CocoonTestsHoldings(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")
        cls.api_factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=secrets_file
        )
        cls.logger = logger.LusidLogger("debug")

    @lusid_feature(
        "T3-1",
        "T3-2",
        "T3-3",
        "T3-4",
        "T3-5",
        "T3-6",
        "T3-7",
        "T3-8",
        "T3-9",
        "T3-10",
        "T3-11",
        "T3-12",
        "T3-13",
        "T3-14",
        "T3-15",
        "T3-16",
        "T3-17",
        "T3-18",
        "T3-19",
    )
    @parameterized.expand(
        [
            [
                "Standard successful load",
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Standard load with ~700 portfolios",
                "prime_broker_test",
                "data/holdings-example-large.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Standard successful load with adjustment only",
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                True,
                lusid.models.Version,
            ],
            [
                "Standard successful load with string index",
                "prime_broker_test",
                "data/holdings-example-unique-date-string-index.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Standard successful load with duplicate index",
                "prime_broker_test",
                "data/holdings-example-unique-date-duplicate-index.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Standard successful load with unique properties scope",
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                f"operations001_{create_scope_id()}",
                None,
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Add in some constants",
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": "$3000",
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Add in a default value for a required field with no specified column",
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": {"default": "2019-10-08"},
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Add in a column as a nested dictionary",
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": {"column": "Effective Date"},
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Add in a column and default as a nested dictionary",
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": {
                        "column": "Effective Date",
                        "default": "2019-10-08",
                    },
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Multiple effective dates in a holdings file",
                "prime_broker_test",
                "data/holdings-example.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Standard successful load with sub-holding-keys based on a column that exists in the file",
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                ["Security Description"],
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Standard successful load with sub-holding-keys based on two columns that exists in the file",
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                ["Security Description", "Prime Broker"],
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Standard successful load with a sub-holding-key that is not populated for all rows",
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                ["swap"],
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Standard successful load with a sub-holding-key that has no pre-existing property definition",
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                f"operations001_{create_scope_id()}",
                ["Prime Broker"],
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Standard successful load with a sub-holding-key that has no pre-existing property definition and a different scope",
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                f"operations001",
                ["Prime Broker"],
                f"accountview_{create_scope_id()}",
                False,
                lusid.models.Version,
            ],
            [
                "Standard successful load with a sub-holding-key that has a metric value",
                "prime_broker_test",
                "data/holdings-example-unique-date.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                ["Quantity"],
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Duplicate column in the source file",
                "prime_broker_test",
                "data/holdings-example-unique-date-duplicate-column.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "Quantity",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Pass string as tax_lots.units value",
                "prime_broker_test",
                "data/holdings-example-unique-date-duplicate-column.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": "$1",
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.models.Version,
            ],
            [
                "Pass integer as tax_lots.units value",
                "prime_broker_test",
                "data/holdings-example-unique-date-duplicate-column.csv",
                {
                    "code": "FundCode",
                    "effective_at": "Effective Date",
                    "tax_lots.units": 1,
                },
                {
                    "tax_lots.cost.amount": None,
                    "tax_lots.cost.currency": "Local Currency Code",
                    "tax_lots.portfolio_cost": None,
                    "tax_lots.price": None,
                    "tax_lots.purchase_date": None,
                    "tax_lots.settlement_date": None,
                },
                {
                    "Isin": "ISIN Security Identifier",
                    "Sedol": "SEDOL Security Identifier",
                    "Currency": "is_cash_with_currency",
                },
                ["Prime Broker"],
                "operations001",
                None,
                None,
                False,
                lusid.models.Version,
            ],
        ]
    )
    def test_load_from_data_frame_holdings_success(
        self,
        _,
        scope,
        file_name,
        mapping_required,
        mapping_optional,
        identifier_mapping,
        property_columns,
        properties_scope,
        sub_holding_keys,
        sub_holding_key_scope,
        holdings_adjustment_only,
        expected_outcome,
    ) -> None:
        """
        Test that holdings can be loaded successfully

        :param str scope: The scope of the portfolios to load the transactions into
        :param str file_name: The name of the test data file
        :param dict{str, str} mapping_required: The dictionary mapping the dataframe fields to LUSID's required base transaction/holding schema
        :param dict{str, str} mapping_optional: The dictionary mapping the dataframe fields to LUSID's optional base transaction/holding schema
        :param dict{str, str} identifier_mapping: The dictionary mapping of LUSID instrument identifiers to identifiers in the dataframe
        :param list[str] property_columns: The columns to create properties for
        :param str properties_scope: The scope to add the properties to
        :param list sub_holding_keys: The sub holding keys to populate on the adjustments as transaction properties
        :param bool holdings_adjustment_only: Whether to use the adjust_holdings api call rather than set_holdings when
               working with holdings
        :param any expected_outcome: The expected outcome

        :return: None
        """
        data_frame = pd.read_csv(Path(__file__).parent.joinpath(file_name))

        responses = cocoon.cocoon.load_from_data_frame(
            api_factory=self.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required=mapping_required,
            mapping_optional=mapping_optional,
            file_type="holdings",
            identifier_mapping=identifier_mapping,
            property_columns=property_columns,
            properties_scope=properties_scope,
            sub_holding_keys=sub_holding_keys,
            holdings_adjustment_only=holdings_adjustment_only,
            sub_holding_keys_scope=sub_holding_key_scope,
        )

        self.assertGreater(len(responses["holdings"]["success"]), 0)

        self.assertEqual(len(responses["holdings"]["errors"]), 0)

        self.assertTrue(
            expr=all(
                isinstance(succcess_response.version, expected_outcome)
                for succcess_response in responses["holdings"]["success"]
            )
        )

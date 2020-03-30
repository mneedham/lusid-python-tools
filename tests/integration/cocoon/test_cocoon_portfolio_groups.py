import unittest
from pathlib import Path
import pandas as pd
import json
import lusid
import lusid.models as models
from lusidtools import cocoon as cocoon
from lusidtools.cocoon.utilities import create_scope_id
import datetime
from dateutil.tz import tzutc
import logging
from lusidtools.cocoon.logger import LusidLogger

logger = LusidLogger(l)

logging.warning(fksdlkfsdlfkslfs)


class CocoonTestPortfolioGroup(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:

        cls.portfolio_scope = create_scope_id()
        secrets_file = Path(__file__).parent.parent.parent.joinpath("secrets.json")
        cls.api_factory = lusid.utilities.ApiClientFactory(
            api_secrets_filename=secrets_file
        )
        cls.unique_portfolios = pd.read_csv(
            Path(__file__).parent.joinpath(
                "data/port_group_tests/test_1_pg_create_with_portfolios.csv"
            )
        )["FundCode"].tolist()

        def create_portfolio_model(code):
            model = models.CreateTransactionPortfolioRequest(
                display_name=code,
                code=code,
                base_currency="GBP",
                description="Paper transaction portfolio",
                created="2020-02-25T00:00:00Z",
            )
            return model

        try:

            for code in cls.unique_portfolios:
                cls.api_factory.build(
                    lusid.api.TransactionPortfoliosApi
                ).create_portfolio(
                    scope=cls.portfolio_scope,
                    transaction_portfolio=create_portfolio_model(code),
                )
        except lusid.exceptions.ApiException as e:
            if e.status == 404:
                logger.error(f"The portfolio {code} already exists")

    def log_error_requests_title(cls, domain, responses):
        try:

            if len(responses.get(domain, {}).get("errors", []))

            if len(responses[domain]["errors"]) > 0:
                for error in responses[domain]["errors"]:
                    return logger.error(json.loads(error.body)["title"])
        except KeyError:
            pass

    def csv_to_data_frame_with_scope(cls, csv, scope):
        data_frame = pd.read_csv(Path(__file__).parent.joinpath(csv))
        data_frame["Scope"] = scope
        return data_frame

    def cocoon_load_from_dataframe(
        cls,
        scope,
        data_frame,
        mapping_optional={"values.scope": "Scope", "values.code": "FundCode",},
        property_columns=[],
        properties_scope=None,
    ):

        return cocoon.cocoon.load_from_data_frame(
            api_factory=cls.api_factory,
            scope=scope,
            data_frame=data_frame,
            mapping_required={
                "code": "PortGroupCode",
                "display_name": "PortGroupDisplayName",
            },
            mapping_optional=mapping_optional,
            file_type="portfolio_group",
            property_columns=property_columns,
            properties_scope=properties_scope,
        )

    def test_01_pg_create_with_portfolios(self) -> None:

        """
        Description:
        ------------
        Here we test adding multiple new portfolio groups with multiple portfolios.
        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_1_pg_create_with_portfolios.csv",
            self.portfolio_scope,
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope, data_frame=data_frame
        )

        self.log_error_requests_title("portfolio_groups", responses)

        self.assertEqual(
            len(
                [
                    port_group
                    for nested_group in [
                        port_group.portfolios
                        for port_group in responses["portfolio_groups"]["success"]
                    ]
                    for port_group in nested_group
                ]
            ),
            len(data_frame),
        )

    def test_02_pg_create_with_no_portfolio(self) -> None:

        """
        Description:
        ------------
        Here we test adding one new portfolio group with no portfolios.
        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_2_pg_create_with_no_portfolio.csv",
            self.portfolio_scope,
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope, data_frame=data_frame, mapping_optional={}
        )

        self.log_error_requests_title("portfolio_groups", responses)

        self.assertEqual(
            first=len(
                [
                    port_group._id
                    for port_group in responses["portfolio_groups"]["success"]
                ]
            ),
            second=len(data_frame),
        )

        self.assertEqual(
            first=responses["portfolio_groups"]["success"][0].id,
            second=lusid.models.ResourceId(
                scope=test_case_scope, code=data_frame["PortGroupCode"].tolist()[0]
            ),
        )

    def test_03_pg_create_multiple_groups_no_portfolio(self) -> None:

        """
        Description:
        ------------
        Here we test adding multiple new portfolio group with no portfolios.
        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_3_pg_create_multiple_groups_no_portfolio.csv",
            self.portfolio_scope,
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope, data_frame=data_frame, mapping_optional={}
        )

        self.log_error_requests_title("portfolio_groups", responses)

        self.assertEqual(
            first=len(
                [
                    port_group._id
                    for port_group in responses["portfolio_groups"]["success"]
                ]
            ),
            second=len(data_frame),
        )

        self.assertEqual(
            first=responses["portfolio_groups"]["success"][1].id,
            second=lusid.models.ResourceId(
                scope=test_case_scope, code=data_frame["PortGroupCode"].tolist()[1]
            ),
        )

    def test_04_pg_create_with_portfolio_not_exist(self):

        """
        Description:
        ------------
        Here we test attempting to add a portfolio which does not exist to a portfolio group.
        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_4_pg_create_with_portfolio_not_exist.csv",
            self.portfolio_scope,
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope, data_frame=data_frame
        )

        self.log_error_requests_title("portfolio_groups", responses)

        self.assertEqual(
            json.loads(responses["portfolio_groups"]["errors"][0].body)["name"],
            "PortfolioNotFound",
        )

        self.assertEqual(len(responses["portfolio_groups"]["success"]), 0)

    def test_05_pg_create_with_duplicate_portfolios(self):

        """
        Description:
        ------------
        Here we test attempting to add two of the same portfolios to a portfolio group.
        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_5_pg_create_with_duplicate_portfolios.csv",
            self.portfolio_scope,
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope, data_frame=data_frame
        )

        self.log_error_requests_title("portfolio_groups", responses)

        data_frame["unique_combinations_for_test"] = (
            data_frame["PortGroupCode"]
            + "-"
            + data_frame["FundCode"]
            + "-"
            + data_frame["Scope"]
        )

        self.assertEqual(
            len(
                [
                    port_group
                    for nested_group in [
                        port_group.portfolios
                        for port_group in responses["portfolio_groups"]["success"]
                    ]
                    for port_group in nested_group
                ]
            ),
            len(data_frame["unique_combinations_for_test"].unique()),
        )

    def test_06_pg_create_duplicate_port_group(self):

        """
        Description:
        ------------
        Here we test updating a portfolio group which already exists
        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_6_pg_create_duplicate_port_group.csv",
            self.portfolio_scope,
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope, data_frame=data_frame, mapping_optional={}
        )

        self.log_error_requests_title("portfolio_groups", responses)

        self.assertEqual(len(responses["portfolio_groups"]["success"]), 1)

        self.assertEqual(
            first=responses["portfolio_groups"]["success"][0].id,
            second=lusid.models.ResourceId(
                scope=test_case_scope, code=data_frame["PortGroupCode"].tolist()[0]
            ),
        )

    def test_08_pg_add_bad_portfolio(self):

        """
        Description:
        ------------
        Here we test updating a portfolio group which already exists.
        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_8_pg_add_bad_portfolio.csv",
            self.portfolio_scope,
        )

        # Create the portfolio group as a seperate request
        port_group_request = lusid.models.CreatePortfolioGroupRequest(
            code=data_frame["PortGroupCode"][0],
            display_name=data_frame["PortGroupCode"][0],
        )

        self.api_factory.build(lusid.api.PortfolioGroupsApi).create_portfolio_group(
            scope=test_case_scope, request=port_group_request
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope, data_frame=data_frame
        )

        self.log_error_requests_title("portfolio_groups", responses)

        self.assertTrue(len(responses["portfolio_groups"]["errors"]) > 0)

        self.assertEqual(
            json.loads(responses["portfolio_groups"]["errors"][0].body)["name"],
            "PortfolioNotFound",
        )

    def test_07_pg_create_with_properties(self) -> None:

        """
        Description:
        ------------
        Here we test adding multiple new portfolio groups with multiple portfolios.
        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_7_pg_create_with_properties.csv",
            self.portfolio_scope,
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope,
            data_frame=data_frame,
            property_columns=["location", "MifidFlag"],
            properties_scope=test_case_scope,
        )

        self.log_error_requests_title("portfolio_groups", responses)

        response_with_properties = self.api_factory.build(
            lusid.api.PortfolioGroupsApi
        ).get_group_properties(
            scope=test_case_scope, code=data_frame["PortGroupCode"].tolist()[0],
        )

        self.assertEqual(
            {
                "PortfolioGroup/"
                + test_case_scope
                + "/location": lusid.models.ModelProperty(
                    key="PortfolioGroup/" + test_case_scope + "/location",
                    value=lusid.models.PropertyValue(label_value="UK"),
                    effective_from=datetime.datetime(1, 1, 1, 0, 0, tzinfo=tzutc()),
                ),
                "PortfolioGroup/"
                + test_case_scope
                + "/MifidFlag": lusid.models.ModelProperty(
                    key="PortfolioGroup/" + test_case_scope + "/MifidFlag",
                    value=lusid.models.PropertyValue(label_value="Y"),
                    effective_from=datetime.datetime(1, 1, 1, 0, 0, tzinfo=tzutc()),
                ),
            },
            response_with_properties.properties,
        )

    def test_09_pg_add_duplicate_portfolio(self) -> None:

        """
        Description:
        ------------
        Here we test adding multiple new portfolio groups with multiple portfolios.
        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_9_pg_add_duplicate_portfolio.csv",
            self.portfolio_scope,
        )

        # Create the portfolio group as a seperate request
        port_group_request = lusid.models.CreatePortfolioGroupRequest(
            code=data_frame["PortGroupCode"][0],
            display_name=data_frame["PortGroupCode"][0],
        )

        self.api_factory.build(lusid.api.PortfolioGroupsApi).create_portfolio_group(
            scope=test_case_scope, request=port_group_request
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope, data_frame=data_frame
        )

        self.log_error_requests_title("portfolio_groups", responses)

        self.assertEqual(
            first=responses["portfolio_groups"]["success"][0].portfolios[0],
            second=lusid.models.ResourceId(
                code=data_frame["FundCode"][0], scope=data_frame["Scope"][0]
            ),
        )

    def test_10_pg_add_no_new_portfolio(self) -> None:

        """
        Description:
        ------------
        Here we test adding multiple new portfolio groups with multiple portfolios.
        """

        test_case_scope = create_scope_id()
        data_frame = self.csv_to_data_frame_with_scope(
            "data/port_group_tests/test_10_pg_add_no_new_portfolio.csv",
            self.portfolio_scope,
        )

        port_group_request = lusid.models.CreatePortfolioGroupRequest(
            code=data_frame["PortGroupCode"][0],
            display_name=data_frame["PortGroupCode"][0],
        )

        self.api_factory.build(lusid.api.PortfolioGroupsApi).create_portfolio_group(
            scope=test_case_scope, request=port_group_request,
        )

        responses = self.cocoon_load_from_dataframe(
            scope=test_case_scope, data_frame=data_frame,
        )

        self.log_error_requests_title("portfolio_groups", responses)

        self.assertEqual(
            first=responses["portfolio_groups"]["success"][0].portfolios[0],
            second=lusid.models.ResourceId(
                code=data_frame["FundCode"][0], scope=data_frame["Scope"][0]
            ),
        )

import unittest
from pathlib import Path
import tests.integration.examples.lusid_utils as lusid_utils

# tag::imports[]
import lusid
from lusid import models

import pytz
import pandas as pd
import uuid
from datetime import datetime
from dateutil.parser import parse
# end::imports[]


class Valuation(unittest.TestCase):
    def write_to_test_output(self, df, file_name):
        df.to_csv(Path(__file__).parent.joinpath(f"data/test_valuation/test_output/{file_name}"), index=False)

    def test_valuation(self) -> None:
        api_factory = lusid_utils.api_factory

        # tag::apis[]
        instruments_api = api_factory.build(lusid.api.InstrumentsApi)
        transaction_portfolios_api = api_factory.build(lusid.api.TransactionPortfoliosApi)
        quotes_api = api_factory.build(lusid.api.QuotesApi)
        aggregation_api = api_factory.build(lusid.api.AggregationApi)
        # end::apis[]
        portfolios_api = api_factory.build(lusid.api.PortfoliosApi)

        scope = portfolio_code = "Developer-Valuation-Tutorial-2021-04-30-10_16_58"
        effective_at = "2021-04-21T00:00:00Z"
        effective_at_filter = "quoteId.effectiveAt gte 2021-04-20T23:00:00.0000000Z AND quoteId.effectiveAt lt 2021-04-24T21:00:00.0000000Z"

        # scope = "valuation-simplified"
        # portfolio_code = "EQUITY_UK"
        # effective_at = "2020-08-24T01:01:00Z"
        # effective_at_filter = "quoteId.effectiveAt gte 2020-08-23T23:00:00.0000000Z AND quoteId.effectiveAt lt 2020-08-24T23:00:00.0000000Z"

        # tag::compute-valuation[]
        def compute_valuation_with_recipe(scope, portfolio_code, recipe_code, date):
            return aggregation_api.get_valuation(
                valuation_request=models.ValuationRequest(
                    recipe_id=models.ResourceId(scope=scope, code=recipe_code),
                    metrics=[
                        models.AggregateSpec("Instrument/default/Name", "Value"),
                        models.AggregateSpec("Holding/default/Units", "Sum"),
                        models.AggregateSpec("Holding/default/PV", "Sum"),
                        models.AggregateSpec("Holding/default/PV", "Proportion"),
                    ],
                    group_by=["Instrument/default/Name"],
                    valuation_schedule=models.ValuationSchedule(effective_at=date),
                    portfolio_entity_ids=[models.PortfolioEntityId(
                        scope=scope,
                        code=portfolio_code,
                        portfolio_entity_type="SinglePortfolio"
                    )]
                )
            ).data
        # end::compute-valuation[]

        # tag::debug[]
        # response = instruments_api.list_instruments(
        #     filter="Identifiers[LusidInstrumentId] startswith 'LUID'"
        # )
        #
        # instruments_df = pd.DataFrame([{
        #     "Instrument": instrument.name,
        #     "LUID": instrument.lusid_instrument_id}
        #     for instrument in response.values
        # ])
        # print("")
        # print("Instruments:")
        # print(instruments_df)

        holding_response = transaction_portfolios_api.get_holdings(
            scope=scope,
            code=portfolio_code,
            property_keys=["Instrument/default/Name", "Instrument/default/LusidInstrumentId"])
        holdings_df = pd.DataFrame([{
            "Instrument": value.properties["Instrument/default/Name"].value.label_value,
            "LUID": value.properties["Instrument/default/LusidInstrumentId"].value.label_value,
            "Amount": value.cost.amount,
            "Units": value.units,
            "Type": value.holding_type,
        } for value in holding_response.values])
        print("")
        print("Holdings:")
        print(holdings_df)

        print("")
        print("Quotes:")
        quotes_response = quotes_api.list_quotes_for_scope(
            scope=scope,
            filter=effective_at_filter,
        )
        quotes_df = pd.DataFrame([{
            "Instrument": value.quote_id.quote_series_id.instrument_id,
            "InstrumentIdType": value.quote_id.quote_series_id.instrument_id_type,
            "Provider": value.quote_id.quote_series_id.provider,
            "Price": value.metric_value.value,
            "Field": value.quote_id.quote_series_id.field,
            "EffectiveAt": value.quote_id.effective_at
        } for value in quotes_response.values])

        print(quotes_df)
        # end::debug[]

        # tag::get-valuation-20210421[]
        effective_at = effective_at
        response = compute_valuation_with_recipe(scope, portfolio_code, "default", effective_at)
        valuation = pd.DataFrame(response)
        # end::get-valuation-20210421[]
        print("")
        print("Valuation:")
        print(valuation)


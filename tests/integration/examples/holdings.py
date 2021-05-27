from datetime import datetime
import pytz


def funds_loaded(transaction_portfolios_api, scope, portfolio_code):
    # tag::get-holdings-funds-loaded[]
    holding_response = transaction_portfolios_api.get_holdings(
        scope=scope,
        code=portfolio_code,
        property_keys=["Instrument/default/Name"],
        effective_at=datetime(year=2020, month=1, day=1, hour=1, tzinfo=pytz.UTC),
    )
    # end::get-holdings-funds-loaded[]
    return holding_response


def day1_trading(transaction_portfolios_api, scope, portfolio_code):
    # tag::get-holdings-first-day-trading[]
    holding_response = transaction_portfolios_api.get_holdings(
        scope=scope,
        code=portfolio_code,
        property_keys=["Instrument/default/Name"],
        effective_at=datetime(year=2020, month=1, day=2, hour=1, tzinfo=pytz.UTC),
    )
    # end::get-holdings-first-day-trading[]
    return holding_response


def day2_trading(transaction_portfolios_api, scope, portfolio_code):
    # tag::get-holdings-second-day-trading[]
    holding_response = transaction_portfolios_api.get_holdings(
        scope=scope,
        code=portfolio_code,
        property_keys=["Instrument/default/Name"],
        effective_at=datetime(year=2020, month=1, day=3, hour=1, tzinfo=pytz.UTC),
    )
    # end::get-holdings-second-day-trading[]
    return holding_response


def latest_day_trading(transaction_portfolios_api, scope, portfolio_code):
    # tag::get-holdings-today[]
    holding_response = transaction_portfolios_api.get_holdings(
        scope=scope,
        code=portfolio_code,
        property_keys=["Instrument/default/Name"],
        effective_at=datetime(year=2020, month=1, day=3, hour=1, tzinfo=pytz.UTC),
    )
    # end::get-holdings-today[]
    return holding_response


def positions(transaction_portfolios_api, scope, portfolio_code):
    # tag::get-holdings-positions[]
    holding_response = transaction_portfolios_api.get_holdings(
        scope=scope,
        code=portfolio_code,
        filter="holdingType eq 'P'",
        property_keys=["Instrument/default/Name"]
    )
    # end::get-holdings-positions[]
    return holding_response

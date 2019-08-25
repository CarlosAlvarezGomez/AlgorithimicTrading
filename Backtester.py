import datetime
import math
import pandas as pd
import sqlite3

################################################################################
# Helper functions for backtest begin here
################################################################################


def execute_action(action, date, dataframe, portfolio, commission):
    """
    checks if an action is possible given the current portfolio, commission, and
    prices(obtained using date and dataframe), and executes it if it is
    possible. An action is a string the contains a type (either BUY or SELL),
    followed by a quantity(an int), and a ticker that is in the dataframe
    """

    action = action.split()
    quantity = int(action[1])
    if quantity <= 0:
        raise Exception("You must trade a positive number of stocks")
    company = action[2]
    price = dataframe.loc[lambda dataframe: (dataframe["Company"] == company), :].iloc[
        0
    ][2]
    if action[0] == "BUY":
        if portfolio["Dollars"] >= (price * quantity) + commission:
            portfolio["Dollars"] -= (price * quantity) + commission
            if company in portfolio.keys():
                portfolio[company] += quantity
            else:
                portfolio.update({company: quantity})
                print(portfolio)
            print(portfolio)
            return portfolio
        else:
            print(portfolio)
            raise Exception("You cannot afford to buy this many stocks")
    elif action[0] == "SELL":
        if (company in portfolio.keys()) & (portfolio[company] >= quantity):
            if portfolio["Dollars"] >= commission:
                portfolio["Dollars"] += (price * quantity) - commission
                portfolio[company] -= quantity
                if portfolio[company] == 0:
                    del portfolio[company]
                print(portfolio)
                return portfolio
            else:
                print(portfolio)
                raise Exception("You cannot afford the commission expense")
        else:
            print(portfolio)
            raise Exception("You do not own this many stocks")
    else:
        raise Exception("This type of command could not be processed")
    return portfolio


def split_by_date(dataframe, start_date, end_date):
    """
    splits a dataframe into a dictionary of n dataframes. Data that is available
    before the start_date is at key "before start date," the rest will be the
    key with the corresponding date. For example: if df is a dataframe that
    contains data about daily temperatures in the 1980s, the start_date is
    January 1, 1980, and the end_date is January 1, 1983, then split_by_date(df,
    start_date, end_date) will return a dictionary of dataframes containing all
    the temperatures on weekdays between January 1, 1981 and January 1, 1983
    """

    print("Splitting dataframe")
    is_available = dataframe["Date"] < str(start_date)[:10]
    not_available = dataframe["Date"] >= str(start_date)[:10]
    available_data = dataframe[is_available]
    dataframe = dataframe[not_available]
    dictionary = dict(tuple(dataframe.groupby("Date")))
    dictionary.update({"before start date": available_data})
    print("Dictionary has been split")
    return dictionary


# returns a dataframe with all the companies from the dataframe that are in the
def filter_by_company(dataframe, company_list):
    df = dataframe.iloc[0:0]
    for stock in company_list:
        df = pd.concat([df, dataframe[dataframe["Company"] == stock]])
    return df


################################################################################
# Helper functions for backtest end here
################################################################################


def backtest(database, strategy, initial_cash, commission=0):

    connection = sqlite3.connect(database)
    cursor = connection.cursor()

    cursor.execute("SELECT MIN(date) FROM Historical_Prices_and_Volumes")
    earliest_date = datetime.datetime.strptime(cursor.fetchall()[0][0], "%Y-%m-%d")

    cursor.execute("SELECT MAX(date) FROM Historical_Prices_and_Volumes")
    latest_date = datetime.datetime.strptime(cursor.fetchall()[0][0], "%Y-%m-%d")

    # earliest_date = datetime.datetime(2005, 1, 1)
    # latest_date = datetime.datetime(2010, 1, 1)

    portfolio = {"Dollars": initial_cash}

    price_volume_command = (
        "SELECT * FROM Historical_Prices_and_Volumes WHERE Date >= '"
        + str(earliest_date)[:10]
        + "' AND Date <= '"
        + str(latest_date)[:10]
        + "';"
    )
    price_volume_dataframe = pd.read_sql_query(price_volume_command, connection)
    price_volume_dataframe = price_volume_dataframe[
        price_volume_dataframe["Price"] != "nan"
    ]
    price_volume_dataframe_dict = split_by_date(
        price_volume_dataframe, earliest_date, latest_date
    )
    del price_volume_command
    del price_volume_dataframe
    del price_volume_dataframe_dict["before start date"]

    dividends_command = (
        "SELECT * FROM Historical_Dividends WHERE Date >= '"
        + str(earliest_date)[:10]
        + "' AND Date <= '"
        + str(latest_date)[:10]
        + "';"
    )
    dividends_dataframe = pd.read_sql_query(dividends_command, connection)
    dividends_dataframe = dividends_dataframe.dropna()

    dividend_dataframe_dict = split_by_date(
        dividends_dataframe, earliest_date, latest_date
    )

    del dividends_command
    del dividends_dataframe
    del dividend_dataframe_dict["before start date"]

    stock_splits_command = (
        "SELECT * FROM Historical_Stock_Splits WHERE Date <= '"
        + str(latest_date)[:10]
        + "' AND Split_Ratio != '1/0';"
    )
    stock_splits_dataframe = pd.read_sql_query(stock_splits_command, connection)
    stock_splits_dataframe = stock_splits_dataframe.dropna()

    connection.close()

    stock_split_dataframe_dict = split_by_date(
        stock_splits_dataframe, earliest_date, latest_date
    )

    del stock_splits_command
    del stock_splits_dataframe
    del stock_split_dataframe_dict["before start date"]

    i = 0

    for date in pd.date_range(earliest_date, latest_date, freq="B"):
        str_date = str(date)[:10]
        print(str_date)
        if len(portfolio) > 1:

            # adds dividend payments to portfolio
            if str_date in dividend_dataframe_dict.keys():
                todays_dividends = dividend_dataframe_dict.pop(str_date)
                todays_dividends = filter_by_company(todays_dividends, portfolio.keys())
                if len(todays_dividends) > 0:
                    for i in range(len(todays_dividends)):
                        portfolio["Dollars"] += (
                            math.floor(
                                (
                                    todays_dividends.iloc[i][2]
                                    * portfolio[todays_dividends.iloc[i][1]]
                                )
                                * 100
                            )
                            / 100
                        )

            # adjusts portfolio for stock splits
            if str_date in stock_split_dataframe_dict.keys():
                todays_stock_splits = stock_split_dataframe_dict.pop(str_date)
                todays_stock_splits = filter_by_company(
                    todays_stock_splits, portfolio.keys()
                )
                if len(todays_stock_splits) > 0:
                    for i in range(len(todays_stock_splits)):
                        ratio = todays_stock_splits.iloc[i][2].split("/")
                        if ratio[1] != "0":
                            quotient = math.floor(
                                portfolio[todays_stock_splits.iloc[i][1]]
                                / int(ratio[0])
                            )
                            remainder = portfolio[todays_stock_splits.iloc[i][1]] % int(
                                ratio[0]
                            )
                            portfolio[todays_stock_splits.iloc[i][1]] = (
                                quotient * int(ratio[1])
                            ) + remainder

        if str_date in price_volume_dataframe_dict.keys():
            # uses strategy to analyze most recent data
            todays_prices = price_volume_dataframe_dict.pop(str_date)
            actions = strategy(portfolio, date, todays_prices, commission)
            print(actions)
            # executes each action given by the strategy
            for action in actions:
                portfolio = execute_action(
                    action, date, todays_prices, portfolio, commission
                )
    return portfolio


################################################################################
# This is a strategy used for testing the backtester that buys the lowest price
# possible and sells all stocks at the end of the year. This is purely for
# testing.
################################################################################


# def buy_lowest_price(portfolio, date, dataframe, commission):
#     actions = []
#     if str(date)[5:7] == "12":
#         if (str(date)[8:10] == "29") & (date.weekday() == 4):
#             for stock in portfolio.keys():
#                 if (stock != "Dollars") & (
#                     any((dataframe[dataframe["Company"] == stock])["Price"] != "nan")
#                 ):
#                     actions.append("SELL " + str(portfolio[stock]) + " " + stock)
#         elif (str(date)[8:10] == "30") & (date.weekday() == 4):
#             for stock in portfolio.keys():
#                 if (stock != "Dollars") & (
#                     any((dataframe[dataframe["Company"] == stock])["Price"] != "nan")
#                 ):
#                     actions.append("SELL " + str(portfolio[stock]) + " " + stock)
#         elif str(date)[8:10] == "31":
#             for stock in portfolio.keys():
#                 if (stock != "Dollars") & (
#                     any((dataframe[dataframe["Company"] == stock])["Price"] != "nan")
#                 ):
#                     actions.append("SELL " + str(portfolio[stock]) + " " + stock)
#     if portfolio["Dollars"] < (2 * commission):
#         return actions
#     todays_prices = dataframe[dataframe["Price"] != "nan"]
#     todays_prices = todays_prices.sort_values(by=["Price"])
#     lowest_price = round(math.ceil(todays_prices.iloc[0][2] * 100) / 100, 2)
#     company = todays_prices.iloc[0][1]
#     volume = math.floor(0.05 * todays_prices.iloc[0][3])
#     if portfolio["Dollars"] >= (lowest_price + (2 * commission)):
#         quantity = min(
#             math.floor((portfolio["Dollars"] - (2 * commission)) / lowest_price), volume
#         )
#         while (quantity * lowest_price + 2 * commission) > portfolio["Dollars"]:
#             quantity -= 1
#         if quantity > 0:
#             actions.append("BUY " + str(quantity) + " " + company)
#     return actions


# portfolio = backtest("historical_data.db", buy_lowest_price, 1000, commission=6.95)

# print(portfolio)

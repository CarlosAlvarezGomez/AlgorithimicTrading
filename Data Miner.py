import pandas as pd
import PyPDF2 as p2
import requests
import sqlite3

################################################################################
# This part creates a list of all tickers of the companies in the Russell 3000
################################################################################

################################################################################
# Helper functions for creating tickers begin here
################################################################################

# is_digit(string) returns true if string is a single digit
def is_digit(string):
    return (
        (string == "0")
        | (string == "1")
        | (string == "2")
        | (string == "3")
        | (string == "4")
        | (string == "5")
        | (string == "6")
        | (string == "7")
        | (string == "8")
        | (string == "9")
    )


# is_valid_ticker(text) returns true if the text has no digits, a length
# between 1 and 5, with the exception of NYLD.A, and if it is not a company's
# full name
def is_valid_ticker(text):
    if (
        (len(text) > 0)
        & ((len(text) < 6) | (text == "NYLD.A"))
        & (text != "AECOM")
        & (text != "CAINC")
        & (text != "HPINC")
        & (text != "PETIQ")
        & (text != "NNINC")
        & (text != "ZUORA")
        & (text != "ZYNEX")
    ):
        for char in list(text):
            if is_digit(char):
                return False
        return True
    else:
        return False


################################################################################
# Helper functions for creating tickers end here
################################################################################

# reads the elements of the PDF files containing all the tickers
PDFfile1 = open("Get Data/Russell 3000 Components.pdf", "rb")
PDFfile2 = open("Get Data/Russell 3000 Additions 2019.pdf", "rb")
Russell_2018 = p2.PdfFileReader(PDFfile1)
Russell_2019_additions = p2.PdfFileReader(PDFfile2)

tickers = []

# iterates through each word in the first pdf and adds it to the ticker list if
# it is a ticker
for number in range(Russell_2018.getNumPages()):
    text = Russell_2018.getPage(number).extractText().split("\n")
    for i in range(len(text)):
        text[i] = text[i].replace(" ", "")
        if is_valid_ticker(text[i]):
            tickers.append(text[i])

# iterates through each word in the second pdf and adds it to the ticker list if
# it is a ticker
for number in range(Russell_2019_additions.getNumPages()):
    text = Russell_2019_additions.getPage(number).extractText().split("\n")
    for i in range(len(text)):
        text[i] = text[i].replace(" ", "")
        if is_valid_ticker(text[i]):
            tickers.append(text[i])

# removes duplicates of companies whose names are the same as their tickers
tickers.remove("RH")
tickers.remove("TYME")
tickers.remove("LYFT")

# removes tickers of companies that are not in Yahoo's database
companies_not_found = """AVHI ABAX ANCX ACXM AET AOI ARII AFSI ANDV APTI ARRS
ASNS ATHN AHL BWINB BEL BNCL BRK.B BH.A OZRK BHBK BOFI BF.A BOJA BF.B BLMT CA
ABCD CPLA CAVM CHFN CLD COBZ CIVI CVON CVG COTV CRD.B CORI DCT DDR DEPO CYS DPS
DSW DNB ECR EDR ESIO ELLI PERY EGC EGL EVHC ECYT EGN ESRX ESND ESL FCB FNGN
FBNK FFKT FNBG FCE.A FMI GGP GPT GNBC GEF.B GBNK GOV HYH HCOM GLF HEI.A HRG ILG
IMDZ HDP IDTI IMPV IPCC ITG KTWO KS KERX KLXI KMG KND KLDX LEN.B LPNT LHO LFGR
LOXO LGF.A LGF.B MBFI KORS MOG.A MB NCOM NSM MTGE NWY NFX NXEO NYLD.A NYLD NTRI
NXTM ORIG OCLR P PAH COOL PHH PHIIK PNK PF PX QCP QSII REN REIS RSPP COL RDC
SHLD SIR SN SCG SHLM SEND SPA SONC STBZ SYNT TAHO SVU SLD TSRO TRNC PAY VR VVC
WEB VTL WRD JW.A WIN WMIH WTW WGL ZOES XCRA XL XOXO CRD.A DVMT SGYP USG"""

companies_not_found_list = companies_not_found.split()

for ticker in companies_not_found_list:
    tickers.remove(ticker)

# adds companies that were not in the original files
tickers.append("SGYPQ")

################################################################################
# This part uses urls to gather data from yahoo finance
################################################################################

# # splits sample urls in order to replace the ticker
# prices_and_volumes_url = (
#     "https://query1.finance.yahoo.com/v7/finance/download/XOM?period1=-25236000"
#     + "0&period2=1562472000&interval=1d&events=history&crumb=H2RuVtotLS3"
# ).split("XOM")

# dividends_url = (
#     "https://query1.finance.yahoo.com/v7/finance/download/XOM?period1=-25235640"
#     + "0&period2=1562472000&interval=1d&events=div&crumb=HJBnDea.bVV"
# ).split("XOM")

# stock_splits_url = (
#     "https://query1.finance.yahoo.com/v7/finance/download/XOM?period1=-25235640"
#     + "0&period2=1562472000&interval=1d&events=split&crumb=HJBnDea.bVV"
# ).split("XOM")

# prices_and_volumes_url_list = []
# dividends_url_list = []
# stock_splits_url_list = []

# # iterates through each ticker, then creates and adds new urls using the sample
# # urls
# for i in range(len(tickers)):
#     prices_and_volumes_url_list.append(
#         prices_and_volumes_url[0] + tickers[i] + prices_and_volumes_url[1]
#     )
#     dividends_url_list.append(dividends_url[0] + tickers[i] + dividends_url[1])
#     stock_splits_url_list.append(stock_splits_url[0] + tickers[i] + stock_splits_url[1])

# # prints each url in each list. Click on each url in order to download the data
# for i in range(10):
#     wget.download(prices_and_volumes_url_list[i], "Downloads")

# for url in dividends_url_list:
#     print(url)

# for url in stock_splits_url_list:
#     print(url)

################################################################################
# This part uses the csv files to create SQL tables containing the historical
# data from each company. THIS WILL NOT RUN IF THE FINANCIAL DATA IS NOT
# DOWNLOADED AND IN THE CURRENT FILE
################################################################################

# connection = sqlite3.connect("historical_data.db")
# cursor = connection.cursor()

# # creates each table in the database. This will raise an error if these tables
# # already exist in the database, so you
# cursor.execute(
#     """CREATE TABLE Historical_Prices_And_Voluemes(
#         Date DATE,
#         Company TEXT,
#         Price decimal,
#         Volume int);"""
# )

# cursor.execute(
#     """CREATE TABLE Historical_Dividends(
#         Date DATE,
#         Company TEXT,
#         Dividend decimal);"""
# )
# cursor.execute(
#     """CREATE TABLE Historical_Stock_Splits(
#         Date DATE,
#         Company TEXT,
#         Split_Ratio TEXT);"""
# )

# # iterates through each ticker in the ticker list and adds the data to each
# # table
# for n in range(len(tickers)):
#     if n % 100 == 0:
#         print(str(n) + " Completed")
#     file = tickers[n] + ".csv"
#     # opens csv files
#     prices_and_volumes = pd.read_csv("Historical Prices and Volumes/" + file)
#     dividends = pd.read_csv("Dividends/" + file)
#     stock_splits = pd.read_csv("Stock Splits/" + file)
#     # adds prices and volumes to database
#     for i in range(len(prices_and_volumes)):
#         sql_command = (
#             """INSERT INTO Historical_Prices_And_Voluemes (Date, Company, Price,
#             Volume)
#             VALUES ('"""
#             + str(prices_and_volumes["Date"][i])
#             + "', '"
#             + tickers[n]
#             + "', '"
#             + str(prices_and_volumes["Close"][i])
#             + "', '"
#             + (str(prices_and_volumes["Volume"][i]))
#             + "');"
#         )
#         cursor.execute(sql_command)
#     # adds dividends to database
#     for i in range(len(dividends)):
#         sql_command = (
#             """INSERT INTO Historical_Dividends (Date, Company, Dividend)
#             VALUES ('"""
#             + str(dividends["Date"][i])
#             + "', '"
#             + tickers[n]
#             + "', '"
#             + str(dividends["Dividends"][i])
#             + "');"
#         )
#         cursor.execute(sql_command)
#     # adds stock splits to database
#     for i in range(len(stock_splits)):
#         sql_command = (
#             """INSERT INTO Historical_Stock_Splits (Date, Company, Split_Ratio)
#             VALUES ('"""
#             + str(stock_splits["Date"][i])
#             + "', '"
#             + tickers[n]
#             + "', '"
#             + str(stock_splits["Stock Splits"][i])
#             + "');"
#         )
#         cursor.execute(sql_command)

# connection.commit()

# connection.close()

for ticker in tickers:
    url = "https://api.etrade.com/v1/market/quote/{" + ticker + "}"
    r = requests.get(url)
    print(r)

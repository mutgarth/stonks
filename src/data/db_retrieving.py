from db_connect import db_connect
import pandas as pd
import sys
sys.path.append('..')
from utils import printProgressBar

def ret_tickers():
    print('Retrieving tickers...')
    con = db_connect()
    cursor = con.cursor()
    cursor.execute("SELECT id, ticker FROM symbol")
    data = cursor.fetchall()
    con.close()
    return [(d[0], d[1]) for d in data]

def ret_adj_close(symbol):
    con = db_connect()
    sql = """SELECT dp.price_date, dp.adj_close_price
            FROM symbol AS sym
            INNER JOIN daily_price AS dp
            ON dp.symbol_id = sym.id
            WHERE sym.ticker = '%s'
            ORDER BY dp.price_date ASC;"""
        
    sql = sql % symbol
    dataframe = pd.read_sql_query(sql, con=con, index_col='price_date')
    con.close()
    return dataframe

def ret_prices(symbol):
    con = db_connect()
    sql = """SELECT dp.price_date, dp.adj_close_price,
            dp.open_price, dp.high_price, dp.low_price,
            dp.close_price
            FROM symbol AS sym
            INNER JOIN daily_price AS dp
            ON dp.symbol_id = sym.id
            WHERE sym.ticker = '%s'
            ORDER BY dp.price_date ASC;"""
        
    sql = sql % symbol
    dataframe = pd.read_sql_query(sql, con=con, index_col='price_date')
    con.close()
    return dataframe

def ret_last_price_date(tickers):
    con = db_connect()
    sql = """SELECT dp.price_date, ticker
            FROM symbol AS sym
            INNER JOIN daily_price AS dp
            ON dp.symbol_id = sym.id
            WHERE sym.ticker = '%s'
            ORDER BY dp.price_date DESC LIMIT 1;"""
    tup_data = []
    it = 0
    printProgressBar(0, len(tickers), prefix = 'Retrieving data:', suffix = 'Done', length = 50)

    for t in tickers:
        try:
            symbol = t[-1]
            query = sql % symbol
            cursor = con.cursor()
            cursor.execute(query)
            data = cursor.fetchall()
            symbol = data[-1][1]
            date = data[-1][0]
            tup_data.append((t[0],date, symbol))
        except:
            pass
        it += 1
        printProgressBar(it,len(tickers), prefix='Retrieving data:', suffix='Done',length=50)
    con.close()
    return tup_data

def check_if_exists(tickers):
    con = db_connect()
    sql = """SELECT dp.price_date
            FROM symbol AS sym
            INNER JOIN daily_price AS dp
            ON dp.symbol_id = sym.id
            WHERE sym.ticker = '%s'
            ORDER BY dp.price_date DESC LIMIT 1;"""
    nexist = []
    it = 0
    printProgressBar(0, len(tickers), prefix = 'Checking DB:', suffix = 'Done', length = 50)

    for t in tickers:
        if len(t[-1]) < 2:
            query = sql % t
        else:
            query = sql % t[-1]
        cursor = con.cursor()
        cursor.execute(query)
        if len(cursor.fetchall()) <= 0:
            nexist.append(t)
        
        it += 1
        printProgressBar(it,len(tickers), prefix='Checking DB', suffix='Done',length=50)

    return nexist


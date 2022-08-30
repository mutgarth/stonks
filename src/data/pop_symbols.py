import yfinance as yf
import os
import csv
from db_connect import db_connect

parent_dir = os.path.dirname(os.getcwd())
data_dir = os.path.join(os.path.join(parent_dir,"data"), "b3symbols.csv")

def read_b3_symbols_list():
    with open(data_dir, newline='') as f:
        reader = csv.reader(f)
        data = list(reader)

    symbols = [''.join(x) for x in data]
    symbols = [x.strip() for x in symbols]

    symbols = [symbols[k].strip() +'.SA' for k in range(len(symbols))]

    return symbols

def get_companies_info(symbols):
    lista = []
    nlista = []
    i = 0
    for ticker in symbols:
        try:
            data = yf.Ticker(ticker)
            exchange = data.info.get('exchange')
            name = data.info.get('longName')
            sector = data.info.get('sector')
            industry = data.info.get('industry')
            currency = data.info.get('currency')

            lista.append([ticker,exchange, name,sector, industry, currency])
            i+=1
            print(ticker + ' has Finished --- ' + str(i) + ' of ' + str(len(symbols)))
        except:
            i+=1
            print(ticker + ' has Failed')
            nlista.append(ticker)
            pass

    return lista, nlista

def insert_symbol_data(con, exchange, ticker, name, sector, industry, currency):
    try:
        cursor = con.cursor()
        insert_query = """INSERT INTO symbol (exchange, ticker, name, sector, industry, currency) VALUES (%s, %s, %s, %s, %s, %s)"""
        record = (exchange, ticker, name, sector, industry, currency)        
        cursor.execute(insert_query, record)
        con.commit()
        print('successfully recorded')
    except:
        print('Failed to insert into DB table')
        pass

def check_symbol_db(con, symbol):

    return

if __name__ == '__main__':

    con = db_connect()
    symbols = read_b3_symbols_list()
    comp_info, err_log = get_companies_info(symbols=symbols)

    for i in range(0, len(symbols)):
        ticker = comp_info[i][0]
        exchange = comp_info[i][1]
        name = comp_info[i][2]
        sector = comp_info[i][3]
        industry = comp_info[i][4]
        currency = comp_info[i][5]

        insert_symbol_data(con, exchange, ticker, name, sector, industry, currency)




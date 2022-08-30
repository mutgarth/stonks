import yfinance as yf
from db_connect import db_connect
import datetime
import pandas_datareader.data as pdr

def update_tup_to_datetime(tup):
   lst = list(tup)
   lst[1] = lst[1].to_pydatetime().date()
   lst[2] = lst[2].to_pydatetime().date()
   lst[3] = lst[3].to_pydatetime().date()

   return tuple(lst)

def get_db_tickers():
    cursor = con.cursor()
    cursor.execute("SELECT id, ticker FROM symbol")
    data = cursor.fetchall()
    return [(d[0], d[1]) for d in data]

def get_hist_data(ticker):
    yf.pdr_override()
    trade_data = pdr.get_data_yahoo(ticker, period='max', interval='1d', group_by = 'column')
    return trade_data

def insert_price_db(daily_data):

    cursor = con.cursor()
    columns_name = """symbol_id, price_date, created_date, last_updated_date, open_price,
                    high_price, low_price, close_price, adj_close_price, volume"""
    insert_str = ("%s, "*10)[:-2]
    insert_query = "INSERT INTO daily_price (%s) VALUES (%s)" % (columns_name, insert_str)
    cursor.executemany(insert_query, daily_data)
    con.commit()


if __name__ == "__main__":
    
    con = db_connect()
    tickers = get_db_tickers()

    failed_tickers = []

    for t in tickers:
        print(t[1])
        try:
            daily_data = get_hist_data(t[1])
            
            now = datetime.datetime.utcnow()
            daily_data = daily_data.rename_axis('date').reset_index()
            daily_data.insert(0,'symbol_id',t[0])
            daily_data.insert(2,'created_date', now)
            daily_data.insert(3,'last_updated_date', now)

            daily_data = list(daily_data.itertuples(index=False, name=None))

            for i in range(0,len(daily_data)):
                daily_data[i] = update_tup_to_datetime(daily_data[i])  
            
        
            insert_price_db(daily_data)

        except Exception as e:
            print('Error for: %s' % e)
            failed_tickers.append(t[1])
            pass
            
    con.close()

    print(failed_tickers)




 

             
        

from db_retrieving import ret_last_price_date, ret_tickers
from pop_daily_price import insert_price_db, handling_nan, update_tup_to_datetime
from db_connect import db_connect
from utils import printProgressBar
import pandas as pd
import yfinance as yf
import datetime

def check_duplicates():
    con = db_connect()
    cursor = con.cursor()
    cursor.execute("""SELECT ticker FROM symbol
                GROUP BY ticker
                HAVING COUNT(*) > 1;""")

    duplicates = cursor.fetchall()
    con.close()
    return duplicates

def duplicates_to_remove(entry):
    con = db_connect()
    cursor = con.cursor()
    query = ("""SELECT id, ticker FROM symbol
                    WHERE ticker = '%s';""")
    cursor.execute(query % entry)
    tups = cursor.fetchall()
    con.close()
    return list(tups)[1:]

def execute_query(con, query):
    cursor = con.cursor()
    cursor.execute(query)
    con.commit()

def remove_duplicate(duplicates):
    con = db_connect()
    count = 0
    printProgressBar(0, len(duplicates), prefix = 'Removing duplicates:', suffix = 'Done', length = 50)
    for entry in duplicates:
        count +=1
        drop_list = duplicates_to_remove(entry)
        for i in drop_list:
            query = ("DELETE FROM symbol WHERE id = %s;" % i[0])
            execute_query(con,query)
            query = ("DELETE FROM daily_price WHERE symbol_id = %s;" % i[0])
            execute_query(con,query)
    con.close()

def check_last_db_day(today, db_date):
    if today.weekday() < 5:
        if today != db_date:
            return True
        else:
            return False
    else:
        tempdate = today - datetime.timedelta(days=today.weekday()-4)
        if tempdate != db_date:
            return True
        else:
            return False

if __name__ == '__main__':
    
    """ First thing is to check for any duplicate entries and remove them"""
    duplicates = check_duplicates()
    if len(duplicates) > 0:
        remove_duplicate(duplicates)

    tickers = ret_tickers()

    today = datetime.date.today()
    tups_date = ret_last_price_date(tickers)
    failed_tickers = []

    """
    In the tups_date:
        t[0] is ticker id
        t[1] is a date
        t[2] is the ticker
    """
    con = db_connect()
    for t in tups_date:
        if check_last_db_day(today,t[1]):
            try:
                nday = t[1]+datetime.timedelta(days=1)
                daily_data = yf.download(t[2], start=nday, end=today, interval='1d', group_by='column')
                if 'Empty DataFrame' in str(daily_data):
                    raise Exception()
                
                daily_data = handling_nan(daily_data)

                now = datetime.datetime.utcnow()
                daily_data = daily_data.rename_axis('date').reset_index()
                daily_data.insert(0,'symbol_id',t[0])
                daily_data.insert(2,'created_date', now)
                daily_data.insert(3,'last_updated_date', now)

                daily_data = list(daily_data.itertuples(index=False, name=None))

                for i in range(0,len(daily_data)):
                    daily_data[i] = update_tup_to_datetime(daily_data[i])  
            
                insert_price_db(con, daily_data)
            except Exception as e:
                print(e)
                failed_tickers.append(t[2])
                pass
    
    print('%s tickers have not been updated:' % len(failed_tickers))
    for fail in failed_tickers:
        print(fail)

    con.close()
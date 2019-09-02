import pandas as pd 
import requests as req
from bs4 import BeautifulSoup
from pymongo import MongoClient
import datetime

# Setup Database with pymongo
client = MongoClient()
db = client.dsebd_bak
daily_stock = db.daily_stock


# URL of the dse 
url = "https://www.dsebd.org/day_end_archive.php"

# Start and End date 
today = datetime.date.today()
start = str(today.year - 2) + "-" + str(today.month) + "-" + str(today.day)
end =  today

# Create a date range
dt_range = pd.date_range(start= start, end = end, periods = 24).strftime("%Y-%m-%d")

for i in range(len(dt_range)-1):
    #print(dt_range[i] + " and " + dt_range[i+1])
    form = {
        "DayEndSumDate1" : dt_range[i],
        "DayEndSumDate2" : dt_range[i+1],
        "Symbol" : "All Instrument",
        "ViewDayEndArchive" : "View Day End Archive"
    }
    html = BeautifulSoup(req.post(url, data = form).content, "lxml").select_one("body > table:nth-child(9) > tbody > tr > td:nth-child(2) > table > tbody > tr:nth-child(2) > td:nth-child(1) > table")
    df = pd.read_html(html.prettify())[0]
    df = df.rename(df.iloc[0], axis=1)[1:]
    df = df.drop('#', axis=1)
    df['Date'] = pd.to_datetime(df['DATE'])
    df = df.set_index('Date')
    daily_stock.insert_many(df.to_dict('records'))





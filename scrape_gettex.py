import requests
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import io
import zipfile

def gen_url(date):

    year = f"{date.year}" #2021
    # quarter = f"q{date.quarter}" #q1
    month = '{:02d}'.format(date.month) #01-04
    date_long = f"{date.year}" + '{:02d}'.format(date.month) + '{:02d}'.format(date.day) #20210104

    return f"https://www.gettex.de/fileadmin/rts27/MUNC-MUND/{year}/{month}/{date_long}_MUND.zip"


def get_file(date, url = "https://www.ls-tc.de/media/bestex/2021/q1/01-04/be_6_lssi_0001_20210104.csv.zip"):

    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"}
        r = requests.get(url, stream=True, headers=headers)
    except:
        print(f"{date.date} is not a workday?")

    # if not r.ok:
    #     raise Exception

    zip_content = zipfile.ZipFile(io.BytesIO(r.content))
    
    # No. 6 includes the orders
    # TODO: Skip first row since gettex is stupid
    df = pd.read_csv(zip_content.open(zip_content.namelist()[5]), sep=";")
    # drop unexecuted orders to save diskspace
    # df = df.drop(df[df["GeschaefteKurswert"]==0].index)
    df["date"] = date

    print(df)

    return df


def get_data(start_date, end_date):

    df = pd.DataFrame()

    date_range = pd.date_range(pd.to_datetime(start_date, dayfirst=True),pd.to_datetime(end_date, dayfirst=True),freq='d').tolist()
    
    for date in date_range:

        url = gen_url(date=date)
        print(url)
        try:
            file = get_file(date=date, url=url)
            df = df.append(file)
        except:
            pass


    df.to_csv(f"gettex_{start_date}_to_{end_date}.csv")


if __name__ == "__main__":
    get_data(start_date="03.01.2022", end_date="04.01.2022")
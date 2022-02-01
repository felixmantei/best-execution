import requests
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import io
import zipfile

def gen_url(date):

    year = f"{date.year}" #2021
    quarter = f"q{date.quarter}" #q1
    month_day = '{:02d}'.format(date.month) + "-" + '{:02d}'.format(date.day) #01-04
    date_long = f"{date.year}" + '{:02d}'.format(date.month) + '{:02d}'.format(date.day) #20210104

    return f"https://www.ls-tc.de/media/bestex/{year}/{quarter}/{month_day}/be_6_lssi_0001_{date_long}.csv.zip"


def get_file(date, url = "https://www.ls-tc.de/media/bestex/2021/q1/01-04/be_6_lssi_0001_20210104.csv.zip"):

    try:
        r = requests.get(url, stream=True)
    except:
        print(f"{date.date} is not a workday?")

    # if not r.ok:
    #     raise Exception

    zip_content = zipfile.ZipFile(io.BytesIO(r.content))
    
    df = pd.read_csv(zip_content.open(zip_content.namelist()[0]), sep=";")
    # drop unexecuted orders to save diskspace
    df = df.drop(df[df["GeschaefteKurswert"]==0].index)
    df["date"] = date

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


    df.to_csv(f"lus_{start_date}_to_{end_date}.csv")


if __name__ == "__main__":
    get_data(start_date="01.12.2020", end_date="10.12.2020")
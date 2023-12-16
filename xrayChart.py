from datetime import datetime
from google.cloud import firestore as firestore
import firebase_admin
from firebase_admin import db
from firebase_admin import storage
import json
import urllib3
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()
import matplotlib.pyplot as plt
import pandas as pd
import collections
import os
import gc

def hello_pubsub(event, context):
    if not firebase_admin._apps:
        firebase_admin.initialize_app(options={'databaseURL' : '<your-firebase-rtdb-url>'})
    dashRef = db.reference('dash')
    bucket = storage.bucket("carrington-9.appspot.com")
    primaryURL = 'https://services.swpc.noaa.gov/json/goes/primary/xrays-3-day.json'
    dataset = collections.OrderedDict()

    # GOES Primary Plot
    try:
        http = urllib3.PoolManager()
        priResp = http.request('GET', primaryURL)
        # clean & format data
        datap = priResp.data.decode('utf-8')
        priResp.release_conn()
        http.clear()
        pJSONp = json.loads(datap)
        del datap
        for n in pJSONp:
            t = n["time_tag"]
            if not t in dataset:
                dataset[t] = {}
            if n["energy"] == "0.1-0.8nm":
                dataset[t]['priHigh'] = n["flux"]
            elif n["energy"] == "0.05-0.4nm":
                dataset[t]['priLow'] = n["flux"]

        time_series = dataset.keys()
        times = []
        lowPri = []
        highPri = []
        lastHP = 0
        lastLP = 0
        for t in time_series:
            times.append(datetime.strptime(t, "%Y-%m-%dT%H:%M:%SZ"))
            if 'priLow' in dataset[t]:
                lastLP = dataset[t]['priLow']
                lowPri.append(dataset[t]['priLow'])
            else:
                lowPri.append(lastLP)
            if 'priHigh' in dataset[t]:
                lastHP = dataset[t]['priHigh']
                highPri.append(dataset[t]['priHigh'])
            else:
                highPri.append(lastHP)
         
        df = pd.DataFrame([times, highPri, lowPri], index=['Time', 'HighPri', 'LowPri']).T
        df = df.sort_values('Time')
        times = list(df.Time)
        highPri = list(df.HighPri)
        lowPri = list(df.LowPri)
    
        fig, ax = plt.subplots()
        ax.plot_date(times, highPri, color='red', marker='', linestyle='solid', linewidth=.2, label = 'GOES 16L')
        ax.plot_date(times, lowPri, color='blue', marker='', linestyle='solid', linewidth=.2, label = 'GOES 16S')
    
        ax.grid(True)
        ax.set_ylim([10**-9,10**-2])

        ax.set_yscale('log')
        ax.set_title('GOES X-Ray Flux (1 min)')
        ax.set_xlabel('UTC')
        ax.set_ylabel('Watts - m^-2')
        plt.figtext(0.99, 0.01, 'X-Ray data from swpc.noaa.gov. Plot by "9 RESE LLC" for spaceweathernews.com', horizontalalignment='right', fontsize='x-small')
        plt.figtext(.94, .2, 'GOES 16 0.1-0.8nm  ', fontsize=8, rotation=90, color='red')
        plt.figtext(.97, .2, 'GOES 16 0.05-0.4nm', fontsize=8, rotation=90, color='blue')
        plt.figtext(.905, .25, 'A', fontsize=12)
        plt.figtext(.905, .36, 'B', fontsize=12)
        plt.figtext(.905, .47, 'C', fontsize=12)
        plt.figtext(.905, .59, 'M', fontsize=12)
        plt.figtext(.905, .7, 'X', fontsize=12)
        plt.savefig('/tmp/goes_xray.png')
        try:
            with open('/tmp/goes_xray.png', "rb") as gop_file:
                try:
                    blob = bucket.blob('goes_xray.png')
                    blob.upload_from_filename('/tmp/goes_xray.png')
                except Exception as e:
                    print('upload_blob Error', e)
            gop_file.close()
            os.remove('/tmp/goes_xray.png')
            blob = bucket.blob('goes_xray.png')
            blob.make_public()
        except Exception as e:
            print('bucket Error', e)
    except Exception as e:
        print("Plot Error: ", e)
    
    del dataset
    del pJSONp
    del time_series
    del df
    del blob
    times = []
    lowPri = []
    highPri = []
    lastHP = 0
    lastLP = 0
    gc.collect()
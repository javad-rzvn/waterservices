import os
import tempfile
import urllib
import requests
from tqdm.auto import tqdm
import pandas as pd
import numpy as np
import dask.dataframe as dd
import csv

class NWIS:
    def __init__(self, siteType, hucList, filters, csvFilename):
        self.siteType = siteType
        self.hucList = hucList
        self.csvFilename = csvFilename
        self.filters = filters

    
    def siteInfo(self):
        fd, path = tempfile.mkstemp(suffix=".txt", prefix="tmp")
        all_lines = []
        try:
            with os.fdopen(fd, 'r+') as tmp:
                for huc in tqdm(self.hucList):
                    filter0 = 'format=rdb'
                    filter1 = f'&huc={huc}'
                    filter2 = f'&siteType={self.siteType}'
                    filter3 = '&seriesCatalogOutput=true'
                    URL = f"https://waterservices.usgs.gov/nwis/site/?{filter0}{filter1}{filter2}{filter3}"
                    r = requests.get(URL)

                    string_to_add = ''
                    file_lines = [''.join([x.strip(), string_to_add, '\n']) for x in r.text.splitlines() if "#" not in x]
                    all_lines.extend(file_lines)
                tmp.writelines(all_lines)
                tmp.flush()
                tmp.seek(0)
                
                usecols = ['site_no', 'station_nm', 'dec_lat_va', 'dec_long_va', 'huc_cd', 'begin_date', 'end_date']
                df_grouped = dd.read_csv(path, sep="\t", usecols=usecols).compute()
                df_grouped.drop(df_grouped.loc[df_grouped['site_no']=='15s'].index, inplace=True)
                df_grouped.drop(df_grouped.loc[df_grouped['site_no']=='site_no'].index, inplace=True)
                df_grouped.to_csv(f'{self.csvFilename}.csv', index=False)
        finally:
            os.remove(path)
# WATERSERVICES

A pyhton package to work with WaterServices USGS

My Personal Website: [Water Directory](https://waterdirectory.ir/).


To import, use command below:

```bash
from waterservices import NWIS
```

Available functions:
- siteInfo()


## Get a csv file for site info of any type

```bash
columns = ['site_no', 'station_nm', 'dec_lat_va', 'dec_long_va', 'huc_cd', 'data_type_cd',
        'parm_cd', 'stat_cd', 'begin_date', 'end_date']
filters = {
    'seriesCatalogOutput': 'true',
    'outputDataTypeCd': 'dv,pk,gw',
    'siteStatus': 'all',
    'hasDataTypeCd': 'dv,gw'
}
nwis = NWIS('GW', ['01'], filters, columns, 'wells').siteInfo()
```

You can customize columns, filters, stationType, and HUC code.
# ETL Project - _Eric Franco_
##_Combining US Natural Gas Storage and Temperature Data_

This project 
This project will created a database of:

1. Historical population-weighted US temperature data from National Oceanic and Atmospheric Administration’s (NOAA) Climate Prediction Center (CPC)

2. Historical natural gas storage data reported by the Energy Information administration. This data reports both the total amount of working gas in US storage facilities at the end of each week, as well as the week over week change in inventory. 



### Data sources
1. **EIA Natural Gas Storage Data** was pulled using the [EIA's API](https://www.eia.gov/opendata/), which reports data in a JSON format

2. **Temperature Data** was pulled from the NOAA CPC website. The files are posted on an [FTP Site](ftp://ftp.cpc.ncep.noaa.gov/htdocs/degree_days/weighted/daily_data/) in a .txt format


### Extraction
**EIA Storage Data**
Pulled using requests package. A function was created to iterate through a dictionary that mapped weekly inventory reported for each US region to their unique serires ids
``` python
eia_api_series_ids = {
    'l48' : 'NG.NW2_EPG0_SWO_R48_BCF.W',
    'east' : 'NG.NW2_EPG0_SWO_R31_BCF.W',
    'midwest' : 'NG.NW2_EPG0_SWO_R32_BCF.W',
    'mountain' : 'NG.NW2_EPG0_SWO_R34_BCF.W',
    'pacific' : 'NG.NW2_EPG0_SWO_R35_BCF.W',
    'south_central' : 'NG.NW2_EPG0_SWO_R33_BCF.W',
    'salt' : 'NG.NW2_EPG0_SSO_R33_BCF.W',
    'nonsalt': 'NG.NW2_EPG0_SNO_R33_BCF.W'
}
```
The keys of this dictionary were then used to generate keys in a new dictionary storing the JSON object for each regional time series
``` python
call_dict = {}

def generate_call(n):
    return json.loads(requests.get(base_url + eia_api_series_ids[n]).text)

for n in eia_api_series_ids:
    call_dict[n] = generate_call(n)
```
Finally, a function was used to convert each of the above-mentione JSON objects into a dictionary of pandas DataFrames
``` python
df_dict = {}

def generate_df(call):
   return pd.DataFrame(call['series'][0]['data'])

 for n in call_dict:
   df_dict[n] = generate_df(call_dict[n])
```

**CPC Temperature Data** was pulled from the CPC's FTP site using the pandas .read_csv method. a for loop was used to iterate through each of the yearly pages of the FTP site going back to 1981. this operation was performed for both the pages reporting heating degree days (HDDs) and cooling degree days (CDDs). As an initial step, each annual dataset was concatenated together, the DataFrame index was set to daily datetime values, and the final DataFrame was pared down to only include column 9 of each dataset, which captures only population weighted degree days for the entire US.

_HDD pull:_
``` python
list_ = []
for x in range(1981,datetime.datetime.now().year+1):
    df = pd.read_csv(f'ftp://ftp.cpc.ncep.noaa.gov/htdocs/degree_days/weighted/daily_data/{x}/Population.Heating.txt',skiprows = 3, delimiter = '|').T
    df.drop(df.index[0], inplace = True)
    list_.append(df)
CPC_HDDs = pd.concat(list_)
CPC_HDDs.index = pd.to_datetime(CPC_HDDs.index)
CPC_HDDs = CPC_HDDs[9]
CPC_HDDs = pd.DataFrame(CPC_HDDs)
CPC_HDDs.columns = ['HDDs']


```


_CDD pull:_
``` python
list_ = []
for x in range(1981,datetime.datetime.now().year+1):
    df = pd.read_csv(f'ftp://ftp.cpc.ncep.noaa.gov/htdocs/degree_days/weighted/daily_data/{x}/Population.Cooling.txt',skiprows = 3, delimiter = '|').T
    df.drop(df.index[0], inplace = True)
    list_.append(df)
CPC_CDDs = pd.concat(list_)
CPC_CDDs.index = pd.to_datetime(CPC_CDDs.index)
CPC_CDDs = CPC_CDDs[9]
CPC_CDDs = pd.DataFrame(CPC_CDDs)
CPC_CDDs.columns = ['CDDs']
```


### Transformation
**EIA Data**

**Temperature Data**




### Loading and Database configuration
pandas .to_csv method was used to send xxxx to a local SQLite .db file

discuss setting the schema

show python code



### Discussion, conclusions, etc.

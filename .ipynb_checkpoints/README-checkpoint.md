# ETL Project - _Eric Franco_
## _Collecting US Natural Gas Storage and Population-Weighted Temperature Data_

This project extracts and transforms US natural gas and temperature data in order to create a database that reports two key pieces of information required to understand the natural gas market:

1. **Temperature Data**: Historical population-weighted US temperature data from National Oceanic and Atmospheric Administration’s (NOAA) Climate Prediction Center (CPC). 

	Temperature data is reported as _population weighted degree days_. Degree days reflect the temperature deviation from a 65°F base: cooling degree days reflect each degree _above_ 65°F while cooling degree days reflect each degree _below_ 65°F. For example, a 7 day period that reports constant temperatures of 64°F would total 7 heating degree days, whie a 7 day period with constant temperatures of 66°F would total 7 cooling degree days.

	Degree day statistics are reported for 9 Census regions, but for purposes of this analysis, the script only pulls data for the Lower 48 US.![Census Regions](https://www.cpc.ncep.noaa.gov/products/analysis_monitoring/cdus/degree_days/degree_day_regions.gif)

2. **Storage Data** Historical natural gas storage data reported by the Energy Information Administration. This data reports both the total amount of working gas in US storage facilities at the end of each week for across the Lower 48 US as well as five sub-regions shown below. Inventory data is reported in billion cubic feet (Bcf)![Storage Regions](http://ir.eia.gov/ngs/ngsmap.png)

All operations are conducted in Python using the requests libary to access the relevant API and FTP urls, pandas to transform the data, and SQLAlchemy to push the data to a .db file.

### Data sources
1. **EIA Natural Gas Storage Data** was pulled using the [EIA's API](https://www.eia.gov/opendata/), provided in JSON format

2. **Temperature Data** was pulled from the NOAA CPC website. The files are posted on an [FTP Site](ftp://ftp.cpc.ncep.noaa.gov/htdocs/degree_days/weighted/daily_data/) in a .txt format


### Extraction
**EIA Storage Data**
Pulled using requests package. A function was created to iterate through a dictionary that mapped weekly inventory reported for each US region to their unique series ids
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
**EIA Data**: Applies the following function on on the regional storage DataFrames to:

1. label each column
2. convert the 'week' column to datetime format
3. set the 'week column' to the index
4. create a 'REGION_change' column that reflects the week-over-week change in storage inventory (in Bcf)
5. subtract a single day from the 'week' column to accurately  reflect that each week of data ends on Thursday
	(_the EIA labels each week on dates ending friday, although the data itself reflects week running from Friday - Thursday_)
6. merge all regional data into a single dataframe

``` python
def format_df(n):
    df_dict[n].columns=['week',f'{n}_inventory']
    df_dict[n]['week'] = pd.to_datetime(df_dict[n]['week'])
    df_dict[n] = df_dict[n].sort_values('week')
    df_dict[n] = df_dict[n].set_index('week')
    df_dict[n][f'{n}_change'] = df_dict[n][f'{n}_inventory'] - df_dict[n][f'{n}_inventory'].shift(1)
    df_dict[n].index = df_dict[n].index - datetime.timedelta(days=1)

for n in df_dict:
    format_df(n)

eia_storage_data = df_dict['l48'].merge(
    df_dict['east'], left_index = True, right_index = True).merge(
    df_dict['midwest'], left_index = True, right_index = True).merge(
    df_dict['mountain'], left_index = True, right_index = True).merge(
    df_dict['pacific'], left_index = True, right_index = True).merge(
    df_dict['south_central'], left_index = True, right_index = True).merge(
    df_dict['salt'], left_index = True, right_index = True).merge(
    df_dict['nonsalt'], left_index = True, right_index = True)
 ```

**Temperature Data**: Daily temperature data is manipulated to:
1. merge the HDD and CDD data into a single DataFrame
2. add a 'TDD' column that reports total degree days (the sum of heating + cooling degree days for each day)
3. resample the daily values to report total degree days for each week ending Thursday

``` python
CPC_TDDs = pd.merge(CPC_HDDs,CPC_CDDs, left_index = True, right_index = True)
CPC_TDDs['TDDs'] = CPC_TDDs['HDDs'] + CPC_TDDs['CDDs']
CPC_TDDs_Weekly = CPC_TDDs.resample('W-Thu').sum()

```

Finally, both DataFrames reporting weather and storage data are merged

``` python
combined_df = pd.merge(eia_storage_data,CPC_TDDs_Weekly, left_index = True, right_index = True)
```




### Loading and Database configuration
Utilizes the pandas .to_csv method  to send the combined dataset to a local SQLite .db file

``` python
class StorageWeather(Base):
    __tablename__ = 'storage_weather'
    week = Column(Date, primary_key = True)
    l48_inventory = Column(Integer)
    l48_change = Column(Integer)
    east_inventory = Column(Integer)
    east_change = Column(Integer)
    midwest_inventory = Column(Integer)
    midwest_change = Column(Integer)
    mountain_inventory = Column(Integer)
    mountain_change = Column(Integer)
    pacific_inventory = Column(Integer)
    pacific_change = Column(Integer)
    south_central_inventory = Column(Integer)
    south_central_change = Column(Integer)
    salt_inventory = Column(Integer)
    salt_change = Column(Integer)
    nonsalt_inventory = Column(Integer)
    nonsalt_change = Column(Integer)
    HDDs = Column(Integer)
    CDDs = Column(Integer)
    TDDs = Column(Integer)

engine = create_engine('sqlite:///storage.db')

Base.metadata.create_all(engine)

from sqlalchemy.orm import Session
session = Session(bind=engine)

combined_df.to_sql('storage_weather', schema = Base.metadata.create_all(engine), con = engine, index_label = 'week', if_exists = 'replace')
```


### Discussion, conclusions, etc.

This final datasource can be used to develop a model to (1) forecast US natural gas storage activity using weekly weather outcomes and (2) weather-adjust historical weather outcomes to develop a baseline understanding of the structural balance of the US natural gas economy. The image below says it all: weather outcomes are a major driver of US natural gas storage withdrawals and injections.

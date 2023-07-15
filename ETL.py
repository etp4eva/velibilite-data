import requests, logging, datetime, sys, sqlite3, json, os
from shapely.geometry import Polygon, Point
import fiona
from geopandas import GeoDataFrame, read_file
import pandas as pd

velib_url = 'https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_status.json'
stations_url = 'https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_information.json'

working_folder = os.path.dirname(__file__)
source_data_folder = working_folder + '\\data\\'
out_data_folder = working_folder + '\\output\\'

arrondissments_path = source_data_folder + 'arrondissements.geojson'
quartiers_path = source_data_folder + 'quartier_paris.geojson'
communes_path = source_data_folder + 'communes-dile-de-france-au-01-janvier.geojson'
stations_path = source_data_folder + 'stations.json'
sqlite_path = source_data_folder + 'data.db'

outstn_path = out_data_folder + 'stations.geojson' 

current_dt = datetime.datetime.now()

log_folder = working_folder + '\\logs\\'
log_file = current_dt.strftime('%Y_%m.log')
log_path = log_folder + log_file
logging.basicConfig(
    filename=log_path, 
    encoding='utf-8', 
    level=logging.DEBUG,
    format='%(asctime)s : %(levelname)s : %(message)s'
    )

## Loading live data
logging.info('Getting velib status...')
r = requests.get(velib_url)

if r.status_code != 200:
    logging.error('Failed. Status {}'.format(r.status_code))
    sys.exit(1)

newdata_json = r.json()

logging.info('Getting stations status...')
r = requests.get(stations_url)

if r.status_code != 200:
    logging.error('Failed. Status {}'.format(r.status_code))
    sys.exit(1)

stations_json = r.json()

## Loading stations ##
logging.info('Getting stations data...')

stations_enriched = {}

if os.path.exists(stations_path):
    logging.info('Stations data found.')

    with open(stations_path) as json_file:
        stations_enriched = json.load(json_file)
else:
    logging.info('Stations data not found.')

## Check for new stations not in enriched data ##
logging.info('Checking for stations changes...')

stations_to_update = []

for station in stations_json['data']['stations']:
    station_id = station['station_id']

    if station_id in stations_enriched:
        if station != stations_enriched[station_id]['base']:
            logging.info('Updating station {}...'.format(station_id))
            stations_enriched[station_id]['base'] = station
            stations_to_update.append(station)
    else:
        logging.info('Adding station {}...'.format(station_id))
        stations_enriched[station_id] = {
            'station_id': station_id,
            'neighbourhood_id': None,
            'arrondissment_id': None,
            'commune_id': None,
            'base': station
        }
        stations_to_update.append(station)

## Update neighbourhood and arrondissment ids ##

def isNaN(num):
    return num != num

arrond_df = read_file(arrondissments_path)
quarti_df = read_file(quartiers_path)
commune_df = read_file(communes_path)

if len(stations_to_update) > 0:
    logging.info('Loading zone geometry...')

    point_df = GeoDataFrame(
        [
            {
                "geometry": Point(s['lon'], s['lat']), 
                "station_id": s['station_id']
            } for s in stations_to_update
        ]
    )

    point_df.crs = arrond_df.crs

    logging.info('Calculating zone ids...')
    point_df = point_df.sjoin(arrond_df, how='left')
    point_df = point_df[['geometry', 'station_id', 'c_arinsee']]
    point_df = point_df.sjoin(quarti_df, how='left')
    point_df = point_df[['geometry', 'station_id', 'c_arinsee', 'c_quinsee']]
    point_df = point_df.sjoin(commune_df, how='left')
    point_df = point_df[['geometry', 'station_id', 'c_arinsee', 'c_quinsee', 'insee']]

    for index, station in point_df.iterrows():
        station_id = station['station_id']
        
        try:
            stations_enriched[station_id]['neighbourhood_id'] = int(station['c_quinsee'])
        except:
            stations_enriched[station_id]['neighbourhood_id'] = None

        try:            
            stations_enriched[station_id]['arrondissment_id'] = int(station['c_arinsee'])
        except:
            stations_enriched[station_id]['arrondissment_id'] = None

        try:
            stations_enriched[station_id]['commune_id'] = int(station['insee'])
        except:
            stations_enriched[station_id]['commune_id'] = None


## Write updated enriched data ##

with open(stations_path, 'w') as jfile:
    json.dump(stations_enriched, jfile)

## Connecting to past data db ##

logging.info('Connecting to historic data...')
con = sqlite3.connect(sqlite_path)
c = con.cursor()

c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='stations' ''')

# if the count is 1, then table exists
if c.fetchone()[0] != 1:
    logging.info('Stations table does not exist. Creating...')
    c.execute('''
        CREATE TABLE stations(
            station_id INTEGER NOT NULL,
            neighbourhood_id INTEGER,
            arrondissement_id INTEGER,
            commune_id INTEGER NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            day INTEGER NOT NULL,
            time_hr INTEGER NOT NULL,
            green_count INTEGER,
            blue_count INTEGER
        );
    ''')

day_of_week = current_dt.weekday
hour_of_day = current_dt.hour

def recalc_avg(avgOld, valNew, sizeNew):
    return avgOld + ((valNew - avgOld) / sizeNew)

## Load up new data points ##
for station in newdata_json['data']['stations']:
    station_id = station['station_id']
    mech = station['num_bikes_available_types'][0]['mechanical']
    ebike = station['num_bikes_available_types'][1]['ebike']
    
    se = stations_enriched[station_id]
    total_docks = station['num_bikes_available'] + station['num_docks_available']

    if total_docks != 0:
        pct_avail = station['num_bikes_available'] / total_docks
    else:
        pct_avail = 0

    params = (
        station_id, se['neighbourhood_id'], se['arrondissment_id'],
        se['commune_id'], current_dt, current_dt.weekday(), current_dt.hour,
        mech, ebike
    )
    c.execute('''
        INSERT INTO stations(
            station_id, neighbourhood_id, arrondissement_id,
            commune_id, timestamp, day, time_hr,
            green_count, blue_count
        ) VALUES (
            ?,?,?,?,?,?,?,?,?
        );''', params)

con.commit()

df_station = pd.read_sql_query('''
    SELECT 
        station_id, neighbourhood_id, arrondissement_id, commune_id, 
        day, time_hr, 
        AVG(green_count) as green_avg, AVG(blue_count) as blue_avg
    FROM stations
    GROUP BY station_id, day, time_hr
''', con)

stnout_df = GeoDataFrame(
    [
        {
            "geometry": Point(s['lon'], s['lat']), 
            "station_id": s['station_id']
        } for s in stations_json['data']['stations']
    ]
)

stnout_json = json.loads(stnout_df.to_json())

for idx, stn in enumerate(stnout_json['features']):
    stnid = stn['properties']['station_id']
    stn_data = df_station[(df_station['station_id'] == int(stnid))]

    stn['properties']['values'] = { 
        dow: {
            hod: {
                'green_avg': 0,
                'blue_avg': 0,
            } for hod in range(0, 23)
        } for dow in range(0, 6)
    }

    for ind,row in stn_data.iterrows():
        dow = int(row['day'])
        hod = int(row['time_hr'])

        stn['properties']['values'][dow][hod] = {
            'green_avg': row['green_avg'],
            'blue_avg': row['blue_avg']
        }
    
    stnout_json['features'][idx] = stn

with open(outstn_path, 'w') as of:
    json.dump(stnout_json, of)

zone_query = '''
    SELECT 
        {0}, day, time_hr, 
        AVG(green_sum) as green_avg, AVG(blue_sum) as blue_avg
    FROM (
        SELECT
            {0}, timestamp, day, time_hr, 
            SUM(green_count) as green_sum, SUM(blue_count) as blue_sum
        FROM stations
        WHERE {0} IS NOT NULL
        GROUP BY {0}, timestamp
    )
    GROUP BY {0}, day, time_hr
'''

zone_keys = [
    ('neighbourhood_id', 'c_quinsee', quarti_df, out_data_folder + 'nhood.geojson'), 
    ('arrondissement_id', 'c_arinsee', arrond_df, out_data_folder + 'arrond.geojson'), 
    ('commune_id', 'insee', commune_df, out_data_folder + 'commune.geojson'),
]

for keys in zone_keys:
    zone_id_key = keys[0]
    zone_key = keys[1]
    odf = keys[2]
    out_path = keys[3]

    df = pd.read_sql_query( zone_query.format(zone_id_key), con )   

    zone_json = json.loads(odf.to_json())

    remove_idx_list = []

    for idx, feat in enumerate(zone_json['features']):
        zid = feat['properties'][zone_key]
        nhood = df[(df[zone_id_key] == int(zid))]

        if nhood.empty:
            remove_idx_list.append(idx)
            continue

        feat['properties']['values'] = { 
            dow: {
                hod: {
                    'green_avg': 0,
                    'blue_avg': 0,
                } for hod in range(0, 23)
            } for dow in range(0, 6)
        }

        for dow in range(0,6):
            for hod in range(0,23):
                filtered = nhood[(df['day'] == dow) & (df['time_hr'] == hod)]
                
                if filtered.empty:
                    feat['properties']['values'][dow][hod] = {
                        'green_avg': 0,
                        'blue_avg': 0,
                    }
                else:
                    green_avg, blue_avg = filtered.iloc[0].tolist()[3:]
                    
                    feat['properties']['values'][dow][hod] = {
                        'green_avg': green_avg,
                        'blue_avg': blue_avg,
                    }

        zone_json['features'][idx] = feat

    zone_json['features'] = [
        zone_json['features'][i] for i in range(len(zone_json['features'])) if i not in remove_idx_list
    ]

    with open(out_path, 'w') as of:
        json.dump(zone_json, of)

c.close()
con.close()
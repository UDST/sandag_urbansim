import numpy as np
import pandas as pd
import cStringIO
import psycopg2
import pandas.io.sql as sql
from spandex import TableLoader
loader = TableLoader()

conn_string = "host='urbancanvas.cp2xwchuariu.us-west-2.rds.amazonaws.com' dbname='sandag_testing' user='sandag' password='PASSWORD' port=5432"
conn=psycopg2.connect(conn_string)
cur = conn.cursor()

def uc_db_to_df(query):
    return sql.read_frame(query, conn)
    
parcels = uc_db_to_df("select parcel_id, zoning_id, devtype_id as development_type_id from parcel "
                      "where projects = '{1}' and valid_from = '{-infinity}';").set_index('parcel_id')
buildings = uc_db_to_df("SELECT building_id, parcel_id, building_type_id as development_type_id, improvement_value, "
                        "residential_units, non_residential_sqft, stories, year_built, residential_sqft, "
                        "note FROM building where projects = '{1}' and valid_from = '{-infinity}';").set_index('building_id')
                        
# Put tables in HDF5
h5_path = loader.get_path('out/sandag.h5')
store = pd.HDFStore(h5_path)

del store['buildings']
store['buildings'] = buildings

p_prev = store.parcels.copy()
p_prev['zoning_id'] = parcels.zoning_id
p_prev['development_type_id'] = parcels.development_type_id
del store['parcels']
store['parcels'] = p_prev

store.close()
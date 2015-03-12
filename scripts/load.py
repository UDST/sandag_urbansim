import logging

import pandas as pd,  numpy as np
from pandas.io.excel import read_excel
from spandex import TableLoader
from spandex.io import df_to_db, logger
from spandex.spatialtoolz import conform_srids, tag

logger.setLevel(logging.INFO)

shapefiles = {
    'staging.parcels':
    'space/parcel.shp',

    'staging.buildings':
    'space/building.shp',

    'staging.blocks':
    'space/tl_2010_06073_tabblock10.shp',

    'staging.sitespec':
    'scheduled/site_spec.shp',

}

# Install PostGIS and create staging schema.
loader = TableLoader()
with loader.database.cursor() as cur:
    cur.execute("""
        CREATE EXTENSION IF NOT EXISTS postgis;
        CREATE SCHEMA IF NOT EXISTS staging;
    """)
loader.database.refresh()

t = loader.tables
staging = loader.tables.staging


# Load shapefiles specified above to the project database.
loader.load_shp_map(shapefiles)


# Fix invalid geometries and reproject.
conform_srids(loader.srid, schema=staging, fix=True)


# Tag parcels with a block_id
tag(t.staging.parcels, 'block_geoid', t.staging.blocks, 'geoid10')


# Load csv's
csvs = {
    'local_effect_distances':
    'space/local_effect_distances.csv',

    'sqft_per_job_by_devtype':
    'employment/sqftPerEmpByDevType.csv',

    'sqft_per_job_by_activity_by_devtype':
    'employment/sqftPerEmpByActivityByDevType.csv',

    'jobs_lehd':
    'employment/jobs_lehd_raw.csv',

    'households':
    'population/household.csv',

    'assessor_home_transactions':
    'price/priceDataSet.csv',

    'costar2012':
    'price/costar2012.csv',

    'costar_transactions':
    'price/costarTransactionHistory.csv',

    'zoning':
    'zoning/zoning.csv',

    'zoning_allowed_uses':
    'zoning/zoning_allowed_uses.csv',

}

for tbl in csvs.iterkeys():
    csv = loader.get_path(csvs[tbl])
    df = pd.read_csv(csv)
    df.index.name = 'index'
    if df.isnull().sum().sum() > 0:
        for col in df.dtypes.iteritems():
            col_name = col[0]
            col_type = col[1]
            firstval = df[col_name].loc[0]
            if firstval in (True, False):
                if type(firstval) == bool:
                    df[col_name] = df[col_name].fillna(False)
            if col_type == np.int64:
                df[col_name] = df[col_name].fillna(0)
            elif col_type == np.float64:
                df[col_name] = df[col_name].fillna(0.0)
            elif col_type == np.object:
                df[col_name] = df[col_name].fillna(' ')
    if 'id' in df.columns:
        new_id_colname = tbl + '_id'
        df = df.rename(columns = {'id':new_id_colname})
    df_to_db(df, tbl, schema = staging)


# Load excel
# xls_path = loader.get_path('scheduled/scheduled_development.xlsx')
# df = pd.read_excel(xls_path)
# df_to_db(df, 'scheduled_development', schema = staging)


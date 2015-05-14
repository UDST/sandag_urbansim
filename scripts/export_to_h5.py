import pandas as pd, numpy as np
import pandas.io.sql as sql
from spandex import TableLoader

loader = TableLoader()

def db_to_df(query):
    """Executes SQL query and returns DataFrame."""
    conn = loader.database._connection
    return sql.read_frame(query, conn)

# Read from database (public schema)
parcels = db_to_df('select * from parcels').set_index('parcel_id')
jobs = db_to_df('select * from jobs').set_index('job_id')
households = db_to_df('select * from households').set_index('household_id')
buildings = db_to_df('select * from buildings').set_index('building_id')
hh_controls = db_to_df('select year, activity_id, luz_id, total_number_of_households from annual_household_control_totals')
pecas_prices = db_to_df('select year, luz_id, development_type_id, price from pecas_prices')
assessor_transactions = db_to_df('select * from assessor_transactions').set_index('building_id')
zoning = db_to_df('select * from zoning')
zoning_allowed_uses = db_to_df('select zoning_id, development_type_id from zoning_allowed_uses')
fee_schedule = db_to_df('select fee_schedule_id, development_type_id, development_fee_per_unit_space_initial from staging.fee_schedule')
parcel_fee_schedule = db_to_df('select parcelid as parcel_id, fee_schedule_id, portion from staging.parcel_fee_schedule')


# Remove uneccesary id columns appended by spandex
for df in [buildings, jobs, households, assessor_transactions, zoning]:
    if 'id' in df.columns:
        del df['id']
        
zoning = zoning.set_index('zoning_id')

# Get OSM nodes and edges for Pandana
nodes_path = loader.get_path('travel/nodes.csv')
edges_path = loader.get_path('travel/edges.csv')
nodes = pd.read_csv(nodes_path).set_index('node_id')
edges = pd.read_csv(edges_path)
nodes.index.name = 'index'

# Get price datasets
costar = db_to_df('select * from public.costar')
if 'id' in costar.columns:
    del costar['id']

# Put tables in HDF5
h5_path = loader.get_path('out/sandag.h5')

store = pd.HDFStore(h5_path)
store['edges'] = edges
store['nodes'] = nodes
store['parcels'] = parcels
store['buildings'] = buildings
store['households'] = households
store['jobs'] = jobs
store['costar'] = costar
store['annual_household_control_totals'] = hh_controls
store['pecas_prices'] = pecas_prices
store['assessor_transactions'] = assessor_transactions
store['zoning'] = zoning
store['zoning_allowed_uses'] = zoning_allowed_uses
store['fee_schedule'] = fee_schedule
store['parcel_fee_schedule'] = parcel_fee_schedule

store.close()


#Scheduled development events
sched_dev = db_to_df('select * from scheduled_development_events').set_index('scheduled_development_event_id')
del sched_dev['id']
sched_dev.to_csv('../data/scheduled_development_events.csv')
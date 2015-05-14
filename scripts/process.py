import pandas as pd, numpy as np
import pandas.io.sql as sql
from spandex.io import exec_sql,  df_to_db
from spandex import TableLoader
from urbansim.models.lcm import unit_choice
import datetime

loader = TableLoader()

def db_to_df(query):
    """Executes SQL query and returns DataFrame."""
    conn = loader.database._connection
    return sql.read_frame(query, conn)


##There are some duplicate parcel_id's in local_effect_distances, delete one record from each duplicate pair
delete_dup_parcel_id_sql = '''
with a as (select parcel_id, count(*) as numparcels from staging.local_effect_distances group by parcel_id)
, b as(
select parcel_id from a where numparcels > 1 --These are the parcel_id's where one of two of each pair needs to be removed
)
, c as(
select max(ctid) as ctid_max from staging.local_effect_distances where parcel_id in (select parcel_id from b) group by parcel_id  --These are the ctid's to delete
)
delete from staging.local_effect_distances
where ctid in (select ctid_max from c);
'''
exec_sql(delete_dup_parcel_id_sql)


#Join parcels to local_effect_distances
parcel_join_sql = '''
DROP table if exists public.parcels;
SELECT a.*, b.distance_to_coast, b.distance_to_freeway, b.distance_to_onramp,  b.distance_to_park, b.distance_to_school, b.distance_to_transit into public.parcels from staging.parcels a join staging.local_effect_distances b on a.parcel_id = b.parcel_id;
ALTER TABLE public.parcels DROP COLUMN gid;
ALTER TABLE public.parcels RENAME COLUMN developmen TO development_type_id;
ALTER TABLE public.parcels RENAME COLUMN parcel_acr TO parcel_acres;
ALTER TABLE public.parcels RENAME COLUMN proportion TO proportion_undevelopable;
'''
exec_sql(parcel_join_sql)


#XY coords
exec_sql("alter table public.parcels add centroid geometry;")
exec_sql("update public.parcels set centroid = ST_centroid(geom);")
exec_sql('ALTER TABLE public.parcels ADD x numeric;')
exec_sql('ALTER TABLE public.parcels ADD y numeric;')
exec_sql("update public.parcels set x = ST_X(ST_Transform(centroid, 4326));")
exec_sql("update public.parcels set y = ST_Y(ST_Transform(centroid, 4326));")


#Get tables
parcels = db_to_df('select * from public.parcels;')
parcels = parcels.set_index('parcel_id')

buildings = db_to_df('select * from staging.buildings;')
#Add mgra_id and block_id to the buildings table for demand agent allocation purposes
##We lose 2 buildings in this merge-  2 buildings have invalid parcel_ids
buildings = pd.merge(buildings, parcels[['mgra_id', 'block_geoid']], left_on = 'parcel_id', right_index = True)
buildings = buildings.rename(columns = {'bid':'building_id', 'dev_typeid':'development_type_id', 'imprvvalue':'improvement_value',
                                        'res_unit':'residential_units', 'nonressqft':'non_residential_sqft', 'ressqft':'residential_sqft',
                                        'year_build':'year_built', 'block_geoid':'block_id'})
buildings = buildings.set_index('building_id')
del buildings['gid']
del buildings['objectid']
del buildings['shape_leng']
del buildings['shape_ar_1']
del buildings['price_per_']
del buildings['geom']
buildings.block_id = buildings.block_id.astype('int64')
buildings['job_spaces'] = np.round(buildings.non_residential_sqft/200).astype('int')

households = db_to_df('select * from staging.households;').set_index('lu_hh_id')
del households['index']
del households['id']
households.index.name = 'household_id'
households.building_id = -1
households = households.rename(columns = {'mgra13':'mgra_id'})

jobs = db_to_df('select * from staging.jobs_lehd;').set_index('job_id')
del jobs['index']
del jobs['id']
jobs.building_id = -1


## Capacity-constrained random allocation of agents
def random_allocate_agents(agents, buildings, zone_id_col_name, capacity_col_name):

    ## Universe of alternatives
    empty_units = buildings[buildings[capacity_col_name] > 0][capacity_col_name].order(ascending=False)
    alternatives = buildings[['development_type_id', 'parcel_id', zone_id_col_name]]
    alternatives = alternatives.ix[np.repeat(empty_units.index.values,empty_units.values.astype('int'))]

    ## Agents by zone
    taz_agent_counts = agents.groupby(zone_id_col_name).size()

    ## Allocate agents for each zone
    for taz in np.unique(agents[zone_id_col_name]):
        num_agents = taz_agent_counts[taz_agent_counts.index.values == taz].values[0]
        chooser_ids = agents.index[agents[zone_id_col_name] == taz].values
        print 'There are %s demand agents in TAZ %s' % (num_agents, taz)
        alts = alternatives[alternatives[zone_id_col_name] == taz]
        alternative_ids = alts.index.values
        probabilities = np.ones(len(alternative_ids)) #Each unit has equal probability.  Change if alternative weights desired.
        num_units = len(alts)
        print 'There are %s supply units in TAZ %s' % (num_units, taz)
        choices = unit_choice(chooser_ids,alternative_ids,probabilities)
        agents.loc[chooser_ids,'building_id'] = choices
        if num_agents > num_units:
            print 'Warning:  number of demand agents exceeds number of supply units in TAZ %s' % taz


## Note zones are often over-occupied- look into relationship between agents and supply.

#ALLOCATE HOUSEHOLDS TO BUILDING
random_allocate_agents(households, buildings, zone_id_col_name='mgra_id', capacity_col_name='residential_units')

#ALLOCATE JOBS TO BUILDING
random_allocate_agents(jobs, buildings, zone_id_col_name='block_id', capacity_col_name='job_spaces')


#EXPORT DEMAND AGENTS TO DB
if 'block_id' in jobs.columns:
    del jobs['block_id']
    
if 'mgra_id' in households.columns:
    del households['mgra_id']
    
for df in [jobs, households]:
    df.building_id[df.building_id.isnull()] = -1
    for col in df.columns:
        df[col] = df[col].astype('int')

df_to_db(jobs, 'jobs', schema=loader.tables.public)
df_to_db(households, 'households', schema=loader.tables.public)


#Export formated buildings to db
if 'mgra_id' in buildings.columns:
    del buildings['mgra_id']
    
if 'block_id' in buildings.columns:
    del buildings['block_id']
    
if 'job_spaces' in buildings.columns:
    del buildings['job_spaces']
    
df_to_db(buildings, 'buildings', schema=loader.tables.public)


## Price model estimation datasets

# Costar nonresidential rent
costar = db_to_df('select * from staging.costar2012')

costar = costar[['property_id', 'rentable_building_area', 'number_of_stories','year_built', 'property_type', 'secondary_type', 'average_weighted_rent', 'parcelid' ]]

# for tex_col in ['property_type', 'secondary_type']:
#        costar[tex_col] = costar[tex_col].fillna(' ')
#        costar[tex_col] = costar[tex_col].str.encode('utf-8')

costar.index.name = 'idx'

costar = costar.rename(columns = {'property_id':'building_id', 'rentable_building_area':'non_residential_sqft', 
                                  'average_weighted_rent':'nonres_rent_per_sqft','number_of_stories':'stories', 'parcelid':'parcel_id'})

costar_estimation = costar[costar.nonres_rent_per_sqft > 0]
costar_estimation = pd.DataFrame(costar_estimation.groupby('parcel_id').nonres_rent_per_sqft.mean())
costar_joined = pd.merge(costar_estimation, buildings, left_index = True, right_on = 'parcel_id')
costar_joined = costar_joined[costar_joined.development_type_id.isin([2,5,4])]
costar_joined.index.name = 'idx'
if 'id' in costar_joined.columns:
    del costar_joined['id']
    
df_to_db(costar_joined, 'costar', schema=loader.tables.public)


## LUZ Control Totals

hh_controls = db_to_df('select * from staging.pecas_hh_controls;')
hh_controls = hh_controls[['yr', 'activity_id', 'luz_id', 'total_hh_controls']]
hh_controls = hh_controls.rename(columns = {'yr':'year', 'total_hh_controls':'total_number_of_households'})
hh_controls.total_number_of_households = np.round(hh_controls.total_number_of_households).astype('int')
hh_controls.index.name = 'idx'
df_to_db(hh_controls, 'annual_household_control_totals', schema=loader.tables.public)

## LUZ Prices
pecas_prices = db_to_df('select * from staging.pecas_price_predictions;')
space_dev_type_xref =  db_to_df('select * from staging.xref_space_type_dev_type;')
pecas_prices = pd.merge(pecas_prices, space_dev_type_xref, left_on = 'space_type_id', right_on = 'space_type_id')[['yr', 'luz_id', 'development_type_id', 'price']]
pecas_prices = pecas_prices.groupby(['yr', 'luz_id', 'development_type_id']).price.mean().reset_index()
pecas_prices = pecas_prices.rename(columns = {'yr':'year'})
pecas_prices.index.name = 'idx'
df_to_db(pecas_prices, 'pecas_prices', schema=loader.tables.public)


## Assessor residential parcel transactions
transactions = db_to_df('select * from staging.assessor_transactions')
transactions['oc_doc_date'] = pd.to_datetime(transactions.oc_doc_date)
transactions = transactions[transactions.oc_doc_date > datetime.datetime(2010,1,1)]
transactions = transactions[(transactions.parcelid > 0) & (transactions.oc_price > 0)]
transactions = transactions.rename(columns = {'parcelid':'parcel_id', 'year_effective':'year_built',})
transactions = transactions.groupby('parcel_id').apply(lambda t: t[t.oc_doc_date==t.oc_doc_date.max()])
transactions['view'] = False
transactions.view[transactions.has_view == 'True'] = True
transactions = transactions[['year_built', 'beds', 'baths', 'sqft', 'view', 'oc_doc_date', 'oc_price']]

transactions = transactions.reset_index().set_index('parcel_id')
if 'level_1' in transactions.columns:
    del transactions['level_1']

buildings = db_to_df('select * from buildings').set_index('building_id')
if 'id' in buildings.columns:
    del buildings['id']
if 'job_spaces' in buildings.columns:
    del buildings['job_spaces']

transactions_joined = pd.merge(transactions[['oc_doc_date', 'oc_price',]], buildings, left_index = True, right_on = 'parcel_id')
transactions_joined = transactions_joined[transactions_joined.development_type_id.isin([19, 20, 21])]
transactions_joined = transactions_joined[(transactions_joined.residential_units > 0) & (transactions_joined.residential_sqft > 0)]

transactions_joined['res_price_per_sqft'] = transactions_joined.oc_price / transactions_joined.residential_sqft
del transactions_joined['oc_doc_date']
del transactions_joined['oc_price']

df_to_db(transactions_joined, 'assessor_transactions', schema=loader.tables.public)


## Zoning
zoning = db_to_df('select * from staging.zoning')
zoning_allowed_uses = db_to_df('select zoning_allowed_uses_id as zoning_id, development_type_id from staging.zoning_allowed_uses')

del zoning['index']
del zoning['id']
zoning = zoning.set_index('zoning_id')
zoning_allowed_uses.index.name = 'idx'

df_to_db(zoning_allowed_uses, 'zoning_allowed_uses', schema=loader.tables.public)
df_to_db(zoning, 'zoning', schema=loader.tables.public)


# Scheduled development events
site_spec = db_to_df('select * from staging.sitespec;')
site_spec = site_spec.rename(columns = {'placetype':'development_type_id', 'nonres_sqf':'non_residential_sqft',
                            'res_unit':'residential_units', 'sqft_prunt':'sqft_per_unit', 
                            'avg_story':'stories', 'phase':'year_built'})
site_spec['note'] = 'Sitespec ' + site_spec.siteid.astype('str') + '. ' + site_spec.source + ": " + site_spec.sitename
site_spec = site_spec[['year_built', 'development_type_id', 'stories',
                       'non_residential_sqft', 'sqft_per_unit', 'residential_units', 'parcel_id', 'note']]
site_spec['improvement_value'] = 0
site_spec['res_price_per_sqft'] = 0.0
site_spec['nonres_rent_per_sqft'] = 0.0
site_spec.stories[site_spec.stories < 1] = 1
site_spec.sqft_per_unit[(site_spec.residential_units > 0) & (site_spec.sqft_per_unit < 1)] = 1500
site_spec.index.name = 'scheduled_development_event_id'

df_to_db(site_spec, 'scheduled_development_events', schema=loader.tables.public)

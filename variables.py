import numpy as np
import pandas as pd
from urbansim.utils import misc
import urbansim.sim.simulation as sim
import datasources
from urbansim_defaults import utils
from urbansim_defaults import variables


#####################
# COSTAR VARIABLES
#####################

@sim.column('costar', 'node_id')
def node_id(parcels, costar):
    return misc.reindex(parcels.node_id, costar.parcel_id)

@sim.column('costar', 'mgra_id')
def mgra_id(parcels, costar):
    return misc.reindex(parcels.mgra_id, costar.parcel_id)
    
@sim.column('costar', 'luz_id')
def luz_id(parcels, costar):
    return misc.reindex(parcels.luz_id, costar.parcel_id)
    
@sim.column('costar', 'distance_to_coast')
def distance_to_coast(parcels, costar):
    return misc.reindex(parcels.distance_to_coast, costar.parcel_id)
    
@sim.column('costar', 'distance_to_freeway')
def distance_to_freeway(parcels, costar):
    return misc.reindex(parcels.distance_to_freeway, costar.parcel_id)
    
@sim.column('costar', 'distance_to_onramp')
def distance_to_onramp(parcels, costar):
    return misc.reindex(parcels.distance_to_onramp, costar.parcel_id)
    
@sim.column('costar', 'distance_to_park')
def distance_to_park(parcels, costar):
    return misc.reindex(parcels.distance_to_park, costar.parcel_id)
    
@sim.column('costar', 'distance_to_school')
def distance_to_school(parcels, costar):
    return misc.reindex(parcels.distance_to_school, costar.parcel_id)
    
@sim.column('costar', 'distance_to_transit')
def distance_to_transit(parcels, costar):
    return misc.reindex(parcels.distance_to_transit, costar.parcel_id)
    
# @sim.column('costar', 'nonres_occupancy')
# def nonres_occupancy(nodes, costar):
    # return misc.reindex(nodes.nonres_occupancy, costar.node_id)
    
@sim.column('costar', 'pecas_price')
def pecas_price(costar, pecas_prices):
    costar = costar.to_frame(columns = ['development_type_id', 'luz_id'])
    pecas_prices = pecas_prices.to_frame()
    year = sim.get_injectable('year')
    if year is None:
        year = 2012
    pecas_prices = pecas_prices[pecas_prices.year == year]
    costar.index.name = 'costar_id'
    merged = pd.merge(costar.reset_index(), pecas_prices, left_on = ['luz_id', 'development_type_id'], right_on = ['luz_id', 'development_type_id']).set_index('costar_id')
    return pd.Series(data = merged.price, index = costar.index).fillna(0)
    
################################
# ASSESSOR TRANSACTION VARIABLES
################################

@sim.column('assessor_transactions', 'node_id')
def node_id(parcels, assessor_transactions):
    return misc.reindex(parcels.node_id, assessor_transactions.parcel_id)

@sim.column('assessor_transactions', 'mgra_id')
def mgra_id(parcels, assessor_transactions):
    return misc.reindex(parcels.mgra_id, assessor_transactions.parcel_id)
    
@sim.column('assessor_transactions', 'luz_id')
def luz_id(parcels, assessor_transactions):
    return misc.reindex(parcels.luz_id, assessor_transactions.parcel_id)
    
@sim.column('assessor_transactions', 'distance_to_coast')
def distance_to_coast(parcels, assessor_transactions):
    return misc.reindex(parcels.distance_to_coast, assessor_transactions.parcel_id)
    
@sim.column('assessor_transactions', 'distance_to_freeway')
def distance_to_freeway(parcels, assessor_transactions):
    return misc.reindex(parcels.distance_to_freeway, assessor_transactions.parcel_id)
    
@sim.column('assessor_transactions', 'distance_to_onramp')
def distance_to_onramp(parcels, assessor_transactions):
    return misc.reindex(parcels.distance_to_onramp, assessor_transactions.parcel_id)
    
@sim.column('assessor_transactions', 'distance_to_park')
def distance_to_park(parcels, assessor_transactions):
    return misc.reindex(parcels.distance_to_park, assessor_transactions.parcel_id)
    
@sim.column('assessor_transactions', 'distance_to_school')
def distance_to_school(parcels, assessor_transactions):
    return misc.reindex(parcels.distance_to_school, assessor_transactions.parcel_id)
    
@sim.column('assessor_transactions', 'distance_to_transit')
def distance_to_transit(parcels, assessor_transactions):
    return misc.reindex(parcels.distance_to_transit, assessor_transactions.parcel_id)
    
@sim.column('assessor_transactions', 'pecas_price')
def pecas_price(assessor_transactions, pecas_prices):
    assessor_transactions = assessor_transactions.to_frame(columns = ['development_type_id', 'luz_id'])
    pecas_prices = pecas_prices.to_frame()
    year = sim.get_injectable('year')
    if year is None:
        year = 2012
    pecas_prices = pecas_prices[pecas_prices.year == year]
    merged = pd.merge(assessor_transactions.reset_index(), pecas_prices, left_on = ['luz_id', 'development_type_id'], right_on = ['luz_id', 'development_type_id']).set_index('building_id')
    return pd.Series(data = merged.price, index = assessor_transactions.index).fillna(0)
    
@sim.column('assessor_transactions', 'sqft_per_unit')
def sqft_per_unit(assessor_transactions):
    sqft_per_unit = pd.Series(np.zeros(len(assessor_transactions))*1.0, index = assessor_transactions.index)
    sqft_per_unit[assessor_transactions.residential_units > 0] = assessor_transactions.residential_sqft[assessor_transactions.residential_units > 0] / assessor_transactions.residential_units[assessor_transactions.residential_units > 0]
    return sqft_per_unit
    
@sim.column('assessor_transactions', 'year_built_1940to1950')
def year_built_1940to1950(assessor_transactions):
    return (assessor_transactions.year_built >= 1940) & (assessor_transactions.year_built < 1950)
    
@sim.column('assessor_transactions', 'year_built_1950to1960')
def year_built_1950to1960(assessor_transactions):
    return (assessor_transactions.year_built >= 1950) & (assessor_transactions.year_built < 1960)
    
@sim.column('assessor_transactions', 'year_built_1960to1970')
def year_built_1960to1970(assessor_transactions):
    return (assessor_transactions.year_built >= 1960) & (assessor_transactions.year_built < 1970)
    
@sim.column('assessor_transactions', 'year_built_1970to1980')
def year_built_1970to1980(assessor_transactions):
    return (assessor_transactions.year_built >= 1970) & (assessor_transactions.year_built < 1980)
    
@sim.column('assessor_transactions', 'year_built_1980to1990')
def year_built_1980to1990(assessor_transactions):
    return (assessor_transactions.year_built >= 1980) & (assessor_transactions.year_built < 1990)
    
    
####NODES
    
@sim.column('nodes', 'nonres_occupancy_3000m')
def nonres_occupancy_3000m(nodes):
    return nodes.jobs_3000m / (nodes.job_spaces_3000m + 1.0)
    
@sim.column('nodes', 'res_occupancy_3000m')
def res_occupancy_3000m(nodes):
    return nodes.households_3000m / (nodes.residential_units_3000m + 1.0)
    
    
#####################
# BUILDINGS VARIABLES
#####################

@sim.column('buildings', 'job_spaces')
def job_spaces(parcels, buildings):
    return np.round(buildings.non_residential_sqft / 200.0)

@sim.column('buildings', 'luz_id')
def luz_id(buildings, parcels):
    return misc.reindex(parcels.luz_id, buildings.parcel_id)
    
@sim.column('buildings', 'pecas_price')
def pecas_price(buildings, pecas_prices):
    buildings = buildings.to_frame(columns = ['development_type_id', 'luz_id'])
    pecas_prices = pecas_prices.to_frame()
    year = sim.get_injectable('year')
    if year is None:
        year = 2012
    pecas_prices = pecas_prices[pecas_prices.year == year]
    merged = pd.merge(buildings.reset_index(), pecas_prices, left_on = ['luz_id', 'development_type_id'], right_on = ['luz_id', 'development_type_id']).set_index('building_id')
    return pd.Series(data = merged.price, index = buildings.index).fillna(0)
    
@sim.column('buildings', 'node_id')
def node_id(parcels, buildings):
    return misc.reindex(parcels.node_id, buildings.parcel_id)

@sim.column('buildings', 'mgra_id')
def mgra_id(parcels, buildings):
    return misc.reindex(parcels.mgra_id, buildings.parcel_id)
    
@sim.column('buildings', 'zone_id')
def zone_id(buildings):
    return np.zeros(len(buildings))
    
@sim.column('buildings', 'parcel_size')
def parcel_size(buildings):
    return np.zeros(len(buildings))
    
@sim.column('buildings', 'building_sqft')
def building_sqft(buildings):
    return buildings.residential_sqft + buildings.non_residential_sqft
    
@sim.column('buildings', 'sqft_per_unit')
def sqft_per_unit(buildings):
    sqft_per_unit = pd.Series(np.zeros(len(buildings))*1.0, index = buildings.index)
    sqft_per_unit[buildings.residential_units > 0] = buildings.residential_sqft[buildings.residential_units > 0] / buildings.residential_units[buildings.residential_units > 0]
    return sqft_per_unit
    
@sim.column('parcels', 'parcel_size')
def parcel_size(parcels):
    return np.zeros(len(parcels))
    
@sim.column('buildings', 'building_type_id')
def building_type_id(buildings):
    return buildings.development_type_id
    
@sim.column('buildings', 'distance_to_coast')
def distance_to_coast(parcels, buildings):
    return misc.reindex(parcels.distance_to_coast, buildings.parcel_id)
    
@sim.column('buildings', 'distance_to_freeway')
def distance_to_freeway(parcels, buildings):
    return misc.reindex(parcels.distance_to_freeway, buildings.parcel_id)
    
@sim.column('buildings', 'distance_to_onramp')
def distance_to_onramp(parcels, buildings):
    return misc.reindex(parcels.distance_to_onramp, buildings.parcel_id)
    
@sim.column('buildings', 'distance_to_park')
def distance_to_park(parcels, buildings):
    return misc.reindex(parcels.distance_to_park, buildings.parcel_id)
    
@sim.column('buildings', 'distance_to_school')
def distance_to_school(parcels, buildings):
    return misc.reindex(parcels.distance_to_school, buildings.parcel_id)
    
@sim.column('buildings', 'distance_to_transit')
def distance_to_transit(parcels, buildings):
    return misc.reindex(parcels.distance_to_transit, buildings.parcel_id)
    
@sim.column('buildings', 'year_built_1940to1950')
def year_built_1940to1950(buildings):
    return (buildings.year_built >= 1940) & (buildings.year_built < 1950)
    
@sim.column('buildings', 'year_built_1950to1960')
def year_built_1950to1960(buildings):
    return (buildings.year_built >= 1950) & (buildings.year_built < 1960)
    
@sim.column('buildings', 'year_built_1960to1970')
def year_built_1960to1970(buildings):
    return (buildings.year_built >= 1960) & (buildings.year_built < 1970)
    
@sim.column('buildings', 'year_built_1970to1980')
def year_built_1970to1980(buildings):
    return (buildings.year_built >= 1970) & (buildings.year_built < 1980)
    
@sim.column('buildings', 'year_built_1980to1990')
def year_built_1980to1990(buildings):
    return (buildings.year_built >= 1980) & (buildings.year_built < 1990)
    
#####################
# HOUSEHOLD VARIABLES
#####################

@sim.column('households', 'luz_id', cache=True)
def luz_id(households, buildings):
    return misc.reindex(buildings.luz_id, households.building_id)
    
@sim.column('households', 'activity_id')
def activity_id(households):
    idx_38 = (households.income < 25000) & (households.persons < 3)
    idx_39 = (households.income < 25000) & (households.persons >= 3)
    idx_40 = (households.income >= 25000) & (households.income < 150000) & (households.persons < 3)
    idx_41 = (households.income >= 25000) & (households.income < 150000) & (households.persons >= 3)
    idx_42 = (households.income >= 150000) & (households.persons < 3)
    idx_43 = (households.income >= 150000) & (households.persons >= 3)
    return 38*idx_38 + 39*idx_39 + 40*idx_40 + 41*idx_41 + 42*idx_42 + 43*idx_43
    
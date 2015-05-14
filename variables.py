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

@sim.column('buildings', 'is_office')
def is_office(buildings):
    return (buildings.development_type_id == 4).astype('int')
    
@sim.column('buildings', 'is_retail')
def is_retail(buildings):
    return (buildings.development_type_id == 5).astype('int')

@sim.column('buildings', 'job_spaces')
def job_spaces(parcels, buildings):
    return np.round(buildings.non_residential_sqft / 200.0)

@sim.column('buildings', 'luz_id')
def luz_id(buildings, parcels):
    return misc.reindex(parcels.luz_id, buildings.parcel_id)
    
@sim.column('buildings', 'luz_id_buildings')
def luz_id_buildings(buildings, parcels):
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
    
@sim.column('buildings', 'zone_id', cache=True)
def zone_id(buildings):
    return np.zeros(len(buildings))
    
@sim.column('buildings', 'msa_id', cache=True)
def msa_id(buildings, parcels):
    return misc.reindex(parcels.msa_id, buildings.parcel_id)
    
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
    
@sim.column('buildings', 'vacant_residential_units')
def vacant_residential_units(buildings, households):
    return buildings.residential_units.sub(
        households.building_id.value_counts(), fill_value=0)
    
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
    
@sim.column('households', 'mgra_id', cache=True)
def mgra_id(households, buildings):
    return misc.reindex(buildings.mgra_id, households.building_id)
    
@sim.column('households', 'luz_id_households', cache=True)
def luz_id_households(households, buildings):
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
    
@sim.column('households', 'income_halves', cache=True)
def income_halves(households):
    s = pd.Series(pd.qcut(households.income, 2, labels=False),
                  index=households.index)
    s = s.add(1)
    return s
    
#####################
# PARCEL VARIABLES
#####################
# these are actually functions that take parameters, but are parcel-related
# so are defined here

# @sim.injectable('parcel_is_allowed_func', autocall=False)
# def parcel_is_allowed(form):
    # settings = sim.settings
    # form_to_btype = settings["form_to_btype"]
    #### we have zoning by building type but want
    #### to know if specific forms are allowed
    # allowed = [sim.get_table('zoning_baseline')
               # ['type%d' % typ] == 't' for typ in form_to_btype[form]]
    # return pd.concat(allowed, axis=1).max(axis=1).\
        # reindex(sim.get_table('parcels').index).fillna(False)
        
@sim.column('parcels', 'parcel_size')
def parcel_size(parcels):
    return parcels.parcel_acres * 43560
    
@sim.column('parcels', 'proportion_developable')
def proportion_developable(parcels):
    return 1.0 - parcels.proportion_undevelopable
    
@sim.injectable('parcel_is_allowed_func', autocall=False)
def parcel_is_allowed(form):
    parcels = sim.get_table('parcels')
    zoning_allowed_uses = sim.get_table('zoning_allowed_uses').to_frame()
    
    if form == 'sf_detached':
        allowed = zoning_allowed_uses[19]
    elif form == 'sf_attached':
        allowed = zoning_allowed_uses[20]
    elif form == 'mf_residential':
        allowed = zoning_allowed_uses[21]
    elif form == 'light_industrial':
        allowed = zoning_allowed_uses[2]
    elif form == 'heavy_industrial':
        allowed = zoning_allowed_uses[3]
    elif form == 'office':
        allowed = zoning_allowed_uses[4]
    elif form == 'retail':
        allowed = zoning_allowed_uses[5]
    else:
        df = pd.DataFrame(index=parcels.index)
        df['allowed'] = True
        allowed = df.allowed
        
    return allowed
    
@sim.injectable('parcel_sales_price_sqft_func', autocall=False)
def parcel_sales_price_sqft(use):
    s = parcel_average_price(use)
    if use == "residential": s *= 1.2
    return s
    
@sim.injectable('parcel_average_price', autocall=False)
def parcel_average_price(use):
    return misc.reindex(sim.get_table('nodes')[use],
                        sim.get_table('parcels').node_id)
                        
@sim.column('parcels', 'max_dua', cache=True)
def max_dua(parcels, zoning):
    sr = misc.reindex(zoning.max_dua, parcels.zoning_id)
    sr = sr*parcels.proportion_developable
    df = pd.DataFrame({'max_dua':sr.values}, index = sr.index.values)
    df['index'] = df.index.values
    df = df.drop_duplicates()
    del df['index']
    df.index.name = 'parcel_id'
    return df.max_dua
    
@sim.column('parcels', 'max_far', cache=True)
def max_far(parcels, zoning):
    sr = misc.reindex(zoning.max_far, parcels.zoning_id)
    sr = sr*parcels.proportion_developable
    df = pd.DataFrame({'max_far':sr.values}, index = sr.index.values)
    df['index'] = df.index.values
    df = df.drop_duplicates()
    del df['index']
    df.index.name = 'parcel_id'
    return df.max_far
    
##Placeholder-  building height currently unconstrained (very high limit-  1000 ft.)
@sim.column('parcels', 'max_height', cache=True)
def max_height(parcels):
    return pd.Series(np.ones(len(parcels))*1000.0, index = parcels.index)
    
    
@sim.column('parcels', 'total_sqft', cache=True)
def total_sqft(parcels, buildings):
    return buildings.building_sqft.groupby(buildings.parcel_id).sum().\
        reindex(parcels.index).fillna(0)
    

@sim.column('parcels', 'building_purchase_price_sqft')
def building_purchase_price_sqft():
    return parcel_average_price("residential") * .81


@sim.column('parcels', 'building_purchase_price')
def building_purchase_price(parcels):
    return (parcels.total_sqft * parcels.building_purchase_price_sqft).\
        reindex(parcels.index).fillna(0)

## Next step:  run sqft proforma separately for each development type, and for each dev type account for the relevant fees in the cost per sqft variable in sqftproforma.  Actually, the relevant place is going to be in the the building_purchase_price_sqft variable (subtract fees here)
@sim.column('parcels', 'land_cost')
def land_cost(parcels):
    return parcels.building_purchase_price + parcels.parcel_acres * 43560 * 12.21
    
@sim.column('parcels', 'total_sfd_du', cache=False)
def total_sfd_du(parcels, buildings):
    buildings = buildings.to_frame(buildings.local_columns)
    return buildings[buildings.development_type_id == 19].residential_units.groupby(buildings.parcel_id).sum().\
        reindex(parcels.index).fillna(0)
        
@sim.column('parcels', 'total_sfa_du', cache=False)
def total_sfa_du(parcels, buildings):
    buildings = buildings.to_frame(buildings.local_columns)
    return buildings[buildings.development_type_id == 20].residential_units.groupby(buildings.parcel_id).sum().\
        reindex(parcels.index).fillna(0)
        
@sim.column('parcels', 'total_mfr_du', cache=False)
def total_mfr_du(parcels, buildings):
    buildings = buildings.to_frame(buildings.local_columns)
    return buildings[buildings.development_type_id == 21].residential_units.groupby(buildings.parcel_id).sum().\
        reindex(parcels.index).fillna(0)
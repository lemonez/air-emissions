import time, os
import pandas as pd
from collections import OrderedDict

print(time.strftime("%H:%M:%S")+'\tmodule \''+__name__+'\' reloaded.')

####################
##BEGIN USER EDITS##
####################

#### configure data/calculations ####
data_year     = 2019  # year_to_calculate = int(time.strftime('%Y'))
data_dir      = './data/'
out_dir       = './output/'
log_dir       = out_dir+'logs/'
log_suffix    = ''
out_dir_child = 'dev'

first_month_to_calculate = 1
last_month_to_calculate  = 9

# just to be explicit: this is the offset for accessing tstamp intervals
month_offset = first_month_to_calculate

equips_to_calculate = ['coker_e', 'coker_w']
we_are_calculating_toxics = False
calciner_toxics = False

#### configure formatting and logging ####
# select month format for output; defaults to name abbreviations
#   integers     (1, 2, ... 12)
#   name abbrevs ('Jan', 'Feb', ..., 'Dec')
write_month_names = True # write month names (abbrevs) in output

# select verbosity of calculation status logging to console
verbose_calc_status = True

# select timeframe and equipment for emissions calculations
# defaults: last year, all months, all equipment

##################
##END USER EDITS##
##################

months_to_calculate = range(first_month_to_calculate,
                            last_month_to_calculate + 1)

def ends(df, n=2):
    """Return first and last rows of pd.DataFrame"""
    return pd.concat([df.head(n), df.tail(n)])

def generate_ts_interval_list():
    """Generate datetime-aware tuple (start, end) for each month."""
    intervals = []
    for mo in months_to_calculate:
        ts_start = pd.to_datetime(str(data_year)+'-'+str(mo)+'-01', format='%Y-%m-%d') # Timestamp('2018-01-01 00:00:00')
        ts_end   = ts_start + pd.DateOffset(months=1) - pd.DateOffset(hours=1) # Timestamp('2018-01-31 23:00:00')
        interval = (ts_start, ts_end) # interval = pd.date_range(start=ts_start, end=ts_end, freq='H')
        intervals.append(interval)
    return intervals

def generate_date_range():
    """Generate pd.date_range from config.py for missing timestamps."""
    tsi = generate_ts_interval_list()
    s_tstamp = tsi[0][0]
    e_tstamp = tsi[len(tsi) - 1][1]
    dr = pd.date_range(start=s_tstamp, end=e_tstamp, freq='H')
    return dr

def parse_equip_map(path):
    """Read CSV with equipment information and return pd.DataFrame."""
    col_map = {
        'PI Tag'     : 'ptag',
        'Python GUID': 'unit_key',
        'WED Pt'     : 'unit_id', 
        'Unit Name'  : 'unit_name',
        'CEMS'       : 'param',
        'Units'      : 'units'
        }
    
    equip_info = (pd.read_csv(path)).rename(columns=col_map)
    equip_info['param'] = equip_info['param'].str.strip().str.lower()
    equip_info['units'] = equip_info['units'].str.strip().str.lower()
    equip_info.replace({'dry o2': 'o2'}, inplace=True)
            # equip_info.loc[equip_info['ptag'] == '46AI601.PV', 'param'] = 'o2' # change 'dry o2' to 'o2' for H2 Plant
    equip_info['param_units'] = equip_info['param']+'_'+equip_info['units']
            # .set_index(['unit_key', 'ptag'])
            # .set_index('PI Tag')
            # .rename_axis('ptag', axis=0))
    return equip_info

def generate_unitID_unitkey_dict(equip_df):
    """Return OrderedDict of equipment ({unit_id: unit_key})"""
    unitID_unitkey_dict = (equip_df.set_index('unit_id')['unit_key']
                                   .to_dict(into=OrderedDict))
    return unitID_unitkey_dict    

def generate_unitkey_unitname_dict(equip_df):
    """Return dict of equipment to remap output names ({unit_key: unit_name})"""
    unitkey_unitname_dict = (equip_df
                             .drop_duplicates(subset='unit_key')
                             .set_index('unit_key')['unit_name']
                             .to_dict())
    return unitkey_unitname_dict

def generate_month_map():
    """Generate dict to map month numbers to names for output."""
    months_int  = list(range(1,13))  # [1, 2, 3... 12]
    months_str  = [str(i) for i in months_int]
    months_abrv = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                   'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    month_map = {}
    for i, a in zip(months_str, months_abrv):
        month_map[i] = a
    return month_map

if write_month_names:
    month_map = generate_month_map()
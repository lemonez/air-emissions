import time, os
import pandas as pd

#print(time.strftime("%H:%M:%S")+'\tmodule \''+__name__+'\' reloaded.')

####################
##BEGIN USER EDITS##
####################

#### configure data/calculations ####
data_year     = 2019  # year_to_calculate = int(time.strftime('%Y'))
data_dir      = './data/'
out_dir       = './output/'
log_dir       = out_dir+'logs/'
log_suffix    = ''
out_dir_child = 'dev_criteria'

first_month_to_calculate = 1
last_month_to_calculate  = 9

# just to be explicit: this is the offset for accessing tstamp intervals
month_offset = first_month_to_calculate

equip_to_calculate = [
                        'boiler_4',
                        'calciner_1',
                        # 'coker_1',
                        # 'h_furnace_n',
                        # 'h2_plant_2',
                        # 'h2_flare'
                      ]
#equip_to_calculate = ['coker_e', 'coker_w']

pollutants_to_calculate = [
                           # criteria
                           'NOx',
                           'CO',
                           'SO2',
                           'VOC',
                           'PM', 'PM25', 'PM10',
                           'H2SO4',
                           # GHG
#                           'CO2'
                           ]

we_are_calculating_toxics = False
calciner_toxics = False
calculate_PM_fractions = False


#### configure formatting and logging ####
# select month format for output; defaults to name abbreviations
#   integers     (1, 2, ... 12)
#   name abbrevs ('Jan', 'Feb', ..., 'Dec')
write_month_names = True # write month names (abbrevs) in output

# select verbosity of calculation status logging to console
verbose_logging = True

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

def verify_pollutants_to_calc(pol_list):
    """Ensure only criteria or GHG pollutants are being calculated, not both."""
    import sys
    
    if 'CO2' in pol_list and len(pol_list) > 1:
        print('Cannot calculate GHG (CO2) and criteria emissions at the same time.')
        print('Change pollutant selection in config file.')
        sys.exit()
    
equip_types = {
        'coker_1'        : 'coker_old'   ,
        'coker_2'        : 'coker_old'   ,
        'coker_e'        : 'coker_new'   ,
        'coker_w'        : 'coker_new'   ,
        'calciner_1'     : 'calciner'    ,
        'calciner_2'     : 'calciner'    ,
        'h2_plant_2'     : 'h2plant'     ,
        'h2_flare'       : 'flare'       ,
        'crude_rfg'      : 'heaterboiler',
        'crude_vtg'      : 'heaterboiler',
        'n_vac'          : 'heaterboiler',
        's_vac'          : 'heaterboiler',
        'ref_heater_1'   : 'heaterboiler',
        'ref_heater_2'   : 'heaterboiler',
        'naptha_heater'  : 'heaterboiler',
        'naptha_reboiler': 'heaterboiler',
        'dhds_heater_3'  : 'heaterboiler',
        'hcr_1'          : 'heaterboiler',
        'hcr_2'          : 'heaterboiler',
        'rxn_r_1'        : 'heaterboiler',
        'rxn_r_4'        : 'heaterboiler',
        'dhds_heater_1'  : 'heaterboiler',
        'dhds_heater_1'  : 'heaterboiler',
        'dhds_reboiler_1': 'heaterboiler',
        'dhds_reboiler_1': 'heaterboiler',
        'dhds_heater_2'  : 'heaterboiler',
        'h_furnace_n'    : 'heaterboiler',
        'h_furnace_s'    : 'heaterboiler',
        'boiler_4'       : 'heaterboiler',
        'boiler_5'       : 'heaterboiler',
        'boiler_6'       : 'heaterboiler',
        'boiler_7'       : 'heaterboiler'
        }

# column names for final output
output_colnames_map = {
                'month'        : 'Month',
                'equipment'    : 'Equipment',
                'cokerfg_mscfh': 'Coker Fuel Gas',
                'pilot_mscfh'  : 'Pilot Natural Gas',
                'fuel_rfg'     : 'Refinery Fuel Gas',
                'fuel_ng'      : 'Natural Gas',
                'coke_tons'    : 'Calcined Coke',
                'nox'          : 'NOx',
                'co'           : 'CO',
                'so2'          : 'SO2',
                'voc'          : 'VOC', 
                'pm'           : 'PM',
                'pm25'         : 'PM25',
                'pm10'         : 'PM10',
                'h2so4'        : 'H2SO4',
                'co2'          : 'CO2'
                }

pollutants_all = [
                    # criteria
                    'NOx', 'CO', 'SO2', 'VOC', 'PM', 'PM25', 'PM10', 'H2SO4',
                    # GHG
                    'CO2'
                ]
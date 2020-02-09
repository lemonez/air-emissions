import time, os
import pandas as pd

#print(time.strftime("%H:%M:%S")+'\tmodule \''+__name__+'\' reloaded.')

####################
##BEGIN USER EDITS##
####################

#### configure data/calculations ####
first_month_to_calculate = 1
last_month_to_calculate  = 12
data_year     = 2019  # year_to_calculate = int(time.strftime('%Y'))
data_dir      = './data_'+str(data_year)+'/' # all input data files
out_dir       = './output/'
log_dir       = out_dir+'logs/'
log_suffix    = ''
out_dir_child = '2019_criteria'

round_decimals = 4
MAX_CEMS_TO_FILL = 18 # maximum consecutive missing CEMS values to fill

# calculate GHG?
GHG = False

# calculate toxics?
calculate_toxics = False
calculate_calciner_toxics = False
calculate_PM_fractions = False

# directories
annual_prefix  = data_dir+'annual/' # data that changes monthly/annually
static_prefix  = data_dir+'static/' # static data
CEMS_dir       = annual_prefix+'CEMS/' # monthly CEMS data

if data_year == 2019:
    # files
    fname_eqmap    = 'equipmap.csv'            # all equipment names / IDs    
    fname_NG_chem  = 'chemicals_NG.csv'        # NG static chemical data    
    fname_FG_chem  = 'chemicals_FG.csv'        # FG static chemical data
    fname_EFs      = 'EFs_monthly_Dec.xlsx'    # monthly-EF excel workbook
    fname_analyses = str(data_year)+'_analyses_agg_Dec.xlsx'   # all gas lab-test data
    fname_ewcoker  = str(data_year)+'_data_EWcoker_Dec.xlsx'   # coker CEMS, fuel, flow data
    fname_fuel     = str(data_year)+'_usage_fuel_Dec.xlsx'     # annual fuel usage for all equipment
    fname_coke     = str(data_year)+'_usage_coke_Dec.xlsx'     # annual coke usage for calciners
    fname_flarefuel= str(data_year)+'_usage_flarefuel_Dec.xlsx'# annual flare-fuel through H2-plant flare
    fname_h2stack  = str(data_year)+'_flow_h2stack_Dec.xlsx'   # annual H2-stack flow data
    fname_PSAstack = str(data_year)+'_flow_PSAoffgas_Dec.xlsx' # PSA offgas flow data for #2 H2 Plant
    fname_flareEFs = str(data_year)+'_EFs_flare.xlsx'          # EFs for H2 flare
    fname_toxicsEFs= str(data_year)+'_EFs_toxics.xlsx'         # EFs for toxics
    fname_toxicsEFs_calciners = str(data_year)+'_EFs_calciner_toxics.xlsx' # EFs for calciners toxics
    
    labtab_NG       = '#2H2FdNatGas 2019'  # NG-sample lab-test data
    labtab_RFG      = 'RFG 2019'           # RFG-sample lab-test data
    labtab_cokerFG  = 'Coker FG 2019'      # cokerFG-sample lab-test data
    labtab_CVTG     = 'CVTG 2019'          # CVTG-sample lab-test data
    labtab_flare    = '#2H2 Flare 2019'    # flare-gas sample lab-test data
    labtab_PSA      = 'PSA Offgas 2019'    # PSA-Offgas sample lab-test data
    
    sheet_fuel = '12-19'

elif data_year == 2018:

    # files
    fname_eqmap    = 'equipmap.csv'            # all equipment names / IDs    
    fname_NG_chem  = 'chemicals_NG.csv'        # NG static chemical data    
    fname_FG_chem  = 'chemicals_FG.csv'        # FG static chemical data
    fname_EFs      = 'EFs_monthly.xlsx'        # monthly-EF excel workbook
    fname_analyses = str(data_year)+'_analyses_agg.xlsx'   # all gas lab-test data
    fname_ewcoker  = str(data_year)+'_data_EWcoker_DUMMY.xlsx'   # coker CEMS, fuel, flow data
    fname_fuel     = str(data_year)+'_usage_fuel.xlsx'     # annual fuel usage for all equipment
    fname_coke     = str(data_year)+'_usage_coke.xlsx'     # annual coke usage for calciners
    fname_flarefuel= str(data_year)+'_usage_flarefuel.xlsx'# annual flare-fuel through H2-plant flare
    fname_h2stack  = str(data_year)+'_flow_h2stack.xlsx'   # annual H2-stack flow data
    fname_PSAstack = str(data_year)+'_flow_PSAoffgas_2019COPY.xlsx' # PSA offgas flow data for #2 H2 Plant
    fname_flareEFs = str(data_year)+'_EFs_flare.xlsx'# EFs for H2 flare
    fname_toxicsEFs= str(data_year)+'_EFs_toxics.xlsx'     # EFs for toxics
    fname_toxicsEFs_calciners = str(data_year)+'_EFs_calciner_toxics.xlsx' # EFs for calciners toxics     

    labtab_NG       = '#2H2FdNatGas 2018'  # NG-sample lab-test data
    labtab_RFG      = 'RFG 2018'           # RFG-sample lab-test data
    labtab_cokerFG  = 'Coker FG 2018'      # cokerFG-sample lab-test data
    labtab_CVTG     = 'CVTG 2018'          # CVTG-sample lab-test data
    labtab_flare    = '#2H2 Flare 2018'    # flare-gas sample lab-test data
    labtab_PSA      = 'PSA Offgas 2018'    # PSA-Offgas sample lab-test data

    sheet_fuel = '10-18'

#paths    
fpath_eqmap    = static_prefix+fname_eqmap
fpath_NG_chem  = static_prefix+fname_NG_chem
fpath_FG_chem  = static_prefix+fname_FG_chem
fpath_EFs      = annual_prefix+fname_EFs
fpath_analyses = annual_prefix+fname_analyses
fpath_ewcoker  = annual_prefix+fname_ewcoker
fpath_fuel     = annual_prefix+fname_fuel
fpath_coke     = annual_prefix+fname_coke
fpath_flarefuel= annual_prefix+fname_flarefuel
fpath_h2stack  = annual_prefix+fname_h2stack
fpath_PSAstack = annual_prefix+fname_PSAstack
fpath_flareEFs = annual_prefix+fname_flareEFs
fpath_toxicsEFs= annual_prefix+fname_toxicsEFs
fpath_toxicsEFs_calciners = annual_prefix+fname_toxicsEFs_calciners
    

################################################################################
################################################################################

# just to be explicit: this is the offset for accessing tstamp intervals
month_offset = first_month_to_calculate

equip_to_calculate = [
                      'crude_vtg',
                      'crude_rfg',
                      'n_vac',
                      's_vac',
                      'ref_heater_1',
                      'ref_heater_2',
                      'naptha_heater',
                      'naptha_reboiler',
                      'dhds_heater_3',
                      'hcr_1',
                      'hcr_2',
                      'rxn_r_1',
                      'rxn_r_4',
                      'coker_1',
                      'coker_2',
                      'coker_e',
                      'coker_w',
                      'h2_plant_2',
                      'dhds_heater_1',
                      'dhds_reboiler_1',
                      'dhds_heater_2',
                      'h_furnace_n',
                      'h_furnace_s',
                      'calciner_1',
                      'calciner_2',
                      'iht_heater',
                      'boiler_4',
                      'boiler_5',
                      'boiler_6',
                      'boiler_7',
                      'h2_flare',
                      ]

pollutants_to_calculate = [
                           # criteria
                           'NOx',
                           'CO',
                           'SO2',
                           'VOC',
                           'PM', 'PM25', 'PM10',
                           'H2SO4',
#                           'CO2'
                           ]
if GHG:
    equip_to_calculate = ['coker_e','coker_w']
    pollutants_to_calculate = ['CO2']

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

Qmap = {
        1   : 'Q1',
        2   : 'Q1',
        3   : 'Q1',
        4   : 'Q2',
        5   : 'Q2',
        6   : 'Q2',
        7   : 'Q3',
        8   : 'Q3',
        9   : 'Q3',
        10  : 'Q4',
        11  : 'Q4',
        12  : 'Q4',

        'Jan': 'Q1',
        'Feb': 'Q1',
        'Mar': 'Q1',
        'Apr': 'Q2',
        'May': 'Q2',
        'Jun': 'Q2',
        'Jul': 'Q3',
        'Aug': 'Q3',
        'Sep': 'Q3',
        'Oct': 'Q4',
        'Nov': 'Q4',
        'Dec': 'Q4'
       }

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
        'iht_heater'     : 'heaterboiler',
        'boiler_4'       : 'heaterboiler',
        'boiler_5'       : 'heaterboiler',
        'boiler_6'       : 'heaterboiler',
        'boiler_7'       : 'heaterboiler'
        }

equip_types_to_calculate = set([equip_types[emis_unit] for emis_unit
                                                       in equip_to_calculate])

# column names for final output
output_colnames_map = {
                'month'        : 'Month',
                'equipment'    : 'Equipment',
                'stack_dscfh'  : 'Combined Fuel Gas',
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
                
# this list shouldn't be modified unless we get more or fewer EFs
# in the EF spreadsheet
# access from Equipment instance via: `list(self.toxicsEFs['pollutant'])`
toxics_with_EFs = [                
        'Acenaphthene',
        'Acenaphthylene',
        'Acetaldehyde',
        'Acrolein',
        'Anthracene',
        'Antimony',
        'Arsenic',
        'Barium',
        'Benzene',
        'Benzo(a)anthracene',
        'Benzo(a)pyrene',
        'Benzo(b)fluoranthene',
        'Benzo(g,h,i)perylene',
        'Benzo(k)fluoranthene',
        'Beryllium',
        'Cadmium',
        'Carbon Disulfide',
        'Carbonyl Sulfide',
        'Chromium',
        'Chromium (hexavalent)',
        'Chrysene',
        'Copper',
        'Cyclopentane',
        'Dibenz(a,h)anthracene',
        'Ethylbenzene',
        'Fluoranthene',
        'Fluorene',
        'Formaldehyde',
        'Indene',
        'Indeno(1,2,3-cd)pyrene',
        'Lead',
        'Manganese',
        'Mercury',
        'Methylcyclohexane',
        'Naphthalene',
        'Nickel',
        'Phenanthrene',
        'Phenol',
        'Phosphorus',
        'Propylene',
        'Pyrene',
        'Selenium',
        'Silver',
        'Thallium',
        'Toluene',
        'Xylenes (mixed isomers)',
        'm-xylene',
        'o-xylene',
        'p-xylene',
        'Zinc']

calciner_toxics_with_EFs = [
        'Acetaldehyde',
        'Acrolein',
        'Anthracene',
        'Antimony',
        'Arsenic',
        'Benzene',
        'Benzo(a)pyrene',
        'Beryllium',
        'Cadmium',
        'Chromium',
        'Chrysene',
        'Copper',
        'Formaldehyde',
        'Lead',
        'Manganese',
        'Mercury',
        'Naphthalene',
        'Nickel',
        'Phosphorus',
        'Pyrene',
        'Selenium',
        'Silver',
        'Thallium',
        'Toluene',
        'Xylene',
        'Zinc']

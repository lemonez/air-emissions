import time, os
import pandas as pd

print(time.strftime("%H:%M:%S")+'\tmodule \''+__name__+'\' reloaded.')

####################
##BEGIN USER EDITS##
####################

#### testing/dev/qa iterations
outpath_prefix = 'dev_'

#### configure data/calculations ####
data_year     = 2019  # year_to_calculate = int(time.strftime('%Y'))
data_dir      = './data/'
out_dir       = './output/'
log_dir       = out_dir+'logs/'
log_suffix    = ''

first_month_to_calculate = 1
last_month_to_calculate  = 8

equips_to_calculate = ['coker_e', 'coker_w']
we_are_calculating_toxics = False
calciner_toxics = False

#### configure formatting and logging ####
# select month format for output; defaults to name abbreviations
#   integers     (1, 2, ... 12)
#   name abbrevs ('Jan', 'Feb', ..., 'Dec')
month_names = True

# select verbosity of calculation status logging to console
verbose_calc_status = True

# select timeframe and equipment for emissions calculations
# defaults: last year, all months, all equipment

##################
##END USER EDITS##
##################

months_to_calculate = range(first_month_to_calculate,
                            last_month_to_calculate + 1)
                            
# helper function for debugging
def ends(df, n=2):
    """view first and last rows of df"""
    return pd.concat([df.head(n), df.tail(n)])
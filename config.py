import time, os
import pandas as pd

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
last_month_to_calculate  = 7

# just to be explicit: this is the offset for accessing tstamp intervals
month_offset = first_month_to_calculate

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

def generate_ts_interval_list():
    """Generate (start, end) tuple for each month. Handles leap
    years, differing month lengths, DST offsets, etc."""

    intervals = []
    for mo in months_to_calculate:
        ts_start = pd.to_datetime(str(data_year)+'-'+str(mo)+'-01', format='%Y-%m-%d')
                # Timestamp('2018-01-01 00:00:00')
        ts_end   = ts_start + pd.DateOffset(months=1) - pd.DateOffset(hours=1)
                # Timestamp('2018-01-31 23:00:00')

        interval = (ts_start, ts_end)
        # interval = pd.date_range(start=ts_start, end=ts_end, freq='H')

        intervals.append(interval)
            # [(Timestamp('2018-01-01 00:00:00'), Timestamp('2018-01-31 23:00:00')),
            #  (Timestamp('2018-02-01 00:00:00'), Timestamp('2018-02-28 23:00:00')),
    return intervals

def generate_date_range():
    """generate date_range to fill missing timestamps;
    based on year/months in config.py"""

    tsi = generate_ts_interval_list()
    
    s_tstamp = tsi[0][0]
    e_tstamp = tsi[len(tsi) - 1][1]
    
    dr = pd.date_range(start=s_tstamp, end=e_tstamp, freq='H')
    return dr

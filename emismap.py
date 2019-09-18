import pandas as pd
import config as cf

# print timestamp for checking import timing
import time
print(time.strftime("%H:%M:%S")+'\tmodule \''+__name__+'\' reloaded.')

""" 40 CFR § 98.33 - Calculating GHG emissions. - Tier 4 Calc
    <https://www.law.cornell.edu/cfr/text/40/98.33>
    
    CO2 = 5.18E10−7 * [CO2] * Q          (Equation C-6)

where:
    CO2       = CO2 mass emission rate (tons/hr)
    5.18E10−7 = conversion factor (metric tons/scf/%CO2)
    [CO2]     = hourly average CEMS CO2 concentration (%)
    Q         = hourly average stack gas volumetric flow rate (scf/hr)

(iii) If the CO2 concentration is measured on a dry basis, a correction for
      the stack gas moisture content is required {confirmed with BP that the CO2 CEMS does measure on a dry basis}:
      
    CO2* = CO2 * ( (100-%H2O) / 100)            (Equation C-7)

where:
    CO2*      = hourly CO2 mass emission rate, corrected for moisture (metric tons/hr)
    CO2       = hourly CO2 mass emission rate from Equation C-6, uncorrected (metric tons/hr)
    %H2O      = hourly moisture percentage in stack gas
"""


def generate_ts_interval_list():
    """Generate (start, end) tuple for each month. Handles leap
    years, differing month lengths, DST offsets, etc."""

    intervals = []
    for mo in cf.months_to_calculate:
        ts_start = pd.to_datetime(str(cf.data_year)+'-'+str(mo)+'-01', format='%Y-%m-%d')
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

def parse_annual_ewcoker(path, coker):
    """Read in raw coker data."""

    if coker == 'coker_e':
        cols = [0,1,2,3]
    elif coker == 'coker_w':
        cols = [6,7,8,9]
    
    sheet = 'East & West Coker Data'
    dat = pd.read_excel(path, sheet_name=sheet, usecols=cols, header=3)
    
    dat.replace('--', pd.np.nan, inplace=True)
    dat.columns = ['tstamp', 'o2_%', 'co2_%', 'rfg_mscfh']

    HHV = 1206 # HHV       = self.HHV_RFG
    f_factor  = 8645 # f_factor  = self.f_factor_RFG
    
    dat['dscfh'] = (dat['rfg_mscfh']
                                * 1000 
                                * HHV
                                * 1/1000000
                                * f_factor
                                * 20.9 / (20.9 - dat['o2_%']))
    
    dat.set_index('tstamp', inplace=True)
    return dat

def merge_annual_ewcoker(path):
    """merge east and west coker data"""
    
    ecoker_df = parse_annual_ewcoker(path, 'coker_e')
    wcoker_df = parse_annual_ewcoker(path, 'coker_w')
    
    merged = ecoker_df.merge(wcoker_df, how='outer', left_index=True, right_index=True, suffixes=('_e', '_w'), copy=True)
    return merged

def unmerge_annual_ewcoker(path):
    """unmerge E and W coker data"""
    
    merged_df = merge_annual_ewcoker(path)
    merged_df = merged_df.reindex(generate_date_range())
    
    e_cols = []
    w_cols = []
    for col in merged_df.columns:
        if col.endswith('_e'):
            e_cols.append(col)
        elif col.endswith('_w'):
            w_cols.append(col)
    
    e_un = merged_df[e_cols]
    w_un = merged_df[w_cols]
    
    for df in [e_un, w_un]:
        df.columns = [col[:-2] for col in df.columns]
    return e_un, w_un
    
def calculate_monthly_co2_emissions(path):
    """Calculate coker CO2 emissions."""
    
    e, w = unmerge_annual_ewcoker(path)
    for df in [e, w]:
        df['co2'] = df['co2_%'] * df['dscfh'] * 5.18E-7
        
    return e, w

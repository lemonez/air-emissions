# global import external libraries
import pandas as pd
import time

# module-level imports
import config as cf
import ffactor as ff
import emismap as em

start_time_seconds = time.time()
start_time = time.strftime("%H:%M:%S")
print(start_time+'\tmodule \''+__name__+'\' began reloading.')


class Equipment(object):
    """Class containing information for parsing monthly emissions
    from refinery equipment. Annual data is parsed and stored in
    containers as class variables. Each instance represents
    one month of data from one refinery equipment unit, and
    contains methods for calculating monthly emissions."""
    
    # abbreviations used:
    #   RFG  = refinery fuel gas
    #   EF   = emission factor
    
    # variables shared by all instances
    year = cf.data_year
    
    # directories
    data_prefix    = cf.data_dir               # all input data files
    annual_prefix  = data_prefix+'annual/'     # data that changes monthly/annually
    static_prefix  = data_prefix+'static/'     # static data
    
    # files
    fname_FG_chem  = 'chemicals_FG.csv'               # FG static chemical data
    fname_NG_chem  = 'chemicals_NG.csv'               # NG static chemical data    
    fname_RFG      = str(year)+'_analyses_RFG.xlsx'   # RFG-sample lab-test data
    fname_NG       = str(year)+'_analyses_NG.xlsx'    # NG-sample lab-test data
    fname_cokerFG  = str(year)+'_analyses_cokerFG.xlsx' # coker-gas sample lab-test data
    fname_ewcoker  = '2019_data_EWcoker.xlsx'         # coker cems, fuel, flow data
    
    # paths
    fpath_FG_chem  = static_prefix+fname_FG_chem
    fpath_NG_chem  = static_prefix+fname_NG_chem
    fpath_RFG      = annual_prefix+fname_RFG
    fpath_NG       = annual_prefix+fname_NG
    fpath_cokerFG  = annual_prefix+fname_cokerFG
    fpath_ewcoker  = annual_prefix+fname_ewcoker
    
    # consts, dicts, dfs (indented descriptions follow variable assignments)
    ts_intervals   = em.generate_ts_interval_list()
                    # df: hourly cems data for all equipment
    unitID_equip   = {'XX ecoker': 'coker_e', 'YY wcoker': 'coker_w'} # replace w/ dynamic
    unitkey_name   = {'coker_e':'East Coker', 'coker_w':'West Coker'} # replace w/ dynamic
    RFG_annual     = ff.parse_annual_FG_lab_results(fpath_RFG)
                    # df: annual RFG lab-test results
    NG_annual      = ff.parse_annual_NG_lab_results(fpath_NG)
                    # df: annual NG lab-test results
    cokerFG_annual   = ff.parse_annual_FG_lab_results(fpath_cokerFG)
                    # df: annual coker-gas lab-test results                    
    col_name_order = ['equipment', 'month', 'rfg_mscfh', 'co2'] # replace 'rfg_mscfh' with 'fuel_rfg' once units are figured out

    def __init__(self, unit_key, month):
        
        self.unit_key       = unit_key
        self.month          = month
        
        # instance attributes calculated at instance level
        self.ts_interval    = self.ts_intervals[self.month - cf.month_offset]
        
        # gas-sample lab results (DataFrames)
        self.RFG_monthly    = ff.get_monthly_lab_results(self.RFG_annual,
                                                         self.ts_interval)
        
        # fuel higher heating values (floats)
        self.HHV_RFG        = ff.calculate_monthly_HHV(self.RFG_monthly)
        self.HHV_NG         = ff.calculate_monthly_HHV(self.NG_monthly)
        self.HHV_cokerFG      = ff.calculate_monthly_HHV(self.coker_monthly)
        
        # fuel f-factors (floats) calculated using static chem data
        self.f_factor_RFG   = ff.calculate_monthly_f_factor(self.RFG_monthly,
                                        self.fpath_FG_chem, self.ts_interval)
        self.f_factor_NG    = ff.calculate_monthly_f_factor(self.NG_monthly,
                                        self.fpath_NG_chem, self.ts_interval)
        self.f_factor_cokerFG = ff.calculate_monthly_f_factor(self.cokerFG_monthly,
                                        self.fpath_FG_chem, self.ts_interval)
                                        
        # annual CO2 emissions
        self.e_co2, self.w_co2 = em.calculate_monthly_co2_emissions(self.fpath_ewcoker)

    def get_monthly_co2_emissions(self):
        """return co2 emissions for given month"""
        
        if self.unit_key == 'coker_e':
            co2 = self.e_co2
        if self.unit_key == 'coker_w':
            co2 = self.w_co2
        
        return co2.loc[self.ts_intervals[self.month - cf.month_offset][0]:
                       self.ts_intervals[self.month - cf.month_offset][1]]

    def calc_monthly_equip_emissions(self, hourly_df):
        """return series with monthly emissions"""
        
        monthly = hourly_df.sum()
        monthly.loc['equipment'] = self.unit_key
        monthly.loc['month'] = self.month
        monthly = monthly.reindex(self.col_name_order)
        return monthly

# print timestamp for checking import timing
end_time_seconds = time.time()
end_time = time.strftime("%H:%M:%S")
print(end_time+'\tmodule \''+__name__+'\' finished reloading.')

total_time = round(end_time_seconds - start_time_seconds)
print('\t(\''+__name__+'\' total module load time: '
             +str(total_time)+' seconds)')

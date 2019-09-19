start_time_seconds = time.time()
start_time = time.strftime("%H:%M:%S")
print(start_time+'\tmodule \''+__name__+'\' began reloading.')

##============================================================================##

# global import external libraries
import pandas as pd
import time

# module-level imports
import config as cf
import ffactor as ff

class AnnualEquipment(object):
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
    fname_NG       = str(year)+'_analyses_NG_DUMMY.xlsx'    # NG-sample lab-test data
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
    ts_intervals   = cf.generate_ts_interval_list()
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

    def __init__(self):
        """Parse facility-wide equipment-related data."""
        print('AnnualEquipment() init\'ed')

    # def get_monthly_co2_emissions(self):
        # """return co2 emissions for given month"""
        
        # if self.unit_key == 'coker_e':
            # co2 = self.e_co2
        # if self.unit_key == 'coker_w':
            # co2 = self.w_co2
        
        # return co2.loc[self.ts_intervals[self.month - cf.month_offset][0]:
                       # self.ts_intervals[self.month - cf.month_offset][1]]

    def calc_monthly_equip_emissions(self, hourly_df):
        """return series with monthly emissions"""
        
        monthly = hourly_df.sum()
        monthly.loc['equipment'] = self.unit_key
        monthly.loc['month'] = self.month
        monthly = monthly.reindex(self.col_name_order)
        return monthly

class AnnualCoker(AnnualEquipment):
    """Parse and store annual coker data."""
    
    def __init__(self,
                 unit_key):
        """"""
        super().__init__()
        self.unit_key = unit_key
    
    def unmerge_annual_ewcoker(self):
        """Unmerge E/W coker data."""

        merged_df = self.merge_annual_ewcoker()
        merged_df = merged_df.reindex(cf.generate_date_range())

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
    
    def implement_pilot_gas_logic(self):
        """Implement logic to add pilot gas to E/W coker values depending on operational status."""
        pass
    
    def merge_annual_ewcoker(self):
        """Merge E/W coker data."""

        ecoker_df = self.parse_annual_ewcoker(cols=[0,1,2,3])
        wcoker_df = self.parse_annual_ewcoker(cols=[6,7,8,9])

        merged = ecoker_df.merge(wcoker_df, how='outer', left_index=True, right_index=True, suffixes=('_e', '_w'), copy=True)
        return merged
        
    def parse_annual_ewcoker(self, cols):
        """Read in raw coker data."""
        
        sheet = 'East & West Coker Data'
        dat = pd.read_excel(self.fpath_ewcoker, sheet_name=sheet, usecols=cols, header=3)

        dat.replace('--', pd.np.nan, inplace=True)
        dat.columns = ['tstamp', 'o2_%', 'co2_%', 'rfg_mscfh']
        dat.set_index('tstamp', inplace=True)
        return dat

class MonthlyCoker(AnnualCoker):
    """Calculate monthly coker data."""
    """
    Equipment Units:
        'coker_e'
        'coker_w'
    """
    
    def __init__(self,
                 unit_key,
                 month):
        super().__init__(unit_key)
        self.month = month
        
        # instance attributes calculated at month level
        self.ts_interval    = self.ts_intervals[self.month - cf.month_offset]
        
        # gas-sample lab results (DataFrames)
        self.RFG_monthly    = ff.get_monthly_lab_results(self.RFG_annual,
                                                         self.ts_interval)
        self.NG_monthly     = ff.get_monthly_lab_results(self.NG_annual,
                                                         self.ts_interval)
        self.cokerFG_monthly  = ff.get_monthly_lab_results(self.cokerFG_annual,
                                                         self.ts_interval)
        
        # fuel higher heating values (floats)
        self.HHV_RFG        = ff.calculate_monthly_HHV(self.RFG_monthly)
        self.HHV_NG         = ff.calculate_monthly_HHV(self.NG_monthly)
        self.HHV_cokerFG      = ff.calculate_monthly_HHV(self.cokerFG_monthly)
        
        # fuel f-factors (floats) calculated using static chem data
        self.f_factor_RFG   = ff.calculate_monthly_f_factor(self.RFG_monthly,
                                        self.fpath_FG_chem, self.ts_interval)
        self.f_factor_NG    = ff.calculate_monthly_f_factor(self.NG_monthly,
                                        self.fpath_NG_chem, self.ts_interval)
        self.f_factor_cokerFG = ff.calculate_monthly_f_factor(self.cokerFG_monthly,
                                        self.fpath_FG_chem, self.ts_interval)

    def calculate_monthly_co2_emissions(self):
        """Return coker CO2 emissions for given month."""
        
        """
        40 CFR § 98.33 - Calculating GHG emissions. - Tier 4 Calc
            <https://www.law.cornell.edu/cfr/text/40/98.33>
            
            CO2 = 5.18E10−7 * [CO2] * Q                    (Equation C-6)

        where:
            CO2       = CO2 mass emission rate (tons/hr)
            5.18E10−7 = conversion factor (metric tons/scf/%CO2)
            [CO2]     = hourly average CEMS CO2 concentration (%)
            Q         = hourly average stack gas volumetric flow rate (scf/hr)

        (iii) If the CO2 concentration is measured on a dry basis, a correction for
              the stack gas moisture content is required {confirmed with BP that the CO2 CEMS does measure on a dry basis}:
              
            CO2* = CO2 * ( (100-%H2O) / 100)               (Equation C-7)

        where:
            CO2*      = hourly CO2 mass emission rate, corrected for moisture (metric tons/hr)
            CO2       = hourly CO2 mass emission rate from Equation C-6, uncorrected (metric tons/hr)
            %H2O      = hourly moisture percentage in stack gas
        """
        
        e_df, w_df = self.unmerge_annual_ewcoker()
        
        if self.unit_key == 'coker_e':
            df = e_df
        if self.unit_key == 'coker_w':
            df = w_df

        df['dscfh'] = (df['rfg_mscfh']
                            * 1000 
                            * self.HHV_RFG
                            * 1/1000000
                            * self.f_factor_RFG
                            * 20.9 / (20.9 - df['o2_%']))

        df['co2'] = (df['co2_%'] * df['dscfh'] * 5.18E-7)

        return df.loc[self.ts_intervals[self.month - cf.month_offset][0]:
                       self.ts_intervals[self.month - cf.month_offset][1]]

##============================================================================##

# print timestamp for checking import timing
end_time_seconds = time.time()
end_time = time.strftime("%H:%M:%S")
print(end_time+'\tmodule \''+__name__+'\' finished reloading.')

total_time = round(end_time_seconds - start_time_seconds)
print('\t(\''+__name__+'\' total module load time: '
             +str(total_time)+' seconds)')

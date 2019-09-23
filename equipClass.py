# global import external libraries
import pandas as pd
import time

start_time_seconds = time.time()
start_time = time.strftime("%H:%M:%S")
print(start_time+'\tmodule \''+__name__+'\' began reloading.')

##============================================================================##

# module-level imports
import config as cf
import ffactor as ff

class AnnualEquipment(object):
    """Contains annual facility-wide data for emissions calculations."""
    """
    This class contains information for parsing monthly emissions for
    equipment units. Annual data is parsed and stored here as class variables.
    Each instance represents one year of data. from one refinery equipment unit, and
    contains methods for calculating monthly emissions.
    
    Abbreviations:
        EF   = emission factor
        FG   = fuel gas (general term; includes RFG, NG, flare gas)
        RFG  = refinery fuel gas
        NG   = natural gas
    """
    
    # variables shared by all instances
    year = cf.data_year
    
    # directories
    data_prefix    = cf.data_dir               # all input data files
    annual_prefix  = data_prefix+'annual/'     # data that changes monthly/annually
    static_prefix  = data_prefix+'static/'     # static data
    
    # files
    fname_eqmap    = 'equipmap.csv'                   # all equipment names / IDs    
    fname_FG_chem  = 'chemicals_FG.csv'               # FG static chemical data
    fname_NG_chem  = 'chemicals_NG.csv'               # NG static chemical data    
    fname_NG       = str(year)+'_analyses_NG_DUMMY.xlsx'    # NG-sample lab-test data
    fname_cokerFG  = str(year)+'_analyses_cokerFG.xlsx' # coker-gas sample lab-test data
    fname_ewcoker  = '2019_data_EWcoker.xlsx'         # coker cems, fuel, flow data
    
    # paths
    fpath_eqmap    = static_prefix+fname_eqmap
    fpath_FG_chem  = static_prefix+fname_FG_chem
    fpath_NG_chem  = static_prefix+fname_NG_chem
    fpath_NG       = annual_prefix+fname_NG
    fpath_cokerFG  = annual_prefix+fname_cokerFG
    fpath_ewcoker  = annual_prefix+fname_ewcoker
    
    # consts, dicts, dfs (indented descriptions follow variable assignments)
    ts_intervals   = cf.generate_ts_interval_list()
    equip          = cf.parse_equip_map(fpath_eqmap)
                    # df: all equipment/PItag data
    unitID_equip   = cf.generate_unitID_unitkey_dict(equip)
                    # dict: unit IDs (WED Pts) and their Python GUIDs    
                    # unitID_equip   = {'XX ecoker': 'coker_e',
                                      # 'YY wcoker': 'coker_w'}
    unitkey_name   = cf.generate_unitkey_unitname_dict(equip)
                    # dict: Python GUIDs and their unit names for output 
                    # unitkey_name   = {'coker_e':'East Coker Heater',
                                      # 'coker_w':'West Coker Heater'}
    NG_annual      = ff.parse_annual_NG_lab_results(fpath_NG)
                    # df: annual NG lab-test results
    cokerFG_annual = ff.parse_annual_FG_lab_results(fpath_cokerFG)
                    # df: annual cokerFG lab-test results

    def __init__(self):
        """Contstructor for parsing annual emission-unit data."""
        # print(repr(self.__class__)+' init\'ed (**DEBUG**)')
        print('AnnualEquipment() init\'ed')        
        

class AnnualCoker(AnnualEquipment):
    """Parse and store annual coker data."""
    
    def __init__(self):
        """Constructor for parsing annual coker data."""
        super().__init__()
        print('AnnualCoker() init\'ed')
    
    def unmerge_annual_ewcoker(self):
        """Unmerge E/W coker data."""
        merged_df = self.implement_pilot_gas_logic()
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
        """Distribute pilot gas to E/W cokers depending on operational status."""
        
        df = self.merge_annual_ewcoker()
        
        # DUMMY DATA
        df['pilot_mscfh'] = 0.2
        # df.loc['2019-04-29 22:00:00':'2019-04-29 23:00:00', 'rfg_mscfh_e'] = pd.np.nan
        # df.loc['2019-04-30 00:00:00':'2019-04-30 00:00:00', ['rfg_mscfh_e', 'rfg_mscfh_w']] = [pd.np.nan, 110]

        df['pilot_mscfh_e'] = pd.np.nan
        df['pilot_mscfh_w'] = pd.np.nan
        
        # if both units down
        df.loc[(df['rfg_mscfh_e'].isnull())  & (df['rfg_mscfh_w'].isnull()),  ['pilot_mscfh_e', 'pilot_mscfh_w']] = pd.np.nan

        # if east down and west up
        df.loc[(df['rfg_mscfh_e'].isnull())  & (df['rfg_mscfh_w'].notnull()), 'pilot_mscfh_w'] = df['pilot_mscfh']

        # if west down and east up
        df.loc[(df['rfg_mscfh_e'].notnull()) & (df['rfg_mscfh_w'].isnull()),  'pilot_mscfh_e'] = df['pilot_mscfh']

        # if both units up
        df.loc[(df['rfg_mscfh_e'].notnull()) & (df['rfg_mscfh_w'].notnull()), ['pilot_mscfh_e', 'pilot_mscfh_w']] = df['pilot_mscfh'] / 2
        
        return df
    
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
    """Calculate monthly coker emissions."""
    """
    Emission Units:
        'coker_e'
        'coker_w'
    """
# replace 'rfg_mscfh' with 'fuel_rfg' once units are figured out
    col_name_order = ['equipment', 'month', 'rfg_mscfh', 'co2']
    
    def __init__(self,
                 unit_key,
                 month):
        """Constructor for individual coker emission unit calculations."""
        super().__init__()
        self.month = month
        self.unit_key = unit_key
        
        # instance attributes calculated at month level
        self.ts_interval    = self.ts_intervals[self.month - cf.month_offset]
        
        # gas-sample lab results (DataFrames)
        self.NG_monthly     = ff.get_monthly_lab_results(self.NG_annual,
                                                         self.ts_interval)
        self.cokerFG_monthly  = ff.get_monthly_lab_results(self.cokerFG_annual,
                                                         self.ts_interval)
        
        # fuel higher heating values (floats)
        self.HHV_NG         = ff.calculate_monthly_HHV(self.NG_monthly)
        self.HHV_cokerFG      = ff.calculate_monthly_HHV(self.cokerFG_monthly)
        
        # fuel f-factors (floats) calculated using static chem data
        self.f_factor_NG    = ff.calculate_monthly_f_factor(self.NG_monthly,
                                        self.fpath_NG_chem, self.ts_interval)
        self.f_factor_cokerFG = ff.calculate_monthly_f_factor(self.cokerFG_monthly,
                                        self.fpath_FG_chem, self.ts_interval)
        
        print('MonthlyCoker() init\'ed')
    
    def calculate_monthly_equip_emissions(self):
        """Return monthly emissions as pd.Series."""
        
        hourly = self.calculate_monthly_co2_emissions()
        monthly = hourly.sum()
        monthly.loc['equipment'] = self.unit_key
        monthly.loc['month'] = self.month
        monthly = monthly.reindex(self.col_name_order)
        return monthly

    def calculate_monthly_co2_emissions(self):
        """Return coker CO2 emissions for given month as pd.DataFrame."""
        
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

        df['rfg_scfh'] = (df['rfg_mscfh']
                            * 1000 
                            * self.HHV_cokerFG
                            * 1/1000000
                            * self.f_factor_cokerFG
                            * 20.9 / (20.9 - df['o2_%']))

        df['pilot_scfh'] = (df['rfg_mscfh']
                            * 1000 
                            * self.HHV_NG
                            * 1/1000000
                            * self.f_factor_NG
                            * 20.9 / (20.9 - df['o2_%']))

        df['dscfh'] = df['rfg_scfh'] + df['pilot_scfh']
        df['co2']   = df['co2_%'] * df['dscfh'] * 5.18E-7

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

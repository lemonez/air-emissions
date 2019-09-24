import time
import pandas as pd
from collections import OrderedDict

# print timestamp for checking import timing
# start_time_seconds = time.time()
# start_time = time.strftime("%H:%M:%S")
# print(start_time+'\tmodule \''+__name__+'\' began reloading.')

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
    
    def __init__(self):
        """Constructor for parsing annual emission-unit data."""
        self.year           = cf.data_year
        self.months_to_calc = cf.months_to_calculate
        self.month_offset   = cf.month_offset
        
        # directories
        self.data_prefix    = cf.data_dir               # all input data files
        self.annual_prefix  = self.data_prefix+'annual/'# data that changes monthly/annually
        self.static_prefix  = self.data_prefix+'static/'# static data
        
        # files
        self.fname_eqmap    = 'equipmap.csv'            # all equipment names / IDs    
        self.fname_FG_chem  = 'chemicals_FG.csv'        # FG static chemical data
        self.fname_NG_chem  = 'chemicals_NG.csv'        # NG static chemical data    
        self.fname_NG       = str(self.year)+'_analyses_NG.xlsx'# NG-sample lab-test data
        self.fname_cokerFG  = str(self.year)+'_analyses_cokerFG.xlsx' # coker-gas sample lab-test data
        self.fname_ewcoker  = '2019_data_EWcoker.xlsx'  # coker cems, fuel, flow data
        
        # paths
        self.fpath_eqmap    = self.static_prefix+self.fname_eqmap
        self.fpath_FG_chem  = self.static_prefix+self.fname_FG_chem
        self.fpath_NG_chem  = self.static_prefix+self.fname_NG_chem
        self.fpath_NG       = self.annual_prefix+self.fname_NG
        self.fpath_cokerFG  = self.annual_prefix+self.fname_cokerFG
        self.fpath_ewcoker  = self.annual_prefix+self.fname_ewcoker
        
        # consts, dicts, dfs (indented descriptions follow variable assignments)
        self.ts_intervals   = self.generate_ts_interval_list()
            
        # equipment mapping
        self.equip          = self.parse_equip_map()
        self.unitID_equip   = self.generate_unitID_unitkey_dict()
        self.unitkey_name   = self.generate_unitkey_unitname_dict()
        
        # fuel analysis data
        self.NG_annual      = ff.parse_annual_NG_lab_results(self.fpath_NG)
        self.cokerFG_annual = ff.parse_annual_FG_lab_results(self.fpath_cokerFG)
    
    def generate_unitkey_unitname_dict(self):
        """Return dict of equipment keys ({unit_key: unit_name})"""
        """
        ex: {'coker_e': 'Coker E Charge Heater',
             'coker_w': 'Coker W Charge Heater'}
        """
        unitkey_unitname_dict = (self.equip
                                 .drop_duplicates(subset='unit_key')
                                 .set_index('unit_key')['unit_name']
                                 .to_dict())
        return unitkey_unitname_dict
    
    def generate_unitID_unitkey_dict(self):
        """Return OrderedDict of equipment IDs ({unit_id: unit_key})"""
        """
        ex: OrderedDict([('40_E', 'coker_e'),
                         ('41_W', 'coker_w')])
        """
        unitID_unitkey_dict = (self.equip.set_index('unit_id')['unit_key']
                                            .to_dict(into=OrderedDict))
        return unitID_unitkey_dict

    def parse_equip_map(self):
        """Read CSV of equipment information and return pd.DataFrame."""
        col_map = {
            'PI Tag'     : 'ptag',
            'Python GUID': 'unit_key',
            'WED Pt'     : 'unit_id', 
            'Unit Name'  : 'unit_name',
            'CEMS'       : 'param',
            'Units'      : 'units'
            }
        
        equip_info = pd.read_csv(self.fpath_eqmap).rename(columns=col_map)
        equip_info['param'] = equip_info['param'].str.strip().str.lower()
        equip_info['units'] = equip_info['units'].str.strip().str.lower()
        equip_info.replace({'dry o2': 'o2'}, inplace=True)
                # equip_info.loc[equip_info['ptag'] == '46AI601.PV', 'param'] = 'o2' # change 'dry o2' to 'o2' for H2 Plant
        equip_info['param_units'] = equip_info['param']+'_'+equip_info['units']
                # .set_index(['unit_key', 'ptag'])
                # .set_index('PI Tag')
                # .rename_axis('ptag', axis=0))
        return equip_info

    def generate_date_range(self):
        """Generate pd.date_range to fill timestamps."""
        tsi = self.generate_ts_interval_list()
        s_tstamp = tsi[0][0]
        e_tstamp = tsi[len(tsi) - 1][1]
        dr = pd.date_range(start=s_tstamp, end=e_tstamp, freq='H')
        return dr

    def generate_ts_interval_list(self):
        """Return list of monthly datetime tuples (start, end)."""
        intervals = []
        for mo in self.months_to_calc:
            ts_start = pd.to_datetime(str(self.year)+'-'+str(mo)+'-01', format='%Y-%m-%d') # Timestamp('2018-01-01 00:00:00')
            ts_end   = ts_start + pd.DateOffset(months=1) - pd.DateOffset(hours=1) # Timestamp('2018-01-31 23:00:00')
            interval = (ts_start, ts_end) # interval = pd.date_range(start=ts_start, end=ts_end, freq='H')
            intervals.append(interval)
        return intervals

class AnnualCoker(AnnualEquipment):
    """Parse and store annual coker data."""
    
    def __init__(self, annual_equip):
        """Constructor for parsing annual coker data."""
        self.annual_equip = annual_equip
    
    def unmerge_annual_ewcoker(self):
        """Unmerge E/W coker data."""
        merged_df = self.implement_pilot_gas_logic()
        merged_df = merged_df.reindex(self.annual_equip.generate_date_range())

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

        df['pilot_mscfh_e'] = pd.np.nan
        df['pilot_mscfh_w'] = pd.np.nan
        
        # if both units down
        df.loc[(df['cokerfg_mscfh_e'].isnull())  & (df['cokerfg_mscfh_w'].isnull()),
                ['pilot_mscfh_e', 'pilot_mscfh_w']] = pd.np.nan

        # if east down and west up
        df.loc[(df['cokerfg_mscfh_e'].isnull())  & (df['cokerfg_mscfh_w'].notnull()),
                'pilot_mscfh_w'] = df['pilot_mscfh']

        # if west down and east up
        df.loc[(df['cokerfg_mscfh_e'].notnull()) & (df['cokerfg_mscfh_w'].isnull()),
                'pilot_mscfh_e'] = df['pilot_mscfh']

        # if both units up
        df.loc[(df['cokerfg_mscfh_e'].notnull()) & (df['cokerfg_mscfh_w'].notnull()),
                ['pilot_mscfh_e', 'pilot_mscfh_w']] = df['pilot_mscfh'] / 2
        
        return df
    
    def merge_annual_ewcoker(self):
        """Merge E/W coker data."""
        ecoker_df = self.parse_annual_ewcoker(cols=[0,1,2,3])
        wcoker_df = self.parse_annual_ewcoker(cols=[6,7,8,9])
        pilot_df  = self.parse_annual_ewcoker(cols=[12,13], pilot=True)
        
        merged1 = ecoker_df.merge(wcoker_df, how='outer',
                                    left_index=True, right_index=True,
                                    suffixes=('_e', '_w'), copy=True)
        merged2 = merged1.merge(pilot_df, how='outer',
                                    left_index=True, right_index=True,
                                                           copy=True)
        return merged2
    
    def parse_annual_ewcoker(self, cols, pilot=False):
        """Read in raw coker data."""
        sheet = 'East & West Coker Data'
        dat = pd.read_excel(self.annual_equip.fpath_ewcoker,
                            sheet_name=sheet, usecols=cols, header=3)
        dat.replace('--', pd.np.nan, inplace=True)

        if not pilot:
            dat.columns = ['tstamp', 'o2_%', 'co2_%', 'cokerfg_mscfh']
        elif pilot:
            dat.columns = ['tstamp', 'pilot_mscfh']

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
    col_name_order = ['equipment', 'month',
                      'cokerfg_mscfh', 'pilot_mscfh', 'co2']
    
    def __init__(self,
                 unit_key,
                 month,
                 annual_coker):
        """Constructor for individual coker emission unit calculations."""
#        super().__init__()
        self.month          = month
        self.unit_key       = unit_key        
        self.annual_coker   = annual_coker
        self.annual_equip   = annual_coker.annual_equip
        
        # instance attributes calculated at month level
        self.ts_interval    = self.annual_equip.ts_intervals[self.month
                                            - self.annual_equip.month_offset]
        
        # gas-sample lab results (DataFrames)
        self.NG_monthly     = ff.get_monthly_lab_results(
                                            self.annual_equip.NG_annual,
                                            self.ts_interval)
        self.cokerFG_monthly = ff.get_monthly_lab_results(
                                            self.annual_equip.cokerFG_annual,
                                            self.ts_interval)
        
        # fuel higher heating values (floats)
        self.HHV_NG         = ff.calculate_monthly_HHV(self.NG_monthly)
        self.HHV_cokerFG    = ff.calculate_monthly_HHV(self.cokerFG_monthly)
        
        # fuel f-factors (floats) calculated using static chem data
        self.f_factor_NG    = ff.calculate_monthly_f_factor(
                                            self.NG_monthly,
                                            self.annual_equip.fpath_NG_chem,
                                            self.ts_interval)
        self.f_factor_cokerFG = ff.calculate_monthly_f_factor(
                                            self.cokerFG_monthly,
                                            self.annual_equip.fpath_FG_chem,
                                            self.ts_interval)
    
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

        df['cokerfg_scfh'] = (df['cokerfg_mscfh']
                            * 1000 
                            * self.HHV_cokerFG
                            * 1/1000000
                            * self.f_factor_cokerFG
                            * 20.9 / (20.9 - df['o2_%']))

        df['pilot_scfh'] = (df['pilot_mscfh']
                            * 1000 
                            * self.HHV_NG
                            * 1/1000000
                            * self.f_factor_NG
                            * 20.9 / (20.9 - df['o2_%']))

        df['dscfh'] = df['cokerfg_scfh'] + df['pilot_scfh']
        df['co2']   = df['co2_%'] * df['dscfh'] * 5.18E-7

        return df.loc[self.ts_interval[0]:self.ts_interval[1]]

##============================================================================##

# print timestamp for checking import timing
# end_time_seconds = time.time()
# end_time = time.strftime("%H:%M:%S")
# print(end_time+'\tmodule \''+__name__+'\' finished reloading.')

# total_time = round(end_time_seconds - start_time_seconds)
# print('\t(\''+__name__+'\' total module load time: '
             # +str(total_time)+' seconds)')

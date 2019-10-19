import time, glob
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
    This class contains methods for parsing monthly emissions for refinery
    equipment units. Annual data is parsed and stored here as instance attributes,
    one instance representing one calendar year of data.
    
    Abbreviations:
        EF   = emission factor
        FG   = fuel gas (general term; RFG, cokerFG, flare gas)
        RFG  = refinery fuel gas
        NG   = natural gas
        CVTG = crude vac tail gas
        CEMS = continuous emission monitoring system
        GUID = global unique identifier
    """
    def __init__(self):
        """Constructor for listing data paths and emissions unit information."""
        # temporal data constraints
        self.year           = cf.data_year
        self.months_to_calc = cf.months_to_calculate
        self.month_offset   = cf.month_offset
        self.ts_intervals   = self.generate_ts_interval_list()
                             # list of tuples: [monthly intervals (start, end)]
        
        # formatting
        self.col_name_order = ['equipment', 'month', 'fuel_rfg', 'fuel_ng',
                               'nox', 'co', 'so2', 'voc',
                               'pm', 'pm25', 'pm10', 'h2so4']
                             # list: col order for emissions summaries

        # directories
        self.data_prefix    = cf.data_dir               # all input data files
        self.annual_prefix  = self.data_prefix+'annual/'# data that changes monthly/annually
        self.static_prefix  = self.data_prefix+'static/'# static data
        self.CEMS_dir       = self.annual_prefix+'CEMS/'# monthly CEMS data
        
        # files
        self.fname_eqmap    = 'equipmap.csv'            # all equipment names / IDs    
        self.fname_NG_chem  = 'chemicals_NG.csv'        # NG static chemical data    
        self.fname_FG_chem  = 'chemicals_FG.csv'        # FG static chemical data
        self.fname_analyses = str(self.year)+'_analyses_agg.xlsx'       # all gas lab-test data
        #self.fname_RFG      = str(self.year)+'_analyses_RFG.xlsx'   # RFG-sample lab-test data
        #self.fname_NG       = str(self.year)+'_analyses_NG.xlsx'    # NG-sample lab-test data
        #self.fname_CVTG     = str(self.year)+'_analyses_CVTG.xlsx'  # CVTG-sample lab-test data
        #self.fname_flare    = str(self.year)+'_analyses_flare.xlsx' # flare-gas sample lab-test data
        #self.fname_cokerFG  = str(self.year)+'_analyses_cokerFG.xlsx' # coker-gas sample lab-test data
        self.fname_ewcoker  = str(self.year)+'_data_EWcoker.xlsx'   # coker CEMS, fuel, flow data
        self.fname_fuel     = str(self.year)+'_usage_fuel.xlsx'     # annual fuel usage for all equipment
        self.fname_coke     = str(self.year)+'_usage_coke.xlsx'     # annual coke usage for calciners
        self.fname_flarefuel= str(self.year)+'_usage_flarefuel.xlsx'# annual flare-fuel through H2-plant flare
        self.fname_h2stack  = str(self.year)+'_flow_h2stack.xlsx'   # annual H2-stack flow data
        self.fname_flareEFs = str(self.year)+'_EFs_flare_DUMMY.xlsx'      # EFs for H2 flare
        self.fname_toxicsEFs= str(self.year)+'_EFs_toxics.xlsx'     # EFs for toxics
        self.fname_toxicsEFs_calciners = str(self.year)+'_EFs_calciner_toxics.xlsx' # EFs for calciners toxics     
        self.fname_EFs      = 'EFs_monthly_DUMMY.xlsx'        # monthly-EF excel workbook

        # paths
        self.fpath_eqmap    = self.static_prefix+self.fname_eqmap
        self.fpath_NG_chem  = self.static_prefix+self.fname_NG_chem
        self.fpath_FG_chem  = self.static_prefix+self.fname_FG_chem
        self.fpath_analyses = self.annual_prefix+self.fname_analyses
        #self.fpath_RFG      = self.annual_prefix+self.fname_RFG
        #self.fpath_NG       = self.annual_prefix+self.fname_NG
        #self.fpath_flare    = self.annual_prefix+self.fname_flare
        #self.fpath_CVTG     = self.annual_prefix+self.fname_CVTG
        #self.fpath_cokerFG  = self.annual_prefix+self.fname_cokerFG
        self.fpath_ewcoker  = self.annual_prefix+self.fname_ewcoker
        self.fpath_fuel     = self.annual_prefix+self.fname_fuel
        self.fpath_coke     = self.annual_prefix+self.fname_coke
        self.fpath_flarefuel= self.annual_prefix+self.fname_flarefuel
        self.fpath_h2stack  = self.annual_prefix+self.fname_h2stack 
        self.fpath_flareEFs = self.annual_prefix+self.fname_flareEFs
        self.fpath_toxicsEFs= self.annual_prefix+self.fname_toxicsEFs
        self.fpath_toxicsEFs_calciners = self.annual_prefix \
                                            +self.fname_toxicsEFs_calciners
        self.fpath_EFs      = self.annual_prefix+self.fname_EFs
        
        # fuel analysis tabs
        self.labtab_NG       = '#2H2FdNatGas 2019'
        self.labtab_RFG      = 'RFG 2019'
        self.labtab_cokerFG  = 'Coker FG 2019'
        self.labtab_CVTG     = 'CVTG 2019'
        self.labtab_flare    = '#2H2 Flare 2019'
        
        # equipment mapping (indented descriptions follow assignments)
        self.equip          = self.parse_equip_map()
                             # df: all equipment/PItag data
        self.equip_ptags    = self.generate_equip_ptag_dict()
                             # dict: {equipment: [PItag, PItag, ...]}
        self.ptags_pols     = self.generate_ptag_pol_dict()
                             # dict: {PItag: [pollutant, pollutant, ...]}
        self.unitID_equip   = self.generate_unitID_unitkey_dict()
                             # dict: {WED Pt: Python GUID}
        self.ordered_equip  = self.generate_ordered_equip_list()
                             # list: [Python GUIDs ordered ascending by WED Pt]
        self.unitkey_name   = self.generate_unitkey_unitname_dict()
                             # dict: {Python GUID: pretty name for output}
                             
        self.parse_annual_facility_data()

    def parse_annual_facility_data(self):
        """Parse facility-wide annual data."""
        start_time_seconds = time.time()
        start_time = time.strftime("%H:%M:%S")
        print('began parsing annual data at '+start_time)

        # CEMS, fuel analysis and usage, EFs (indented descriptions follow assignments)
        self.CEMS_annual    = self.parse_all_monthly_CEMS()
                             # df: annual CEMS data
        self.NG_annual      = ff.parse_annual_NG_lab_results(self.fpath_analyses,
                                                             self.labtab_NG)
                             # df: annual NG lab-test results
        self.RFG_annual     = ff.parse_annual_FG_lab_results(self.fpath_analyses,
                                                             self.labtab_RFG)
                             # df: annual RFG lab-test results
        self.cokerFG_annual = ff.parse_annual_FG_lab_results(self.fpath_analyses,
                                                             self.labtab_cokerFG)
                             # df: annual cokerFG lab-test results
        self.CVTG_annual    = ff.parse_annual_FG_lab_results(self.fpath_analyses,
                                                             self.labtab_cokerFG)
                             # df: annual CVTG lab-test results
        self.flare_annual   = ff.parse_annual_FG_lab_results(self.fpath_analyses,
                                                             self.labtab_flare)
                             # df: annual flare-gas lab-test results
        self.fuel_annual    = self.parse_annual_fuel()
                             # df: hourly fuel data for all equipment
        self.flarefuel_annual = self.parse_annual_flare_fuel()
                             # df: hourly flare-gas data
        self.coke_annual    = self.parse_annual_coke()
                             # df: hourly coke data for all calciners
        self.h2stack_annual = self.parse_annual_h2stack()
                             # df: hourly coke data for all calciners
        self.flareEFs       = self.parse_annual_flare_EFs()
                             # df: EFs for flare gas
        #self.toxicsEFs      = self.parse_annual_toxics_EFs()
        #                     # df: EFs for toxics
        #self.toxicsEFs_calciners = self.parse_annual_calciner_toxics_EFs()
        #                     # df: EFs for calciner toxics
        self.EFs            = self.parse_annual_EFs()
                             # dict: {integer month: (EFs df, EFunits df, equip_EF_dict)}
        
        end_time_seconds = time.time()
        end_time = time.strftime("%H:%M:%S")
        print('completed parsing annual data at '+end_time)
        total_time = round(end_time_seconds - start_time_seconds)
        print('Total init time: '+str(total_time)+' seconds)')
    
    def parse_annual_EFs(self):
        """Parse EF tabs for specified months; return dict of tuples."""
        """
        dict structure: {integer month: (EFs df, EFunits df, equip_EF_dict)}
        """
        annual_EF_container = {}
        for month in self.months_to_calc:
            tabname = self.ts_intervals[month-1][0].strftime('%Y_%m') # monthly EF excel tab named as 'YYYY_MM'
            annual_EF_container[month] = self.parse_EF_tab(tabname) # tuple (a, b, c) is saved to dict 
        return annual_EF_container
    
    def parse_EF_tab(self, tab):
        """Read monthly EF data, return tuple (values df, units df, {equip:[EFs]} dict."""
        """
            *df: value of each EF for each equipment
            *df: units of each EF for each equipment
            *dict: {equipment: {dict of EFs}}
        """
        
        efs = pd.read_excel(self.fpath_EFs, sheet_name=tab, skiprows=5,
                                            header=None, usecols='A:E')
        efs.columns = ['unit_id', 'src_name_BP', 'pollutant', 'ef', 'units']
        
        # forward-fill the IDs/names
        efs['unit_id'].fillna(method='ffill', inplace=True)
        efs['src_name_BP'].fillna(method='ffill', inplace=True)
        efs['unit_id'] = efs['unit_id'].astype(str)
            #efs['src_id'] = efs['src_id'].apply(lambda x: int(x) if isinstance(x, int) or isinstance(x, float) else str(x))
        
        # strip whitespace, make lowercase to standardize the values
        efs['pollutant'] = efs['pollutant'].str.strip().str.lower()
        efs['units'] = efs['units'].str.strip().str.lower()
    #    efs['pollutant'].replace({'pm10':'pm'}, inplace=True)
        efs['units'].replace({'lb pm/ton coke':'lb/ton coke'}, inplace=True)
        
        # subset pollutant EFs, discard cems, change EFs to numeric
        efs = efs[efs['pollutant'].isin(['nox', 'co', 'so2', 'voc',
                                         'pm', 'pm25', 'pm10', 'h2so4'])]
        
        # discard CEMS so that to_numeric() does not throw error
        efs = efs[~efs['ef'].str.lower().isin(['cems'])]
        efs['ef'] = pd.to_numeric(efs['ef'])
        
        efs['unit_key'] = efs['unit_id'] # for Python unit ID
        
        # map BP IDs to Python IDs
        efs = efs.replace({'unit_key': self.unitID_equip})
                          #,'unit_name':equip_name_map})
    # begin temporary workaround   
        # for now, set calciner EFs to 0.0 for pollutants calculated from coke
        # efs.loc[( ((efs['unit_id'] == '70') | (efs['unit_id'] == '71')) & 
                  # (efs['pollutant'].isin(['co', 'pm', 'h2so4'])) 
                # ), 'ef'] = 0
    # end temporary workaround
        
        # if PM fractions are all being set to equal PM total, then replace EFs
        if not cf.calculate_PM_fractions:
            efs.loc[efs['pollutant'].isin(['pm25','pm10']),'ef'] = pd.np.nan
            efs.fillna(method='ffill', limit=2,axis=0, inplace=True)
        
        # pare down the EF dataframe to be accessed for conversion factors
        pollutant_units = (efs[['unit_key','pollutant','units']]
                            .groupby(['unit_key','pollutant'])
                            .sum())
                # efs[(st.index == 'crude_rfg') & (efs['pollutant'] == 'co')]['units'].loc['crude_rfg']

        equip_EF_dict = pd.pivot_table(efs,
                                       index=['unit_key'],
                                       columns='pollutant',
                                       values='ef').T.to_dict()
        
        return efs, pollutant_units, equip_EF_dict

#not yet implemented/tested for 2019
    def parse_annual_calciner_toxics_EFs(self):
        """Parse calciner EF data, return pd.DataFrame."""
        """
        source (?): https://www3.epa.gov/ttn/chief/efpac/protocol/Protocol%20Report%202015.pdf
        """
        caltox = pd.read_excel(self.fpath_toxicsEFs_calciners,
                                header=7, skipfooter=49, usecols=[0, 4, 6])
        caltox = caltox[1:]
        caltox.columns = ['unit_id', 'pollutant', 'ef']
        # EFs are the same b/w calciners, so for now we can drop calciner_2 EFs
        caltox = caltox[(caltox['unit_id'] == 70)]# | (caltox['unit_id'] == 71)]
        caltox = caltox[(caltox['pollutant'] != '1,1,1-Trichloroethane') &
                        (caltox['pollutant'] != 'Vanadium')]
        caltox['units'] = 'lbs/ton calcined coke'
        return caltox
    
#not yet implemented/tested for 2019
    def parse_annual_toxics_EFs(self):
        """Parse EFs for toxics; return pd.DataFrame."""
        toxics = pd.read_excel(self.fpath_toxicsEFs, skipfooter=5,
                               #skiprows=3,
                               header=3, usecols=[0,2,3])
        
        # because the source test EF for Pb for boiler #5 equals
        # the EF for the rest of the equipment, drop that row and 
        # treat it the same; could add exception for boiler #5
        # in the future but not necessary for 2018 emissions
        toxics = toxics.iloc[:-1]
        toxics.rename(columns={'Chemical Name':'pollutant',
                               'ICR - EF'     : 'ef',
                               'EF Units'     : 'units'},
                      inplace=True)
        return toxics
    
    def parse_annual_flare_EFs(self):
        """Read EFs from H2-flare gas data; return pd.DataFrame."""
        # read, clean, format, subset data
        df = pd.read_excel(self.fpath_flareEFs, sheet_name='Summary',
                                            skiprows=29, usecols=[0,1,2])
        df.columns = df.columns.str.lower()
        df.rename(columns={'value':'ef'}, inplace=True)
        df['pollutant'] = df['pollutant'].str.strip().str.lower()
        df['units'] = df['units'].str.strip().str.lower()
        df.loc[:,'flare_on'] = 'unassigned'
        
        # create separate EF DataFrames for pilot gas vs. active flaring
        flare_off_EFs = df.iloc[0:5]
        flare_off_EFs.loc[:,'flare_on'] = False
        flare_on_EFs = df.iloc[8:13]
        flare_on_EFs.loc[:,'flare_on'] = True
        
        # combine cleaned-up DataFrames and cast as numeric
        flare_EFs = pd.concat([flare_off_EFs, flare_on_EFs])
        # the following returns SettingWithCopyWarning; can't figure out how to fix
        flare_EFs.loc[:,'ef'] = pd.to_numeric(flare_EFs.loc[:,'ef'])
        return flare_EFs
    
    def parse_annual_h2stack(self):
        """Read annual stack-flow data for H2 plant, return pd.DataFrame."""
        stack = pd.read_excel(self.fpath_h2stack, skiprows=2)
        stack['tstamp'] = pd.to_datetime(stack['1 h'])
        stack.set_index('tstamp', inplace=True)
        stack['dscfh'] = stack['46FY38B.PV'] * 1000 # convert to dscfh
        stack = pd.DataFrame(stack['dscfh'])
        stack['dscfh'] = pd.to_numeric(stack.loc[:,'dscfh'], errors='coerce')
        
        stack = stack.clip(lower=0)
        return stack
    
    def parse_annual_coke(self):
        """Read annual coke data for calciners, return pd.DataFrame."""
        coke_df = pd.read_excel(self.fpath_coke, skiprows=[0,1], header=[0,1])
        coke_df.columns = coke_df.columns.droplevel(1)
        coke_df.columns.name = 'unit_key'
        
        # sum hearths 1 and 2, rename, reindex
        coke_df['calciner_1'] = coke_df['20WK5000.PV'] + coke_df['20WK5001.PV']
        coke_df.rename(columns={'20WK5002.PV':'calciner_2'}, inplace=True)
        coke_df['tstamp'] = pd.to_datetime(coke_df['1h'])
        coke_df.set_index('tstamp', inplace=True)
        coke_df = coke_df[['calciner_1', 'calciner_2']]
        if not (coke_df.dtypes.unique()[0] == 'float64' and 
                len(coke_df.dtypes.unique()) == 1):
            coke_df.replace({"[-11059] No Good Data For Calculation": pd.np.nan},
                                                                    inplace=True)
        coke_df = coke_df.clip(lower=0)
        return coke_df    
    
    def parse_annual_flare_fuel(self):
        """Read annual fuel flow data for H2-plant flare, return pd.DataFrame."""
        flare_df = pd.read_excel(self.fpath_flarefuel, skiprows=4)
        
        flare_df.rename(columns={'1 h':'tstamp',
                                 '46FI202.PV':'flare_header_flow',
                                 '46FI231.PV':'discharge_to_flare',
                                 '46PC60.OP':'valve'}
                        , inplace=True)
        flare_df['tstamp'] = pd.to_datetime(flare_df['tstamp'])
        flare_df.set_index('tstamp', inplace=True)
        
        # check if all values are not float (aka if there are strings)
        if not (flare_df.dtypes.unique()[0] == 'float64' and 
                len(flare_df.dtypes.unique()) == 1):
            flare_df.replace({"[-11059] No Good Data For Calculation": pd.np.nan},
                                                                    inplace=True)
        flare_df['flare_header_flow'] = flare_df['flare_header_flow'].clip(lower=0)
        flare_df['discharge_to_flare'] = flare_df['discharge_to_flare'].clip(lower=0)
        return flare_df
    
    def parse_annual_fuel(self):
        """Read annual fuel data for all equipment, return pd.DataFrame."""
        """
        df structure: (WED Pt. x Timestamp)
        df size: <2MB storage for year of data
        """
        fuel = pd.read_excel(self.fpath_fuel, skiprows=9, header=list(range(6)))
        fuel.columns = fuel.columns.droplevel(5)
        fuel.columns = fuel.columns.droplevel(4)
        fuel.columns = fuel.columns.droplevel(1)
        fuel.set_index(fuel.columns.tolist()[0], inplace=True)
        fuel.index.name = 'tstamp'
        
        # subset out columns of interest,
        # because (usecols='A:AC, AF:AL, AP:AR') gives ValueError
        fuel = pd.concat([fuel.iloc[:,0:30],
                          fuel.iloc[:,32:40],
                          fuel.iloc[:,42:45]], axis=1)
        fuel.columns.set_names(['unit_id', 'p_tag', 'units'],
                               level=[0,1,2], inplace=True)
        
        fuel = fuel.apply(pd.to_numeric, errors='coerce')
        
        # calc fuel use for equipment that require summing multiple PI tags
        fuel[('10 VTG'  , 'CALC', 'MSCFH')] = fuel.loc[
                :,['10 VTG_sum1','10 VTG_sum2']].sum(axis=1)
        fuel[('20'      , 'CALC', 'MSCFH')] = fuel.loc[
                :,['20_sum1', '20_sum2', '20_sum3', '20_sum4',]].sum(axis=1)
        fuel[('21'      , 'CALC', 'MSCFH')] = fuel.loc[
                :,['21_sum1', '21_sum2', '21_sum3', '21_sum4',]].sum(axis=1)
        fuel[('70 (RFG)', 'CALC', 'MSCFH')] = fuel.loc[
                :,['70_sum1_RFG', '70_sum2_RFG',]].sum(axis=1)
        
        # convert SCFH --> MSCFH where required
        fuel[('52'     , '26FI774.PV' , 'MSCFH')] = fuel.loc[
                :,(52,      '26FI774.PV',  'SCFH')] / 1000
        fuel[('70 (NG)', '20FI272.PV' , 'MSCFH')] = fuel.loc[
                :,('70_NG', '20FI272.PV',  'SCFH')] / 1000
        fuel[('71 (NG)', '20FI1913.PV', 'MSCFH')] = fuel.loc[
                :,('71_NG', '20FI1913.PV', 'SCFH')] / 1000
        
        # rename columns in order to drop all with underscore ('_') character
        fuel[('71 (RFG)', '20FI1914.PV', 'MSCFH')] = fuel.loc[
                :,('71_RFG', '20FI1914.PV', 'MSCFH')]
        
        col_names_0 = list(fuel.columns.get_level_values(0).map(str))
        
        drop_cols = []
        for elem in col_names_0:
            if '_' in elem:
                drop_cols.append(elem)
        
        fuel.drop(labels=drop_cols, axis='columns', level=0, inplace=True)
        fuel.drop(labels=(52, '26FI774.PV', 'SCFH'), axis='columns', inplace=True)
        
        # drop multiindex levels reduce down to single column index
        fuel.columns = fuel.columns.droplevel(2) # in MSCFH so units unneeded
        fuel.columns = fuel.columns.droplevel(1) # have unit IDs so PI tags unneeded
        
        mapped_names = []
        for s in list(fuel.columns.map(str)):
            if s in self.unitID_equip.keys():
                mapped_names.append(self.unitID_equip[s])
            else:
                mapped_names.append(s)
        
        fuel.columns = mapped_names
        
        fuel = fuel.clip(lower=0)
        return fuel
    
    def parse_all_monthly_CEMS(self):
        """Combine monthly CEMS data into annual, return pd.DataFrame."""
        """
        df structure: (WED Pt. x Timestamp)
        df size: <1MB for year of data
        """
        CEMS_paths = sorted(glob.glob(self.CEMS_dir+'*'))
        
        monthly_CEMS = []
        for fpath in CEMS_paths:
            month = self.parse_monthly_CEMS(fpath)
            monthly_CEMS.append(month)
        
        annual_CEMS = (pd.concat(monthly_CEMS)
                         .sort_values(['ptag', 'tstamp'])
                         .set_index('tstamp'))
        
        annual_CEMS['val'] = annual_CEMS['val'].clip(lower=0)
        return annual_CEMS
    
    @staticmethod
    def parse_monthly_CEMS(path):
        """Read one month of hourly CEMS data, return pd.DataFrame."""
        cems_df = pd.read_csv(path, usecols=[1,2,3], header=None)
        cems_df.columns = ['ptag', 'tstamp', 'val']
        cems_df['tstamp'] = pd.to_datetime(cems_df['tstamp'])
        print('    parsed CEMS data in: '+path)
        return cems_df
    
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
    
    def generate_ordered_equip_list(self):
        """Generate list of equipment ordered by WED Pt. for CSV output."""
        dkeys = list(self.unitID_equip.keys())
        equip_list = []
        for k in dkeys:
            equip_list.append(self.unitID_equip[k])
        return equip_list
    
    def generate_unitID_unitkey_dict(self):
        """Return OrderedDict of equipment IDs ({unit_id: unit_key})"""
        """
        ex: OrderedDict([('40_E', 'coker_e'),
                         ('41_W', 'coker_w')])
        """
        unitID_unitkey_dict = (self.equip.set_index('unit_id')['unit_key']
                                            .to_dict(into=OrderedDict))
        return unitID_unitkey_dict
    
    def generate_ptag_pol_dict(self):
        """Return dictionary of ({PItag : pol_units})."""
        
        # make dictionary of PI tags
        ptag_pols_dict = (self.equip
                          .dropna(axis=0, subset=['ptag'])
                          .set_index('ptag')['param_units']
                          .to_dict())
        return ptag_pols_dict
    
    def generate_equip_ptag_dict(self):
        """Return dictionary of equipment ({unit_key: [PItags_list]})"""
        
        equip_df = self.equip
        # subset to exclude H2S and others
        sub = (equip_df[equip_df['param'].isin(['nox', 'so2', 'co', 'o2'])]
                                             .loc[:,['ptag','unit_key']]
                                             .dropna(subset=['ptag'])
                                             .set_index('unit_key'))
        
        dups = sub.index[sub.index.duplicated()].unique().tolist()
        
        equip_ptag_dict = {}
        for k in sub.index.unique():
            if k in dups:
                equip_ptag_dict[k] = sub.loc[k].loc[
                                            :,sub.loc['s_vac'].columns[0]].tolist()
            else:
                equip_ptag_dict[k] = sub.loc[k].tolist()
        
        return equip_ptag_dict
    
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

class AnnualHB(AnnualEquipment):
    """Parse and store annual heater/boiler data."""
    def __init__(self, annual_equip):
        """Constructor for parsing annual heater/boiler data."""
        self.annual_equip = annual_equip

class MonthlyHB(AnnualHB):
    """Calculate monthly heater/boiler emissions."""
    """
    Emission Units:
        'crude_rfg'
        'crude_vtg'
        'n_vac'
        's_vac'
        'ref_heater_1'
        'ref_heater_2'
        'naptha_heater'
        'naptha_reboiler'
        'dhds_heater_3'
        'hcr_1'
        'hcr_2'
        'rxn_r_1'
        'rxn_r_4'
        'dhds_heater_1'
        'dhds_heater_1'
        'dhds_reboiler_1'
        'dhds_reboiler_1'
        'dhds_heater_2'
        'h_furnace_n'
        'h_furnace_s'
        'boiler_4'
        'boiler_5'
        'boiler_6'
        'boiler_7'
    """
    def __init__(self,
             unit_key,
             month,
             annual_eu):
        """Constructor for individual emission unit calculations."""
                # instance attributes passed as args
        self.unit_key       = unit_key
        self.month          = month
        self.annual_eu      = annual_eu # aka Annual{equiptype}()
        self.annual_equip   = annual_eu.annual_equip # aka AnnualEquipment()
        
        self.ts_interval    = self.annual_equip.ts_intervals[self.month
                                                - self.annual_equip.month_offset]
        self.EFs            = self.annual_equip.EFs[self.month][0]
        self.EFunits        = self.annual_equip.EFs[self.month][1]
        self.equip_EF       = self.annual_equip.EFs[self.month][2]
        self.col_name_order = self.annual_equip.col_name_order
        self.equip_ptags    = self.annual_equip.equip_ptags
        self.ptags_pols     = self.annual_equip.ptags_pols
        
        # fuel-sample lab results (pd.DataFrame)
        self.RFG_monthly    = ff.get_monthly_lab_results(self.annual_equip.RFG_annual, self.ts_interval)
        self.CVTG_monthly   = ff.get_monthly_lab_results(self.annual_equip.CVTG_annual, self.ts_interval)
        # fuel higher heating values (float)
        self.HHV_RFG        = ff.calculate_monthly_HHV(self.RFG_monthly)
        self.HHV_CVTG       = ff.calculate_monthly_HHV(self.CVTG_monthly)
        # fuel f-factors (floats) calculated using static chem data
#        self.f_factor_RFG   = ff.calculate_monthly_f_factor(
#                                            self.RFG_monthly,
#                                            self.annual_equip.fpath_FG_chem,
#                                            self.ts_interval)
        self.f_factor_CVTG  = ff.calculate_monthly_f_factor(
                                            self.CVTG_monthly,
                                            self.annual_equip.fpath_FG_chem,
                                            self.ts_interval)

    def calculate_monthly_equip_emissions(self):
        """Return pd.DataFrame of calculated monthly emissions."""
    # does it return a series or a df?
        """
        Calculate emissions for pollutants w/out CEMS based on
        fuel (or coke), EFs, and HHVs.
        """
        hourly_df = self.convert_from_ppm()
        monthly = hourly_df.sum()
        # calculate all pollutants except for H2SO4
        if 'calciner' in self.unit_key:
            coke_tons = self.get_monthly_coke()['coke_tons'].sum()
            stack_dscf = monthly.loc['dscfh']

        for pol in ['nox', 'co', 'so2', 'voc', 'pm', 'pm25', 'pm10']:
            # if no CEMS
            if pol not in monthly.index:
                # need this logic to avoid errors while PM25 & PM 10 EFs are added
                if pol not in self.EFunits.loc[self.unit_key].index:
                    monthly.loc[pol] = -9999*2000/12 # error flag that will show up as -9999
                else:
                    if (self.EFunits.loc[self.unit_key]
                                    .loc[pol]
                                    .loc['units'] == 'lb/hr'):
                        # don't multiply by fuel quantity if EF is in lb/hr
                        EF_multiplier = 1
                    else:
                        if self.unit_key == 'h2_plant_2':
                            fuel_type = 'fuel_ng'
                        else:
                            fuel_type = 'fuel_rfg'
                    
                        EF_multiplier = monthly.loc[fuel_type]
                    
                    if 'calciner' in self.unit_key:
                        if pol in ['co', 'voc']:
                            EF_multiplier = coke_tons
                        if pol in ['pm', 'pm25', 'pm10']:
                            if 'calciner_1' in self.unit_key:
                                EF_multiplier = coke_tons
                            if 'calciner_2' in self.unit_key:
                                EF_multiplier = stack_dscf / 1000            
                    
                    monthly.loc[pol] = (EF_multiplier
                                        * self.equip_EF[self.unit_key][pol]
                                        * self.get_conversion_multiplier(pol))
            
        # now calculate H2SO4 separately
        if 'calciner' in self.unit_key:
            EF_multiplier = coke_tons
            monthly.loc['h2so4'] = (EF_multiplier
                                    * self.equip_EF[self.unit_key]['h2so4'])
        else:
            monthly.loc['h2so4'] = monthly.loc['so2'] * 0.026
            
        # set other values in series
        monthly.loc['equipment'] = self.unit_key
        monthly.loc['month'] = self.month
        monthly = monthly.reindex(self.col_name_order)
        monthly.loc[self.col_name_order[4:-1]] = monthly.loc[
                                self.col_name_order[4:-1]] / 2000 # lbs --> tons
        
# begin temporary workaround        
        # if fuel not used in emis calc, set to zero
        if self.unit_key in ['h2_plant_2', 'calciner_1', 'calciner_2']:
            monthly.loc['fuel_ng'] = 0
# end temporary workaround
        
        return monthly
    
    def convert_from_ppm(self):
        """Convert CEMS from ppm if needed, return pd.DataFrame of hourly emissions values."""
        """
        Merge fuel and CEMS data, convert ppm values to lb/hr from
        list of pollutants with CEMS.
        """
        both_df = pd.concat([self.get_monthly_fuel(),
                             self.get_monthly_CEMS()],
                             axis=1)
        
        if self.unit_key in self.equip_ptags.keys():
            ptags_list = self.equip_ptags[self.unit_key]
            cems_pol_list = [self.ptags_pols[tag]
                             for tag in ptags_list
                             if self.ptags_pols[tag] != 'o2_%']
            
            if self.unit_key == 'h2_plant_2':
                stack_df = self.get_monthly_h2stack()
                both_df = pd.concat([both_df, stack_df], axis=1)
            else:
                fuel_type = 'fuel_rfg'
                f_factor  = self.f_factor_RFG
                HHV       = self.HHV_RFG
                
                both_df['dscfh'] = (both_df[fuel_type]
                                    * 1000 
                                    * HHV
                                    * 1/1000000
                                    * f_factor
                                    * 20.9 / (20.9 - both_df['o2_%']))
            
            # if there are CEMS pols to convert)
            if 'calciner' in self.unit_key:
                both_df = self.calculate_calciner_total_stack_flow(both_df)
            
            ppm_conv_facts = {
                             #       MW      const   hr/min
                             'nox': (46.1 * 1.557E-7 / 60), # 1.196e-07
                             'co' : (28   * 1.557E-7 / 60), # 7.277e-08
                             'so2': (64   * 1.557E-7 / 60)  # 1.661e-07
                              }
            for pol in cems_pol_list:
                both_df[pol.split('_')[0]] = (
                                        both_df[pol]
                                        * ppm_conv_facts[pol.split('_')[0]]
                                        * both_df['dscfh'])
        return both_df
    
    def get_conversion_multiplier(self, pol):
        """Pass pollutant name, return float multiplier to convert emissions to lbs."""
        if (self.EFunits.loc[self.unit_key]
                        .loc[pol].loc['units'] == 'lb/mmbtu'):
            if self.unit_key in ['coker_1', 'coker_2']:
                multiplier = 1/1000 * self.HHV_coker
            elif self.unit_key == 'crude_vtg':
                multiplier = 1/1000 * self.HHV_CVTG
            else:
                multiplier = 1/1000 * self.HHV_RFG
        elif (self.EFunits.loc[self.unit_key]
                        .loc[pol].loc['units'] == 'lb/mscf'):
            multiplier = 1
        elif (self.EFunits.loc[self.unit_key]
                        .loc[pol].loc['units'] == 'lb/mmscf'):
            multiplier = 1/1000
        elif (self.EFunits.loc[self.unit_key]
                        .loc[pol].loc['units'] == 'lb/hr'):
            fuel_type = 'fuel_rfg'
            if self.unit_key == 'h2_plant_2':
                fuel_type = 'fuel_ng'
            fuel_df = self.get_monthly_fuel()
            # count total hours where (>1 mscf) fuel was burned
            multiplier = fuel_df.loc[fuel_df[fuel_type] > 1, fuel_type].count()
        else:
            multiplier = 1
        return multiplier
    
    def get_monthly_CEMS(self):
        """Return pd.DataFrame of emis unit CEMS data for specified month."""
        CEMS_annual = self.annual_equip.CEMS_annual.copy()
        if self.unit_key in self.equip_ptags.keys():
            ptags_list = self.equip_ptags[self.unit_key]
            # subset PItags of interest
            sub = CEMS_annual[CEMS_annual['ptag'].isin(ptags_list)]
            sub.reset_index(inplace=True)
            sub_pivot = (sub.pivot(index='tstamp',
                                   columns="ptag",
                                   values="val")
                            .rename(columns=self.ptags_pols))
            sub2_pivot = sub_pivot.loc[
                            self.ts_interval[0]:
                            self.ts_interval[1]]
            return sub2_pivot
        else: # empty df if no CEMS
            no_CEMS = pd.DataFrame({'no_CEMS_data' : []})
            return no_CEMS        
    
    def get_monthly_fuel(self):
        """Return pd.DataFrame of emis unit fuel usage for specified month."""
        fuel_data = self.annual_equip.fuel_annual
        
        if self.unit_key == 'h2_plant_2': # only natural gas
            ng_fuel_ser = (fuel_data.loc[
                            self.ts_interval[0]:
                            self.ts_interval[1],
                            self.unit_key])
            fuel_df = ng_fuel_ser.to_frame('fuel_ng')
            fuel_df['fuel_rfg'] = 0
            fuel_df = fuel_df[['fuel_rfg', 'fuel_ng']] # reorder for consistency
        elif self.unit_key == 'calciner_1': # rfg and natural gas
            rfg_fuel_ser = (fuel_data.loc[
                            self.ts_interval[0]:
                            self.ts_interval[1],
                            '70 (RFG)'])
            ng_fuel_ser  = (fuel_data.loc[
                            self.ts_interval[0]:
                            self.ts_interval[1],
                            '70 (NG)'])
            fuel_df = pd.concat([rfg_fuel_ser, ng_fuel_ser], axis=1)
            fuel_df.columns = ['fuel_rfg', 'fuel_ng']
        elif self.unit_key == 'calciner_2': # rfg and natural gas
            rfg_fuel_ser = (fuel_data.loc[
                            self.ts_interval[0]:
                            self.ts_interval[1],
                            '71 (RFG)'])
            ng_fuel_ser  = (fuel_data.loc[
                            self.ts_interval[0]:
                            self.ts_interval[1],
                            '71 (NG)'])
            fuel_df = pd.concat([rfg_fuel_ser, ng_fuel_ser], axis=1)
            fuel_df.columns = ['fuel_rfg', 'fuel_ng']
        else: # only rfg
            rfg_fuel_ser = (fuel_data.loc[
                            self.ts_interval[0]:
                            self.ts_interval[1],
                            self.unit_key])
            fuel_df = rfg_fuel_ser.to_frame('fuel_rfg')
            fuel_df['fuel_ng'] = 0
        return fuel_df

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
                 annual_eu):
        """Constructor for individual coker emission unit calculations."""
#        super().__init__()
        self.month          = month
        self.unit_key       = unit_key
        self.annual_eu      = annual_eu # aka AnnualCoker()
        self.annual_equip   = annual_eu.annual_equip # aka AnnualEquipment()
        
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

        df['cokerfg_dscfh'] = (df['cokerfg_mscfh']
                            * 1000 
                            * self.HHV_cokerFG
                            * 1/1000000
                            * self.f_factor_cokerFG
                            * 20.9 / (20.9 - df['o2_%']))

        df['pilot_dscfh'] = (df['pilot_mscfh']
                            * 1000 
                            * self.HHV_NG
                            * 1/1000000
                            * self.f_factor_NG
                            * 20.9 / (20.9 - df['o2_%']))

        df['dscfh'] = df['cokerfg_dscfh'] + df['pilot_dscfh']
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

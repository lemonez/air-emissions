import time, datetime, glob, math
import pandas as pd
import numpy as np
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
        self.ts_intervals   = self._generate_ts_interval_list()
                             # list of tuples: [monthly intervals (start, end)]
        
        # file paths
        self.fpath_eqmap    = cf.fpath_eqmap
        self.fpath_NG_chem  = cf.fpath_NG_chem
        self.fpath_FG_chem  = cf.fpath_FG_chem
        self.fpath_EFs      = cf.fpath_EFs
        self.fpath_analyses = cf.fpath_analyses
        self.fpath_ewcoker  = cf.fpath_ewcoker
        self.fpath_fuel     = cf.fpath_fuel
        self.fpath_coke     = cf.fpath_coke
        self.fpath_flarefuel= cf.fpath_flarefuel
        self.fpath_h2stack  = cf.fpath_h2stack
        self.fpath_PSAstack = cf.fpath_PSAstack
        self.fpath_flareEFs = cf.fpath_flareEFs
        self.fpath_toxicsEFs= cf.fpath_toxicsEFs
        self.fpath_toxicsEFs_calciners = cf.fpath_toxicsEFs_calciners
        
        # fuel lab analysis tabs
        self.labtab_NG      = cf.labtab_NG     
        self.labtab_RFG     = cf.labtab_RFG    
        self.labtab_cokerFG = cf.labtab_cokerFG
        self.labtab_CVTG    = cf.labtab_CVTG   
        self.labtab_flare   = cf.labtab_flare  
        self.labtab_PSA     = cf.labtab_PSA    
                
        self.sheet_fuel     = cf.sheet_fuel
        
        # formatting list: col order for emissions summaries
        self.col_name_order = ['equipment', 'month', 'fuel_rfg', 'fuel_ng',
                               'co', 'nox', 'pm', 'pm25', 'pm10',
                               'so2', 'voc', 'h2so4']
        
        # equipment mapping containers
        self.equip          = self._parse_equip_map()                # df: all equipment/PItag data
        self.equip_ptags    = self._generate_equip_ptag_dict()       # dict: {equipment: [PItag, PItag, ...]}
        self.ptags_pols     = self._generate_ptag_pol_dict()         # dict: {PItag: [pollutant, pollutant, ...]}
        self.unitID_equip   = self._generate_unitID_unitkey_dict()   # dict: {WED Pt: Python GUID}
        self.ordered_equip  = self._generate_ordered_equip_list()    # list: [Python GUIDs ordered ascending by WED Pt]
        self.unitkey_name   = self._generate_unitkey_unitname_dict() # dict: {Python GUID: pretty name for output}
        self.h2s_cems_map = cf.h2s_cems_map

        self._parse_annual_facility_data()
    
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
#++DATA-PARSING METHODS CALLED BY self._parse_annual_facility_data()+++++++++++#
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
    def _parse_annual_facility_data(self):
        """Parse facility-wide annual data."""
        start_time_seconds = time.time()
        start_time = time.strftime("%H:%M:%S")
        print('began parsing annual data at '+start_time)

        # CEMS, fuel analysis and usage, EFs (indented descriptions follow assignments)
        if not cf.equip_to_calculate == ['h2_flare']:
            print('  parsing CEMS data')
            self.CEMS_annual    = self._parse_all_monthly_CEMS()
                                 # df: annual CEMS data
        print('  parsing lab analysis data')
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
        self.PSA_annual     = ff.parse_annual_FG_lab_results(self.fpath_analyses,
                                                             self.labtab_PSA)
                             # df: annual flare-gas lab-test results
        print('    parsed lab analysis data')
        print('  parsing fuel data')
        self.fuel_annual    = self._parse_annual_fuel()
                             # df: hourly fuel data for all equipment
        self.flarefuel_annual = self._parse_annual_flare_fuel()
                             # df: hourly flare-gas data
        if cf.data_year != 2018:
            self.flareHHV_annual= self._upsample_annual_flare_HHV()
                             # series: hourly flare-gas HHV
        print('    parsed fuel data')
        print('  parsing emission factor data')
        self.flareEFs       = self._parse_annual_flare_EFs()
                             # df: EFs for flare gas
        self.toxicsEFs      = self._parse_annual_toxics_EFs()
                            # df: EFs for fuel gas / natural gas toxics
        self.toxicsEFs_calciners = self._parse_annual_calciner_toxics_EFs()
                            # df: EFs for calciner toxics
        self.EFs            = self._parse_annual_EFs()
                             # dict: {integer month: (EFs df, EFunits df, equip_EF_dict)}
        print('    parsed emission factor data')
        
        end_time_seconds = time.time()
        end_time = time.strftime("%H:%M:%S")
        print('completed parsing annual data at '+end_time)
        total_time = round(end_time_seconds - start_time_seconds)
        print('Total init time: '+str(total_time)+' seconds)')
    
    def _parse_annual_EFs(self):
        """Parse EF tabs for specified months; return dict of tuples."""
        """
        dict structure: {integer month: (EFs df, EFunits df, equip_EF_dict)}
        """
        annual_EF_container = {}
        for month in self.months_to_calc:
            tabname = self.ts_intervals[month-self.month_offset][0].strftime('%Y_%m') # monthly EF excel tab named as 'YYYY_MM'
            annual_EF_container[month] = self._parse_EF_tab(tabname) # tuple (a, b, c) is saved to dict 
        return annual_EF_container
    
    def _parse_EF_tab(self, tab):
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
        
        # subset pollutant EFs
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
    
    def _parse_annual_calciner_toxics_EFs(self):
        """Parse calciner EF data, return pd.DataFrame."""
        """
        source (?): https://www3.epa.gov/ttn/chief/efpac/protocol/Protocol%20Report%202015.pdf
        """
        caltox = pd.read_excel(self.fpath_toxicsEFs_calciners,
                               header=7, skipfooter=49,
                               usecols=[0, 4, 6, 8, 9])
        caltox = caltox[1:]
        caltox.columns = ['unit_id', 'pollutant', 'ef_uncontrolled', 'scrubber_control', 'WESP_control']
        # EFs are the same b/w calciners, so for now we can drop calciner_2 EFs
        caltox = caltox[(caltox['unit_id'] == 70)]# | (caltox['unit_id'] == 71)]
        caltox = caltox[(caltox['pollutant'] != '1,1,1-Trichloroethane') &
                        (caltox['pollutant'] != 'Vanadium')]
        caltox['units'] = 'lbs/ton calcined coke'
        caltox['ef'] = ( caltox['ef_uncontrolled']
                       * ((100 - caltox['scrubber_control']) / 100)
                       * ((100 - caltox['WESP_control']) / 100)
                       )
        return caltox[['pollutant', 'ef', 'units']]
    
    def _parse_annual_toxics_EFs(self):
        """Parse EFs for fuel gas / natural gas toxics; return pd.DataFrame."""
        toxics = pd.read_excel(self.fpath_toxicsEFs,
                               header=3,
                               usecols=[0,2,3],
                               nrows=89)
        toxics.rename(columns={'Chemical' : 'pollutant',
                               'EF'       : 'ef',
                               'EF Units' : 'units'},
                      inplace=True)
        toxics['units'] = toxics['units'].str.strip().str.lower()
        # calculating H2S from CEMS, so drop from EF list
        toxics= toxics[toxics['pollutant'] != 'Hydrogen sulfide']
        return toxics
    
    def _parse_annual_flare_EFs(self):
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
        flare_off_EFs = df.copy().iloc[0:5]
        flare_off_EFs.loc[:,'flare_on'] = False
        flare_on_EFs = df.copy().iloc[8:13]
        flare_on_EFs.loc[:,'flare_on'] = True
        
        # combine cleaned-up DataFrames and cast as numeric
        flare_EFs = pd.concat([flare_off_EFs, flare_on_EFs])
        # the following returns SettingWithCopyWarning; can't figure out how to fix
        flare_EFs.loc[:,'ef'] = pd.to_numeric(flare_EFs.copy().loc[:,'ef'])
        return flare_EFs
    
    def _upsample_annual_flare_HHV(self):
        """Upsample/ffill flare HHV values to hourly, return hourly pd.Series."""
        ser = self.flare_annual.loc['GBTU/CF'].copy()
        ser = ser.append(pd.Series(index=[ser.index[-1]+ datetime.timedelta(days=1)]))
        ser.index = ser.index.normalize()
        ser = ser.resample('1H').ffill()
        ser = ser.iloc[:-1]
        ser.name = 'HHV_flare'
        return ser
    
    def _parse_annual_flare_fuel(self):
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
    
    def _parse_annual_fuel(self):
        """Read annual fuel data for all equipment, return pd.DataFrame."""
        """
        df structure: (WED Pt. x Timestamp)
        df size: <2MB storage for year of data
        """
        fuel = pd.read_excel(self.fpath_fuel, sheet_name=self.sheet_fuel,
                             skiprows=9, header=list(range(6)))
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
        
        # N and S coker heaters went down at these respective dates,
        # after which no fuel flow or emissions
        fuel.loc['2019-05-06 21:00:00': ,'coker_1'] = 0
        fuel.loc['2019-04-26 06:00:00': ,'coker_2'] = 0

        fuel = fuel.clip(lower=0)
        return fuel
    
    def _parse_all_monthly_CEMS(self):
        """Combine monthly CEMS data into annual, fill missing values, return pd.DataFrame."""
        """
        df structure: (WED Pt. x Timestamp)
        """
        CEMS_paths = self._subset_CEMS_filepaths()
        monthly_CEMS = []
        for fpath in CEMS_paths:
            month = self._parse_monthly_CEMS(fpath)
            monthly_CEMS.append(month)
        annual_CEMS = (pd.concat(monthly_CEMS)
                         .sort_values(['ptag', 'tstamp'])
                         .reset_index(drop=True))
        filled_CEMS = self._fill_missing_with_average(annual_CEMS, cf.MAX_CEMS_TO_FILL)
        filled_CEMS.set_index('tstamp', inplace=True)
        filled_CEMS['val'] = filled_CEMS['val'].clip(lower=0)
        return filled_CEMS
    
    def _subset_CEMS_filepaths(self):
        """Return list of filepaths to parse based on months specified in config file."""
        CEMS_paths_all = sorted(glob.glob(cf.CEMS_dir+'*'))
        months_to_parse = [month for month in cf.months_to_calculate]
        CEMS_paths_parse = [path for path in CEMS_paths_all
                          if int(path.split(cf.CEMS_dir)[-1][:2])
                          in months_to_parse]
        return CEMS_paths_parse
    
    @staticmethod
    def _parse_monthly_CEMS(path):
        """Read one month of hourly CEMS data, return pd.DataFrame."""
        if cf.data_year == 2018:
            hdr = 0
        else:
            hdr = None
        
        cems_df = pd.read_csv(path, usecols=[1,2,3,5], header=hdr)
        cems_df.columns = ['ptag', 'tstamp', 'val', 'text_flag']
        cems_df['tstamp'] = pd.to_datetime(cems_df['tstamp'])
        # remove duplicate rows from daylight savings for 11/3/2019
        cems_df = cems_df[cems_df['tstamp'].dt.minute != 2]
        print('    parsed CEMS data in: '+path[:40]+'*.csv')
        return cems_df
    
    @staticmethod
    def _fill_missing_with_average(cems_df, max_consec_to_fill=cf.MAX_CEMS_TO_FILL):
        """Fill missing hours with average of surrounding values; log filled hours."""
        """
        NOTE: Because the data is sorted by both PI tag and timestamp, it is not necessary
        to separate by PI tag. Values from one PI tag will not be used to calculate averages
        for another, because missing values at the boundary between PI tags (first and last
        hour of the year) can only be filled manually.
        """
        flags_for_avg = ['PM', 'Calibration', 'Malfunction', 'CGA', 'Out of Control']
        # list indices for all missing values
        ixs_nan_all = list(cems_df[(cems_df['val'].isna()) & (cems_df['text_flag'].isin(flags_for_avg))].index)
        # subset out indices with missing values from first and last hour of year

        # separate consecutive missing indices into groups
        consec_groups = {}
        label = 0
        consec_groups[label] = []
        for iloc in range(len(ixs_nan_all)):
            consec_groups[label].append(ixs_nan_all[iloc])
            if iloc < len(ixs_nan_all) - 1:
                if ixs_nan_all[iloc + 1] != ixs_nan_all[iloc] + 1:
                    label += 1
                    consec_groups[label] = []
        # convert dict of lists to list of lists
        consec_groups_list = [ixs for ixs in consec_groups.values()]

        first = cems_df['tstamp'].min()
        last = cems_df['tstamp'].max()
        first_last = [first, last]
        # subset out groups containing missing values in first or last hour of year
        ixs_first_last = []
        for group in consec_groups_list:
            for ix in group:
                if ix in first_last:
                    ixs_first_last += group
                    consec_groups_list.remove(group)
                    
        # subset based on threshold for max # of consecutive missing values to fill
        ixs_GT_max_consec = []
        ixs_LE_max_consec = []
        for group in consec_groups_list:
            if len(group) > max_consec_to_fill:
                # flatten into one list for those not being filled
                ixs_GT_max_consec += group
            else:
                # append to list of lists for those being filled
                ixs_LE_max_consec.append(group)
        
        # store filling avgs for each index
        fill_vals = {}
        for group in ixs_LE_max_consec:
            group = sorted(group) # ensure correct order
            val_before = cems_df.loc[group[0] - 1,  'val']
            val_after  = cems_df.loc[group[-1] + 1, 'val']
            val_avg = (val_before + val_after) / 2
            for ix in group:
                fill_vals[ix] = val_avg

        # fill missing values for those that pass the above filters
        ixs_filled = []
        ixs_not_filled_error = []
        for ix, avg in fill_vals.items():
            if math.isnan(avg):
                ixs_not_filled_error.append(ix)
            else:
                cems_df.loc[ix, 'val'] = avg
                ixs_filled.append(ix)
        
        ALL_not_filled = ixs_first_last + ixs_GT_max_consec + ixs_not_filled_error
        """
        ixs_nan_all # all missing values
        ixs_filled # values filled programatically by averaging surrounding two non-NULL values
        ixs_first_last # missing first and last hours of year; must fill manually
        ixs_GT_max_consec # count of consecutive missing rows above threshold; must fill manually
        ixs_not_filled_error # catchall for anything else that could not be filled; must fill manually
        ALL_not_filled # any values that couldn't be filled programatically
        """
        for ixs, outfile in zip(
            [ixs_filled, ixs_first_last, ixs_GT_max_consec, ixs_not_filled_error, ALL_not_filled],
            ['missing_filled', 'missing_first_last', 'missing_above_threshold',
             'missing_NOT_filled_error', 'missing_NOT_filled_ALL']
                               ):
            (cems_df.loc[ixs]
                    .sort_values(by=['ptag', 'tstamp'])
                    .to_csv(cf.log_dir+outfile+'.csv', index=False))
        if len(ixs_filled) == len(ixs_nan_all):
            print('  **Filled {} out of {} missing CEMS hours.'.format(
                  str(len(ixs_filled)), str(len(ixs_nan_all)), cf.log_dir))
        else:
            print('  **Filled {} out of {} missing CEMS hours. \
                  Check {} for missing hours to fill manually.'.format(
                  str(len(ixs_filled)), str(len(ixs_nan_all)), cf.log_dir))
        cems_df.drop(columns='text_flag', inplace=True)
        return cems_df
    
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
#++EQUIPMENT-MAPPING METHODS CALLED BY self.__init__()+++++++++++++++++++++++++#
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
    def _generate_unitkey_unitname_dict(self):
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
    
    def _generate_ordered_equip_list(self):
        """Generate list of equipment ordered by WED Pt. for CSV output."""
        dkeys = list(self.unitID_equip.keys())
        equip_list = []
        for k in dkeys:
            equip_list.append(self.unitID_equip[k])
        return equip_list
    
    def _generate_unitID_unitkey_dict(self):
        """Return OrderedDict of equipment IDs ({unit_id: unit_key})"""
        """
        ex: OrderedDict([('40_E', 'coker_e'),
                         ('41_W', 'coker_w')])
        """
        unitID_unitkey_dict = (self.equip.set_index('unit_id')['unit_key']
                                            .to_dict(into=OrderedDict))
        return unitID_unitkey_dict
    
    def _generate_ptag_pol_dict(self):
        """Return dictionary of ({PItag : pol_units})."""
        
        # make dictionary of PI tags
        ptag_pols_dict = (self.equip
                          .dropna(axis=0, subset=['ptag'])
                          .set_index('ptag')['param_units']
                          .to_dict())
        return ptag_pols_dict
    
    def _generate_equip_ptag_dict(self):
        """Return dictionary of equipment ({unit_key: [PItags_list]})"""
        
        equip_df = self._parse_equip_map()
        # subset to exclude H2S and others
        sub = (equip_df[equip_df['param'].isin(['nox', 'so2', 'co', 'o2',
                                                'so2_lo', 'so2_hi', 'no', 'no2'
                                                #'co2'
                                                ])]
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
    
    def _parse_equip_map(self):
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
        equip_info.replace({'dry o2': 'o2',
                            'lo so2': 'so2_lo',
                            'hi so2': 'so2_hi'
                           }, inplace=True)
        
                # equip_info.loc[equip_info['ptag'] == '46AI601.PV', 'param'] = 'o2' # change 'dry o2' to 'o2' for H2 Plant
        equip_info['param_units'] = equip_info['param']+'_'+equip_info['units']
                # .set_index(['unit_key', 'ptag'])
                # .set_index('PI Tag')
                # .rename_axis('ptag', axis=0))
        return equip_info
    
    def _generate_date_range(self):
        """Generate pd.date_range to fill timestamps."""
        tsi = self._generate_ts_interval_list()
        s_tstamp = tsi[0][0]
        e_tstamp = tsi[len(tsi) - 1][1]
        dr = pd.date_range(start=s_tstamp, end=e_tstamp, freq='H')
        return dr
    
    def _generate_ts_interval_list(self):
        """Return list of monthly datetime tuples (start, end)."""
        intervals = []
        for mo in self.months_to_calc:
            ts_start = pd.to_datetime(str(self.year)+'-'+str(mo)+'-01', format='%Y-%m-%d') # Timestamp('2018-01-01 00:00:00')
            ts_end   = ts_start + pd.DateOffset(months=1) - pd.DateOffset(hours=1) # Timestamp('2018-01-31 23:00:00')
            interval = (ts_start, ts_end) # interval = pd.date_range(start=ts_start, end=ts_end, freq='H')
            intervals.append(interval)
        return intervals
    
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
#++EMISSIONS-CALCULATION METHODS CALLED/SHARED BY MONTHLY CHILD CLASSES++++++++#
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

    def calculate_monthly_equip_emissions(self):
        """Return pd.Series of equipment unit emissions for specified month."""
        monthly = self.aggregate_hourly_to_monthly()
        # calculate all pollutants except for H2SO4
        if 'calciner' in self.unit_key:
            coke_tons = self.get_monthly_coke()['coke_tons'].sum()
            stack_dscf = monthly.loc['dscfh']

        for pol in ['nox', 'co', 'so2', 'voc', 'pm', 'pm25', 'pm10']:
            # if no CEMS
            if pol not in monthly.index:
                # need this logic to avoid errors while PM25 & PM 10 EFs are added
                if pol not in self.EFunits.loc[self.unit_key].index:
                    monthly.loc[pol] = -9999 * 2000 / 12 # error flag that will show up as -9999
                else:
                    if (self.EFunits.loc[self.unit_key]
                                    .loc[pol]
                                    .loc['units'] == 'lb/hr'):
                        # don't multiply by fuel quantity if EF is in lb/hr
                        EF_multiplier = 1
                    else:
                        if self.unit_key == 'h2_plant_2':
                            fuel_type = 'flow'
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
        if self.unit_key in ['calciner_1', 'calciner_2']:
            monthly.loc['fuel_ng'] = 0
# end temporary workaround
        
        return monthly

    def calculate_monthly_nvac_emissions_2019(self):
        """Return pd.Series of nvac emissions for specified month."""
        if self.unit_key != 'n_vac':
            return('ERROR')
        # all complete months with or without NOx CEMS -- normal control path
        if self.month != 5:
            return (self.calculate_monthly_equip_emissions())
        # NOx CEMS implemented mid-way through month
        elif self.year == 2019 and self.month == 5:
            hourly = self.merge_fuel_and_CEMS() 
            # pre-CEMS implementation
            pre  = hourly.copy().loc[:'2019-05-12 14:00:00' ]
            # post-CEMS implementation
            post = hourly.copy().loc[ '2019-05-12 15:00:00':]
            post = self.convert_from_ppm(post)

            subsets = []
            for subset in [pre, post]:
                monthly = subset.sum()
                # calculate all pollutants except for H2SO4
                for pol in ['nox', 'co', 'so2', 'voc', 'pm', 'pm25', 'pm10']:
                    # if no CEMS
                    if pol not in monthly.index:
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

                        monthly.loc[pol] = (EF_multiplier
                                            * self.equip_EF[self.unit_key][pol]
                                            * self.get_conversion_multiplier(pol))

                # now calculate H2SO4 separately
                monthly.loc['h2so4'] = monthly.loc['so2'] * 0.026

                # set other values in series
                monthly.loc['equipment'] = self.unit_key
                monthly.loc['month'] = self.month
                monthly = monthly.reindex(self.col_name_order)
                monthly.loc[self.col_name_order[4:-1]] = monthly.loc[
                                        self.col_name_order[4:-1]] / 2000 # lbs --> tons
                subsets.append(monthly)
            # sum values from before and after NOx CEMS is up and running
            whole = pd.concat(subsets, axis=1).sum(axis=1)
            whole['equipment'] = 'n_vac'
            whole['month'] = int(whole['month'] / 2)
            return whole        
        return monthly

    def aggregate_hourly_to_monthly(self):
        """"""
        # workaround for n_vac NOx CEMS transition
        if self.unit_key == 'n_vac' and self.annual_equip.year <= 2019 and self.month < 5:
            return pd.concat([self.get_monthly_fuel(),
                             pd.DataFrame({'no_CEMS_data' : []})],
                             axis=1).sum()
        if self.unit_key in self.equip_ptags.keys():
            return self.convert_from_ppm(self.merge_fuel_and_CEMS()).sum()
        return self.merge_fuel_and_CEMS().sum()
    
    def convert_from_ppm(self, both_df):
        """Convert CEMS from ppm, return pd.DataFrame of hourly flow values."""
        if self.unit_key == 'h2_plant_2':
            stack = self.get_monthly_PSAstack().copy()
            stack['PSA_flow'] = (stack['46FC36.PV']     # Mscf
                                 * 1000                 # scf/Mscf
                                 * self.HHV_PSA         # Btu/scf
                                 / 1000000              # Btu/MMBtu
                                 * self.f_factor_PSA)   # scf/MMBtu
            stack['NG_flow']  = ((stack['46FI187.PV'] + stack['46FS38.PV'])
                                 * 1000
                                 * self.HHV_NG
                                 / 1000000
                                 * self.f_factor_NG)
            stack['PSA_mscf'] = stack['46FC36.PV']
            stack['NG_mscf'] = stack['46FI187.PV'] + stack['46FS38.PV']
            both_df = pd.concat([both_df, stack], axis=1)
            
            both_df['PSA_dscfh'] = (both_df['PSA_flow'] * 20.9 / (20.9 - both_df['o2_%']))
            both_df['NG_dscfh']  = (both_df['NG_flow']  * 20.9 / (20.9 - both_df['o2_%']))
            both_df['dscfh'] = both_df['PSA_dscfh'] + both_df['NG_dscfh']
        else:
            both_df['dscfh'] = (both_df['fuel_rfg']
                                * 1000 
                                * self.HHV_RFG
                                / 1000000
                                * self.f_factor_RFG
                                * 20.9 / (20.9 - both_df['o2_%']))
# would like to break this into another method so that 
# calculate_calciner_total_stack_flow() does not need a df passed to it as an arg
# right now it is difficult to test, and this method is too nested
        if 'calciner' in self.unit_key:
            both_df = self.calculate_calciner_total_stack_flow(both_df)
        
        ppm_conv_facts = {
                         #       MW      const   hr/min
                         'nox': (46.1 * 1.557E-7 / 60), # 1.196e-07
                         'co' : (28   * 1.557E-7 / 60), # 7.277e-08
                         'so2': (64   * 1.557E-7 / 60)  # 1.661e-07
                          }
        
# if there are CEMS pols to convert
        ptags_list = self.equip_ptags[self.unit_key]
        cems_pol_list = [self.ptags_pols[tag]
                         for tag in ptags_list
                         if self.ptags_pols[tag] != 'o2_%']
        for pol in cems_pol_list:
            both_df[pol.split('_')[0]] = (
                                    both_df[pol]
                                    * ppm_conv_facts[pol.split('_')[0]]
                                        # ==> lb/scf
                                    * both_df['dscfh'])
                                        # ==> lb
        return both_df
    
    def merge_fuel_and_CEMS(self):
        """Merge fuel and CEMS data, return pd.DataFrame."""
        return pd.concat([self.get_monthly_fuel(),
                          self.get_monthly_CEMS()],
                          axis=1)
    
    def get_monthly_PSAstack(self):
        """Return pd.DataFrame of emis unit stack flow for specified month."""
        month_stack = (self.PSAstack_annual.loc[
                            self.ts_interval[0]:
                            self.ts_interval[1]])
        return month_stack
    
    def calculate_calciner_total_stack_flow(self, df1):
        """Return pd.DataFrame of calciner emissions."""
        """
        'Equation 2' from MAR used for calciner_2:
        stack flow = 1823.2 * calcinced coke + 28162
          (ACFM)                  (STPH)      (ACFM)
        """
        monthly_coke = self.get_monthly_coke()
        df2 = pd.concat([df1, monthly_coke], axis=1)
        
        if self.unit_key == 'calciner_1':
# TODO: change to dynamic, not hardcoded...they change yearly w/ EFs
            if self.year < 2019:
                    stack_flow = 98055 # dscf / ton coke
            elif self.year >= 2019:
                if self.month < 9: 
                    stack_flow = 98055 # dscf / ton coke
                if self.month >= 9:
                    stack_flow = 117156 # dscf / ton coke    
            df2['WESP_flow'] = df2['coke_tons'] * stack_flow
        
        if self.unit_key == 'calciner_2':
            stack_flow = 1823.2 # acfm
            prod_rate  = 28162  # unitless?
            dscf_acf   = 0.669  # dscf per acf conversion factor
            
            # must be multiplied by 60 to convert from /min to /hr
            df2['WESP_flow'] = ((df2['coke_tons'] * stack_flow + prod_rate)
                                 * dscf_acf * 60)

        df2['dscfh'] = df2['WESP_flow'] + df2['dscfh']
        return df2    
    
    def get_conversion_multiplier(self, pol):
        """Pass pollutant name, return float multiplier to convert emissions to lbs."""
        if (self.EFunits.loc[self.unit_key]
                        .loc[pol].loc['units'] == 'lb/mmbtu'):
            if self.unit_key in ['coker_1', 'coker_2', 'coker_e', 'coker_w']:
                multiplier = 1/1000 * self.HHV_cokerFG
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
    
    def get_monthly_coke(self):
        """Return pd.DataFrame of emis unit coke usage for specified month."""
        month_coke = (self.coke_annual.loc[
                            self.ts_interval[0]:
                            self.ts_interval[1],
                                self.unit_key])
        month_coke = pd.DataFrame(month_coke)
        month_coke.columns = ['coke_tons']
        return month_coke
    
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

#### BEGIN: methods for calculating toxics
    
    def calculate_monthly_toxics(self):
        """Calculate series of monthly toxics for given emissions unit."""
        fuel = self.get_fuel_type_for_toxics()
        base_ser = self.get_series_for_toxics()
        mult_fuel = base_ser.loc[fuel]
        tox = self.toxicsEFs.copy()
        tox.set_index('pollutant', inplace=True)
        if self.unit_key in ['calciner_1', 'calciner_2']:
            tox['lbs'] = tox['ef'] * mult_fuel
        else:
            mult_HHV = self.get_HHV_multiplier_for_toxics()
            tox.loc[tox['units']=='lb/mmscf', 'lbs'] = tox['ef'] * mult_fuel
            tox.loc[tox['units']=='lb/mmbtu', 'lbs'] = tox['ef'] * mult_fuel * mult_HHV
            tox['lbs'] = tox['lbs'] / 1000 # unit conversion b/c fuel in mscf
        tox_ordered = tox['lbs'].reindex(self.get_reindexer_for_toxics())
        tox_ser = pd.concat([base_ser, tox_ordered]).replace(np.nan, 0)
        return tox_ser
    
    def get_series_for_toxics(self):
        """return Series with sum of monthly natural gas usage."""
        fuel_type = self.get_fuel_type_for_toxics()
        base_ser = pd.Series({'equipment': self.unit_key,
                              'month'    : self.month})
        if self.unit_key in ['calciner_1', 'calciner_2']:
            base_ser.loc[fuel_type] = self.get_monthly_coke()[fuel_type].sum()
        else: 
            base_ser.loc[fuel_type] = self.monthly_emis.loc[fuel_type]        
        return base_ser    
    
    def get_fuel_type_for_toxics(self):
        """Return string indicating fuel type to use for calculating toxics."""
        if self.unit_key in ['calciner_1', 'calciner_2']:
            return 'coke_tons'
        # elif self.unit_key == 'h2_flare':
            # return 'fuel_ng'
        else:
            return 'fuel_rfg'
    
    def get_HHV_multiplier_for_toxics(self):
        """Return HHV multiplier to convert mscf to mmBtu."""
        if self.unit_key in ['coker_1', 'coker_2', 'coker_e', 'coker_w']:
            return self.HHV_cokerFG
        elif self.unit_key == 'crude_vtg':
            return self.HHV_CVTG
        # elif self.unit_key == 'h2_flare':  ## not currently calculating toxics for flare
            # return self.HHV_flare
        else:
            return self.HHV_RFG
    
    def get_reindexer_for_toxics(self):
        """Return correct toxics order for WEIRS."""
        if self.unit_key in ['calciner_1', 'calciner_2']:
            return cf.calciner_toxics_with_EFs
        else:
            return cf.toxics_with_EFs
    
#### END: methods for calculating toxics

#### BEGIN: methods for calculating H2S

    def read_and_write_all_h2s(self):
        """calculate and write h2s emissions"""
        
        d = self.aggregate_to_annual_h2s(cf.h2s_equips_to_calc,
                                         cf.months_to_calculate)
        a = self.format_annual_h2s(d, month_names=cf.month_names)
        self.groupby_and_write_annual_h2s(a, cf.year_to_calculate)

    @staticmethod
    def aggregate_to_annual_h2s(equip_list, months_to_parse):
        """generate annual H2S emissions DataFrame
        from list of equipment keys."""

        # list equipment by ascending WED Pt for calculations and output
        ordered_equips_to_calculate = []
        for e in self.ordered_equip:
            if e in equip_list:
                ordered_equips_to_calculate.append(e)
        
        # calculate H2S emissions for every month x equipment combo specified
        print('Calculating H2S emissions for equipment and months specified...')
        
        each_equip_month_tuple_list = []
        for equip in ordered_equips_to_calculate:
            for month in months_to_parse:
                if cf.verbose_calc_status:
                    print('\tCalculating month {:2d} emissions for {}...'
                            .format(month, equip))
                inst = Equipment(equip, month) # instantiate indiv equip
                m = inst.calc_monthly_h2s_emissions()
                each_equip_month_tuple_list.append(m)
        
        return pd.DataFrame(each_equip_month_tuple_list,
                            columns=['month', 'equipment',
                                     'fuel_rfg', 'h2s', 'h2s_cems'])
    
    @staticmethod
    def format_annual_h2s(annual_df):
        """Format data labels of annual emissions DataFrame
        ([equipment, month] x pollutants) for CSV output."""
        print('Formatting emissions data.')
        # change month integers to abbreviated names if desired
        if cf.write_month_names:
            annual_df['month'] = annual_df['month'].astype(str)
            annual_df.replace({'month': cf.month_map}, inplace=True)
        # pretty up the names of equipment and column headers for output
        annual_df['WED Pt'] = annual_df['equipment'].replace(
                                            dict((v,k)
                                            for k,v
                                            in Equipment.unitID_equip.items()))
        output_colnames = {
            'month'    : 'Month',
            'equipment': 'Equipment',
            'fuel_rfg' : 'Fuel Usage',
           #'fuel_ng'  : 'Natural Gas',
           #'nox'      : 'NOx',
           #'co'       : 'CO',
           #'so2'      : 'SO2',
           #'voc'      : 'VOC', 
           #'pm'       : 'PM',
           #'h2so4'    : 'H2SO4'
            'h2s'      : 'H2S',
            'h2s_cems' : 'H2S_CEMS'
            }
        annual_df = (annual_df.replace({'equipment': self.unitkey_name})
                              .rename(columns=output_colnames))
        col_order = ['WED Pt', 'Equipment', 'Month',
                     'Fuel Usage', #'Natural Gas',
                     #'NOx', 'CO', 'SO2', 'VOC', 'PM', 'H2SO4'
                    'H2S', 'H2S_CEMS']
        return annual_df[col_order]

    @staticmethod
    def groupby_and_write_annual_h2s(annual_df, year_to_parse,
                                    write_csvs=True, return_dfs=False):
        """Generate summary tables and write CSV output."""

        print('Slicing and dicing emissions data for output.')

        # create MultiIndex for renaming columns
        arr_col = [['Fuel Usage', 'H2S'],
                   ['mscf', 'lbs']]
        MI_col = pd.MultiIndex.from_arrays(arr_col,
                                           names=('Parameter', 'Units'))
            # df.index.names = (['Quarter', 'Equipment']) ; q_gb.head()
            # not sure if the tuple is necessary, can just pass a list
        
        # Groupby [equipment, month] --> [equipment, month] x pollutants
        eXm_gb = annual_df.groupby(['WED Pt', 'Equipment', 'Month'],
                                   sort=False).sum()
        eXm_gb.columns = MI_col
        eXm_gb.name = 'by_Equip_x_Month'

        # Groupby equipment --> equipment x pollutants
        e_gb = (annual_df.groupby(['WED Pt', 'Equipment'], sort=False)
                         .sum()[list(annual_df.columns[3:5])])
        e_gb.columns = MI_col
        e_gb.name = 'by_Equip'
        
        frames = [e_gb, eXm_gb]
        
        if write_csvs:
            print('Writing output to files.')
        
            for df in frames:        
                df.round(2).to_csv(cf.outpath_prefix
                                     +str(year_to_parse)+'_'
                                     +df.name+'_H2S'+'.csv')
                
            print('For '+str(cf.year_to_calculate)
                    +' H2S emissions data, see files in '+cf.outpath_prefix+'.')
    
    def calc_monthly_h2s_emissions(self):
        """Calculate monthly H2S (lbs)."""
        return (
                self.month,
                self.unit_key, 
                self.get_monthly_fuel()['fuel_rfg'].sum(),
                self.convert_from_ppm_h2s(),
                self.return_h2s_cems_source()
               )
    
    def convert_from_ppm_h2s(self):
        """Convert H2S from ppm to lbs."""
        h2s_ppm   = self.get_monthly_h2s()
        fuel_mscf = self.get_monthly_fuel()['fuel_rfg'].sum()
        if self.unit_key == 'h2_plant_2':
            fuel_mscf = self.monthly_emis.loc['fuel_rfg']
        h2s_lbs   = (
                       h2s_ppm       # monthly H2S  
                       * fuel_mscf   # mscf
                       * 34.1        # lb/lb-mol
                       / (379 * 1000)  # (scf/lb-mol)
                       * (1 - 0.99)    # (99% control efficiency)
                    )
        return h2s_lbs
    
    def get_monthly_h2s(self):
        """Return float of average monthly H2S CEMS data (ppm)."""
        all_cems = self.annual_equip.CEMS_annual
        h2s_data = self.return_h2s_cems_source()
        if h2s_data == 'coker_h2s':
            h2s_ppm = all_cems[all_cems['ptag'] == '12AI3751.PV']
        elif h2s_data == 'cokerOLD_h2s':
            h2s_ppm = all_cems[all_cems['ptag'] == '12AI55A.PV']
        elif h2s_data == 'CVTG_h2s':
            h2s_ppm = all_cems[all_cems['ptag'] == '10AI136A.PV']
        elif h2s_data == 'RFG_h2s':
            h2s_ppm = all_cems[all_cems['ptag'] == '30AI568A.PV']
        h2s_monthly = h2s_ppm.loc[self.ts_interval[0]:self.ts_interval[1]]
        return h2s_monthly['val'].mean()
    
    def return_h2s_cems_source(self):
        """Return string indicating which H2S cems data to use."""
        if self.unit_key in self.annual_equip.h2s_cems_map['uses_coker_h2s']:
            h2s_data_source = 'coker_h2s'
        elif self.unit_key in self.annual_equip.h2s_cems_map['uses_cokerOLD_h2s']:
            h2s_data_source = 'cokerOLD_h2s'
        elif self.unit_key in self.annual_equip.h2s_cems_map['uses_CVTG_h2s']:
            h2s_data_source = 'CVTG_h2s'
        else:
            h2s_data_source = 'RFG_h2s'
        return h2s_data_source

#### END: methods for calculating H2S
       
class AnnualHB(AnnualEquipment):
    """Parse and store annual heater/boiler data."""
    def __init__(self, annual_equip):
        """Constructor for parsing annual heater/boiler data."""
        self.annual_equip = annual_equip

class MonthlyHB(AnnualHB):
    """Calculate monthly heater/boiler emissions."""
    def __init__(self,
             unit_key,
             month,
             annual_eu):
        """Constructor for individual emission unit calculations."""
        self.unit_key       = unit_key
        self.month          = month
        self.annual_eu      = annual_eu # aka Annual{equiptype}()
        self.annual_equip   = annual_eu.annual_equip # aka AnnualEquipment()
        self.year           = self.annual_equip.year        
        self.ts_interval    = self.annual_equip.ts_intervals[self.month
                                                - self.annual_equip.month_offset]
        self.EFs            = self.annual_equip.EFs[self.month][0]
        self.EFunits        = self.annual_equip.EFs[self.month][1]
        self.equip_EF       = self.annual_equip.EFs[self.month][2]
        self.col_name_order = self.annual_equip.col_name_order
        self.equip_ptags    = self.annual_equip.equip_ptags
        self.ptags_pols     = self.annual_equip.ptags_pols
        self.toxicsEFs      = self.annual_equip.toxicsEFs
        self.year           = self.annual_equip.year
        
        # fuel-sample lab results (pd.DataFrame)
        self.RFG_monthly    = ff.get_monthly_lab_results(self.annual_equip.RFG_annual, self.ts_interval)
        self.CVTG_monthly   = ff.get_monthly_lab_results(self.annual_equip.CVTG_annual, self.ts_interval)
        # fuel higher heating values (float)
        self.HHV_RFG        = ff.calculate_monthly_HHV(self.RFG_monthly)
        self.HHV_CVTG       = ff.calculate_monthly_HHV(self.CVTG_monthly)
        # fuel f-factors (floats) calculated using static chem data
        self.f_factor_RFG   = ff.calculate_monthly_f_factor(
                                            self.RFG_monthly,
                                            self.annual_equip.fpath_FG_chem,
                                            self.ts_interval)
        self.f_factor_CVTG  = ff.calculate_monthly_f_factor(
                                            self.CVTG_monthly,
                                            self.annual_equip.fpath_FG_chem,
                                            self.ts_interval)
        
        if self.unit_key == 'n_vac' and self.annual_equip.year == 2019:
            self.monthly_emis = self.calculate_monthly_nvac_emissions_2019()
        else:
            self.monthly_emis   = self.calculate_monthly_equip_emissions()
        self.monthly_toxics   = self.calculate_monthly_toxics()
        self.monthly_emis_h2s = self.calc_monthly_h2s_emissions()

class AnnualCoker(AnnualEquipment):
    """Parse and store annual coker data for new (2019+) cokers."""
    def __init__(self, annual_equip):
        """Constructor for parsing annual coker data."""
        self.annual_equip = annual_equip
        
        self.coker_dat_tup = self.get_annual_dat_newcoker()
    
    def get_annual_dat_newcoker(self):
        coker_dat_tup = AnnualCoker_CO2(self.annual_equip
                                                    ).unmerge_annual_ewcoker()
        return coker_dat_tup

class MonthlyCoker(AnnualCoker):
    """Calculate monthly coker emissions for new (2019+) cokers."""
    def __init__(self,
             unit_key,
             month,
             annual_eu):
        """Constructor for individual emis unit calculations."""
        self.month          = month
        self.unit_key       = unit_key
        self.annual_eu      = annual_eu
        self.annual_equip   = annual_eu.annual_equip
        self.year           = self.annual_equip.year
        self.ts_interval    = self.annual_equip.ts_intervals[self.month
                                                - self.annual_equip.month_offset]
        self.EFs            = self.annual_equip.EFs[self.month][0]
        self.EFunits        = self.annual_equip.EFs[self.month][1]
        self.equip_EF       = self.annual_equip.EFs[self.month][2]
        self.col_name_order = self.annual_equip.col_name_order
        self.equip_ptags    = self.annual_equip.equip_ptags
        self.ptags_pols     = self.annual_equip.ptags_pols
        self.toxicsEFs      = self.annual_equip.toxicsEFs

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
        if self.unit_key == 'coker_e':
            self.coker_dat = self.annual_eu.coker_dat_tup[0]
        if self.unit_key == 'coker_w':
            self.coker_dat = self.annual_eu.coker_dat_tup[1]
            
        self.monthly_emis     = self.calculate_monthly_equip_emissions_newcoker()
        self.monthly_toxics   = self.calculate_monthly_toxics()
        self.monthly_emis_h2s = self.calc_monthly_h2s_emissions()
    
    def calculate_monthly_equip_emissions_newcoker(self):
        """Return pd.Series of equipment unit emissions for specified month."""
        hourly = self.convert_from_ppm_newcoker()
        monthly = hourly.sum()

        monthly.rename(index={'cokerfg_mscfh': 'fuel_rfg',
                              'pilot_mscfh'  : 'fuel_ng'},
                              inplace=True)
        if (self.annual_equip.year == 2019 
            and (
                 self.month >= 4 and self.unit_key == 'coker_e'
                 ) or (
                 self.month >= 5 and self.unit_key == 'coker_w'
                 )
            ):
                for pol in ['nox', 'co', 'so2', 'voc', 'pm', 'pm25', 'pm10']:
                    # if no CEMS
                    if pol not in monthly.index:
                        # need this logic to avoid errors while PM25 & PM 10 EFs are added
                        if pol not in self.EFunits.loc[self.unit_key].index:
                            monthly.loc[pol] = -9999 * 2000 / 12 # error flag that will show up as -9999
                        else:
                            if (self.EFunits.loc[self.unit_key]
                                            .loc[pol]
                                            .loc['units'] == 'lb/hr'):
                                # don't multiply by fuel quantity if EF is in lb/hr
                                EF_multiplier = 1
                            else:
                                fuel_type = 'fuel_ng'
                                EF_multiplier = monthly.loc[fuel_type]

                                monthly.loc[pol] = (EF_multiplier
                                                * self.equip_EF[self.unit_key][pol]
                                                * self.get_conversion_multiplier(pol))
# TODO: refactor this hacky temp workaround...
                # add in VOC from nat gas fuel flow
                # use emission factors from 'h2_plant_2'
                efs = self.EFs
                ng_voc_ef    = efs[(efs['unit_id'] == '46') &
                                   (efs['pollutant'] == 'voc')
                                  ].loc[:,'ef'].iloc[0]
                ng_voc_units = efs[(efs['unit_id'] == '46') &
                                   (efs['pollutant'] == 'voc')
                                  ].loc[:,'units'].iloc[0]
                if ng_voc_units == 'lb/mmscf':
                    monthly['voc'] = monthly['voc'] + (monthly['fuel_ng']
                                                        * ng_voc_ef / 1000)
                else:
                    print('***WARNING: VOC emission factor not in lb/mmscf***')
        else:
            monthly.loc['pm']    = 0
            monthly.loc['pm25']  = 0
            monthly.loc['pm10']  = 0
            monthly.loc['voc']   = 0
        monthly.loc['h2so4'] = monthly.loc['so2'] * 0.026
        monthly.loc['equipment'] = self.unit_key
        monthly.loc['month'] = self.month
        monthly = monthly.reindex(self.col_name_order)
        monthly.loc[self.col_name_order[4:-1]] = monthly.loc[
                                self.col_name_order[4:-1]] / 2000 # lbs --> tons
        return monthly
    
    def convert_from_ppm_newcoker(self):
        """Convert CEMS from ppm, return pd.DataFrame of hourly flow values."""
        both_df = self.merge_fuel_and_CEMS_newcoker()
        if both_df.shape[0] == 0:
            return both_df
        else:       
            ppm_conv_facts = {
                                #       MW      const   hr/min
                                'nox': (46.1 * 1.557E-7 / 60), # 1.196e-07
                                'co' : (28   * 1.557E-7 / 60), # 7.277e-08
                                'so2': (64   * 1.557E-7 / 60)  # 1.661e-07
                             }
            ptags_list = self.equip_ptags[self.unit_key]
            cems_pol_list = ['nox_ppm', 'co_ppm', 'so2_ppm']
                    # cems_pol_list = [self.ptags_pols[tag] for tag in ptags_list
                                     # if self.ptags_pols[tag]
                                         # not in ['o2_%', 'so2_lo_ppm', 'so2_hi_ppm']]
                    # cems_pol_list += ['so2_ppm']
            for pol in cems_pol_list:
                both_df[pol.split('_')[0]] = (
                                        both_df[pol]
                                        * ppm_conv_facts[pol.split('_')[0]]
                                        * both_df['dscfh'])
            return both_df
    
#TODO: move these to annual level, but will need to deal with
#      the issue of not having yearly data for both datasets
    def merge_fuel_and_CEMS_newcoker(self):
        """Merge fuel and CEMS data, return pd.DataFrame."""
        monthly_CEMS = self.get_monthly_CEMS_newcoker()
        monthly_fuel = self.get_monthly_fuel_newcoker()

        if monthly_CEMS.sum().sum() == 0:
            # make a dummy DataFrame with same index for merging
            monthly_CEMS = pd.DataFrame(index=monthly_fuel.index)
            for col in ['o2_%', 'nox_ppm', 'co_ppm', 'so2_ppm']:
                monthly_CEMS[col] = pd.np.nan
        monthly_fuel.drop(columns='o2_%', inplace=True) # so that column not duplicated when merged
        merged = pd.merge(monthly_CEMS, monthly_fuel, left_index=True, right_index=True)
        
        merged['cokerfg_dscfh'] = (merged['cokerfg_mscfh']
                            * 1000 
                            * self.HHV_cokerFG
                            * 1/1000000
                            * self.f_factor_cokerFG
                            * 20.9 / (20.9 - merged['o2_%']))

        merged['pilot_dscfh'] = (merged['pilot_mscfh']
                            * 1000 
                            * self.HHV_NG
                            * 1/1000000
                            * self.f_factor_NG
                            * 20.9 / (20.9 - merged['o2_%']))

        merged['dscfh'] = merged['cokerfg_dscfh'] + merged['pilot_dscfh']
        return merged
    
    def get_monthly_fuel_newcoker(self):
        annual_dat = self.coker_dat
        monthly_dat = annual_dat.loc[
                                self.ts_interval[0]:
                                self.ts_interval[1]]
        return monthly_dat.copy()
    
    def get_monthly_CEMS_newcoker(self):
        """Return pd.DataFrame of emis unit CEMS data for specified month."""
        CEMS_annual = self.annual_equip.CEMS_annual.copy()
        if True:#self.unit_key in self.equip_ptags.keys():
            ptags_list = self.equip_ptags['coker_e']#self.unit_key]
            # subset PItags of interest
            sub = CEMS_annual[CEMS_annual['ptag'].isin(ptags_list)]
            sub.reset_index(inplace=True)
            sub_pivot = (sub.pivot(index='tstamp',
                                   columns="ptag",
                                   values="val")
                            .rename(columns=self.ptags_pols))
            sub_pivot['nox_ppm'] = sub_pivot['no_ppm'] + sub_pivot['no2_ppm']
            sub_pivot.drop(columns=['no_ppm', 'no2_ppm'], inplace=True)                            
            
            # replace 'lo so2' value with 'hi so2' value if 'hi so2' not null
            mask = sub_pivot['so2_lo_ppm'].isna() & sub_pivot['so2_hi_ppm'].notna()
            sub_pivot['so2_ppm'] = sub_pivot['so2_lo_ppm']
            sub_pivot.loc[mask, 'so2_ppm'] = sub_pivot.loc[mask, 'so2_hi_ppm']
            sub_pivot.drop(columns=['so2_lo_ppm', 'so2_hi_ppm'], inplace=True)
            sub2_pivot = sub_pivot.loc[
                            self.ts_interval[0]:
                            self.ts_interval[1]]
            return sub2_pivot
        else: # empty df if no CEMS
            no_CEMS = pd.DataFrame({'no_CEMS_data' : []})
            return no_CEMS

class AnnualCokerOLD(AnnualEquipment):
    """Parse and store annual coker data for pre-2019 cokers."""
    def __init__(self, annual_equip):
        """Constructor for parsing annual coker data."""
        self.annual_equip = annual_equip

class MonthlyCokerOLD(AnnualCoker):
    """Calculate monthly coker emissions for pre-2019 cokers."""
    def __init__(self,
             unit_key,
             month,
             annual_eu):
        """Constructor for individual emis unit calculations."""
        self.month          = month
        self.unit_key       = unit_key
        self.annual_eu      = annual_eu
        self.annual_equip   = annual_eu.annual_equip
        self.year           = self.annual_equip.year
        self.ts_interval    = self.annual_equip.ts_intervals[self.month
                                                - self.annual_equip.month_offset]
#         self.EFs            = self.annual_equip.EFs[self.month][0]
        self.EFunits        = self.annual_equip.EFs[self.month][1]
        self.equip_EF       = self.annual_equip.EFs[self.month][2]
        self.col_name_order = self.annual_equip.col_name_order
        self.equip_ptags    = self.annual_equip.equip_ptags
#         self.ptags_pols     = self.annual_equip.ptags_pols
        self.toxicsEFs      = self.annual_equip.toxicsEFs
        
        self.cokerFG_monthly = ff.get_monthly_lab_results(
                                            self.annual_equip.cokerFG_annual,
                                            self.ts_interval)
        
        self.HHV_cokerFG    = ff.calculate_monthly_HHV(self.cokerFG_monthly)
        
        self.monthly_emis     = self.calculate_monthly_equip_emissions()
        self.monthly_toxics   = self.calculate_monthly_toxics()
        self.monthly_emis_h2s = self.calc_monthly_h2s_emissions()

class AnnualCoker_CO2(AnnualEquipment):
    """Parse and store annual coker data for CO2 calculations."""
    def __init__(self, annual_equip):
        """Constructor for parsing annual coker data."""
        self.annual_equip = annual_equip
    
    def unmerge_annual_ewcoker(self):
        """Unmerge E/W coker data."""
        merged_df = self.implement_pilot_gas_logic()
        merged_df = merged_df.reindex(self.annual_equip._generate_date_range())

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
        wcoker_df = self.parse_annual_ewcoker(cols=[5,6,7,8])
        pilot_df  = self.parse_annual_ewcoker(cols=[10,11], pilot=True)
        
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

class MonthlyCoker_CO2(AnnualCoker_CO2):
    """Calculate monthly coker CO2 emissions."""
# replace 'rfg_mscfh' with 'fuel_rfg' once units are figured out
    col_name_order = ['equipment', 'month', 'stack_dscfh', 'co2']
    
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
        self.year           = self.annual_equip.year
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
        self.monthly_emis   = self.calculate_monthly_equip_emissions()
                                            
    def calculate_monthly_equip_emissions(self):
        """Write hourly combined fuel csv, return monthly emissions as pd.Series."""
        hourly = self.calculate_monthly_co2_emissions()
        # write csv of hourly combined fuel flow for QA
        outname = (
                    cf.out_dir_child
                  + str(self.annual_equip.year) + '_'
                  + str(self.month).zfill(2) + '_'
                  + 'stackflow' + '_'
                  + self.unit_key + '.csv'
                  )
        hourly['stack_dscfh'].to_csv(outname, header=True)
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

        df['stack_dscfh'] = df['cokerfg_dscfh'] + df['pilot_dscfh']
        df['co2']   = df['co2_%'] * df['stack_dscfh'] * 5.18E-7

        return df.loc[self.ts_interval[0]:self.ts_interval[1]]

class AnnualCalciner(AnnualEquipment):
    """Parse and store annual calciner data."""
    def __init__(self, annual_equip):
        """Constructor for parsing annual calciner data."""
        self.annual_equip = annual_equip
        self.fpath_coke   = annual_equip.fpath_coke

        self.coke_annual    = self.parse_annual_coke()
                             # df: hourly coke data for all calciners

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
        # change STPD --> STPH
        coke_df = coke_df/24
        coke_df = coke_df.clip(lower=0)
        return coke_df
        
class MonthlyCalciner(AnnualCalciner):
    """Calculate monthly calciner emissions."""
    def __init__(self,
             unit_key,
             month,
             annual_eu):
        """Constructor for individual emission unit calculations."""
        self.month          = month
        self.unit_key       = unit_key
        self.annual_eu      = annual_eu
        self.annual_equip   = annual_eu.annual_equip
        self.year           = self.annual_equip.year
        self.ts_interval    = self.annual_equip.ts_intervals[self.month
                                                - self.annual_equip.month_offset]        
# why is it not using self.EFs? should it be calling this?
        # self.EFs            = self.annual_equip.EFs[self.month][0]
        self.EFunits        = self.annual_equip.EFs[self.month][1]
        self.equip_EF       = self.annual_equip.EFs[self.month][2]
        self.col_name_order = self.annual_equip.col_name_order
        self.equip_ptags    = self.annual_equip.equip_ptags
        self.ptags_pols     = self.annual_equip.ptags_pols
        self.toxicsEFs      = self.annual_equip.toxicsEFs_calciners

        # fuel-sample lab results (pd.DataFrame)
        self.RFG_monthly    = ff.get_monthly_lab_results(
                                            self.annual_equip.RFG_annual,
                                            self.ts_interval)
        # fuel higher heating values (float)
        self.HHV_RFG        = ff.calculate_monthly_HHV(self.RFG_monthly)
        self.f_factor_RFG   = ff.calculate_monthly_f_factor(
                                            self.RFG_monthly,
                                            self.annual_equip.fpath_FG_chem,
                                            self.ts_interval)

        self.coke_annual    = self.annual_eu.coke_annual
        self.monthly_emis   = self.calculate_monthly_equip_emissions()
        self.monthly_toxics = self.calculate_monthly_toxics()
        self.monthly_emis_h2s = None

class AnnualFlare(AnnualEquipment):
    """Parse and store annual flare data."""
    def __init__(self, annual_equip):
        """Constructor for parsing annual flare data."""
        self.annual_equip = annual_equip

class MonthlyFlare(AnnualFlare):
    """Calculate monthly flare emissions."""
    def __init__(self,
             unit_key,
             month,
             annual_eu):
        """Constructor for individual emis unit calculations."""
        self.month          = month
        self.unit_key       = unit_key
        self.annual_eu      = annual_eu
        self.annual_equip   = annual_eu.annual_equip
        self.year           = self.annual_equip.year
        self.ts_interval    = self.annual_equip.ts_intervals[self.month
                                                - self.annual_equip.month_offset]
        self.col_name_order = self.annual_equip.col_name_order
        
        self.flareEFs       = self.annual_equip.flareEFs
        
        self.flare_monthly  = ff.get_monthly_lab_results(self.annual_equip.flare_annual,
                                                         self.ts_interval)
        self.HHV_flare      = ff.calculate_monthly_HHV(self.flare_monthly)
#        self.f_factor_flare = ff.calculate_monthly_f_factor(self.flare_monthly,
#                                                            self.annual_equip.fpath_FG_chem,
#                                                            self.ts_interval)
        self.flarefuel_annual = self.annual_equip.flarefuel_annual
        self.flareHHV_annual  = self.annual_equip.flareHHV_annual
        
        #self.monthly_emis   = self.calculate_monthly_flare_emissions()
        self.monthly_emis = self.calculate_monthly_flare_emissions_apply_hourly()
        self.monthly_emis_h2s = None
    
    def calculate_monthly_flare_emissions_apply_hourly(self):
        """Return pd.Series of flare emissions for specified month (HHV applied hourly)."""
        """
        ***HHV is applied at hourly aggregation level***
        """
        fuel = self.get_monthly_flare_fuel().copy()
        # create intermediate column
        fuel['var'] = 0
        fuel.loc[ (fuel['valve'] > 0), 'var'] = fuel['discharge_to_flare']
        # if not 'unit down' and valve > 0: scenario = 'ssm'
        fuel.loc[:, 'op_scenario'] = 'ssm'
        # if not 'unit down' and valve <= 0: scenario = 'routine'
        fuel.loc[ (fuel['var'] == 0), 'op_scenario'] = 'routine'
        # if valve == 100 or valve < -4: scenario = 'unit down'
        fuel.loc[ (fuel['valve'] < -4)
              | (fuel['valve'] == 100), 'op_scenario'] = 'unit down'
        # make sure that only the three scenarios are represented
        if len(fuel.op_scenario.unique()) > 3:
            scenarios = '  '.join(str(x)
                                  for x
                                  in list(fuel.op_scenario.unique()))
            raise ValueError('more than 3 scenarios listed: '+scenarios)
        
        HHV = self.get_monthly_flare_HHV_upsampled()
        fuel = fuel.merge(HHV, right_index=True, left_index=True)
        fuel['tstamp'] = fuel.index
        
        emis = self.calculate_flare_emissions_all_hourly(fuel)
        
        emis['equipment'] = self.unit_key
        emis['month']     = self.month
        emis['fuel_ng']   = fuel[fuel['op_scenario'] == 'routine'].loc[:,'flare_header_flow'].sum()
        emis['fuel_rfg']  = fuel[fuel['op_scenario'] == 'ssm'].loc[:,'flare_header_flow'].sum()
        emis['pm25']      = -9999*2000/12 # temporarily hardcoded, as flag
        emis['pm10']      = -9999*2000/12
        emis['h2so4']     = 0
        
        emis = emis.reindex(self.col_name_order)
        emis.loc[self.col_name_order[4:-1]] = emis.loc[
                                self.col_name_order[4:-1]] / 2000 # lbs --> tons
        return emis

    def calculate_flare_emissions_all_hourly(self,fuel_df):
        """Return list of pd.DataFrames of hourly flare emissions."""
        hourly_emis = []
        for tstamp in fuel_df.index:
            if fuel_df.loc[tstamp, 'op_scenario'] in ['ssm', 'routine']:
                hourly_emis.append(self.calculate_flare_emissions_each_hourly(fuel_df, tstamp))
        emis_sum_by_pol = pd.concat(hourly_emis).groupby(['pollutant'])['emis'].sum()
        return emis_sum_by_pol
    
    def calculate_flare_emissions_each_hourly(self, df, tstamp):
        """Return pd.DataFrame of emissions for specifed hour."""
        flare_EFs = self.flareEFs.copy()
        fuel_flow = df.loc[tstamp, 'flare_header_flow']
        HHV_hourly = df.loc[tstamp, 'HHV_flare']
        
        if df.loc[tstamp, 'op_scenario'] == 'ssm':
            flare_is_on = True
        elif df.loc[tstamp, 'op_scenario'] == 'routine':
            flare_is_on = False

        if flare_is_on:
            ef_df = flare_EFs[flare_EFs['flare_on'] == True].copy()
        elif not flare_is_on:
            ef_df = flare_EFs[flare_EFs['flare_on'] == False].copy()
       
        # set conversion multiplier to convert units not in mscf
        ef_df['conv_mult'] = 1
        ef_df.loc[ef_df['units'] == 'lb/mmbtu', 'conv_mult'] = (1/1000
                                                             * HHV_hourly)
        ef_df.loc[ef_df['units'] == 'lb/mmscf', 'conv_mult'] = 1/1000
        ef_df['emis'] = ef_df['ef'] * ef_df['conv_mult'] * fuel_flow
        return ef_df
    
    def get_monthly_flare_HHV_upsampled(self):
        """Return monthly pd.Series of HHV values upsampled to hourly."""
        annual_upsampled = self.flareHHV_annual
        monthly_upsampled = annual_upsampled.loc[
                                    self.ts_interval[0]:
                                    self.ts_interval[1]]
        return monthly_upsampled
    
    def calculate_monthly_flare_emissions(self):
        """Return pd.Series of flare emissions for specified month."""
        """
        ***HHV is applied at monthly aggregation level***
        
        LOGIC FOR CALCULATING FLARE EMISSIONS
        [possible scenarios]
            unit down --> no emissions calculated
            routine   --> use normal EFs
            SSM       --> use flare EFs
        
        if valve > 0:
            var = discharge_to_flare
        else:
            var = 0
        
        if valve == 100 or valve < -4:
            scenario = 'unit down' (no flow or emis calcs for this hour)
        else:
            if var = 0 :
                scenario == 'Routine'
            else:
                scenario == 'SSM'
        
        [calculation]
        multiply flare_header_flow by appropriate EF
        """
        fuel = self.get_monthly_flare_fuel().copy()
        
        # create intermediate column
        fuel['var'] = 0
        fuel.loc[ (fuel['valve'] > 0), 'var'] = fuel['discharge_to_flare']
        
        #     # create column to assign scenarios
        #     fuel.loc[:,'op_scenario'] = 'unassigned'
        
        # if not 'unit down' and valve > 0: scenario = 'ssm'
        fuel.loc[:, 'op_scenario'] = 'ssm'
        
        # if not 'unit down' and valve <= 0: scenario = 'routine'
        fuel.loc[ (fuel['var'] == 0), 'op_scenario'] = 'routine'
        
        # if valve == 100 or valve < -4: scenario = 'unit down'
        fuel.loc[ (fuel['valve'] < -4)
              | (fuel['valve'] == 100), 'op_scenario'] = 'unit down'
        
        # make sure that only the three scenarios are represented
        if len(fuel.op_scenario.unique()) > 3:
            scenarios = '  '.join(str(x)
                                  for x
                                  in list(fuel.op_scenario.unique()))
            raise ValueError('more than 3 scenarios listed: '+scenarios)
        
        # divide into scenarios
        flare_off_fuel = fuel[fuel['op_scenario'] == 'routine']
        flare_on_fuel  = fuel[fuel['op_scenario'] == 'ssm']
        
        # get monthly summed fuel values
        sum_flare_off_fuel = flare_off_fuel['flare_header_flow'].sum()
        sum_flare_on_fuel = flare_on_fuel['flare_header_flow'].sum()
        
        # get EF data
        efs = self.get_monthly_flare_EFs()
        
        # calculate emissions for intervals of pilot light and active flaring
        efs.loc[:,'emis'] = 0
        efs.loc[~efs['flare_on'], 'emis'] = (efs['ef'] * efs['conv_mult']
                                                       * sum_flare_off_fuel)
        efs.loc[efs['flare_on'], 'emis']  = (efs['ef'] * efs['conv_mult']
                                                       * sum_flare_on_fuel)
        
        emis = efs.groupby(['pollutant'])['emis'].sum()
        
        emis['equipment'] = self.unit_key
        emis['month']     = self.month
        emis['fuel_ng']   = sum_flare_off_fuel
        emis['fuel_rfg']  = sum_flare_on_fuel
        emis['pm25']      = -9999*2000/12 # temporarily hardcoded, as flag
        emis['pm10']      = -9999*2000/12
        emis['h2so4']     = 0
        
        emis = emis.reindex(self.col_name_order)
        emis.loc[self.col_name_order[4:-1]] = emis.loc[
                                self.col_name_order[4:-1]] / 2000 # lbs --> tons
        return emis
    
    def get_monthly_flare_EFs(self):
        """Return pd.DataFrame of flare EFs for specified month."""
        ef_df = self.flareEFs.copy()
        # set conversion multiplier to convert units not in mscf
        ef_df['conv_mult'] = 1
        ef_df.loc[ef_df['units'] == 'lb/mmbtu', 'conv_mult'] = (1/1000
                                                             * self.HHV_flare)
        ef_df.loc[ef_df['units'] == 'lb/mmscf', 'conv_mult'] = 1/1000
        return ef_df
    
    def get_monthly_flare_fuel(self):
        """Return pd.DataFrame of flare fuel for specified month."""
        fuel_df = self.flarefuel_annual.loc[
                            self.ts_interval[0]:
                            self.ts_interval[1]]
        return fuel_df

class AnnualH2Plant(AnnualEquipment):
    """Parse and store annual calciner data."""
    def __init__(self, annual_equip):
        """Constructor for parsing annual calciner data."""
        self.annual_equip = annual_equip
        
        self.fpath_h2stack = self.annual_equip.fpath_h2stack # no longer using
        self.fpath_PSAstack= self.annual_equip.fpath_PSAstack
        
        self.PSAstack_annual = self.parse_annual_PSAstack()
                              # df: hourly PSA offgas data for H2 plant
    
    def parse_annual_PSAstack(self):
        """Read annual PSA offgas data for H2 plant, return pd.DataFrame in mscfh."""
        gas = pd.read_excel(self.fpath_PSAstack, skiprows=2, header=[0,1])
        gas.columns = gas.columns.droplevel(1)
        gas.set_index('1 h', drop=True, inplace=True)
        gas.index = pd.to_datetime(gas.index)
        gas.index.name = 'tstamp'
        gas['46FC36.PV'] = pd.to_numeric(gas.loc[:,'46FC36.PV'], errors='coerce')
        gas['46FI187.PV'] = pd.to_numeric(gas.loc[:,'46FI187.PV'], errors='coerce')
        gas['46FS38.PV'] = pd.to_numeric(gas.loc[:,'46FS38.PV'], errors='coerce')
        gas = gas.clip(lower=0)
        return gas

class MonthlyH2Plant(AnnualH2Plant):
    """Calculate monthly H2 plant emissions."""
    def __init__(self,
             unit_key,
             month,
             annual_eu):
        """Constructor for individual emission unit calculations."""
        self.month          = month
        self.unit_key       = unit_key
        self.annual_eu      = annual_eu
        self.annual_equip   = annual_eu.annual_equip
        self.year           = self.annual_equip.year
        self.ts_interval    = self.annual_equip.ts_intervals[self.month
                                                - self.annual_equip.month_offset]
        self.EFunits        = self.annual_equip.EFs[self.month][1]
        self.equip_EF       = self.annual_equip.EFs[self.month][2]
        self.col_name_order = self.annual_equip.col_name_order
        self.equip_ptags    = self.annual_equip.equip_ptags
        self.ptags_pols     = self.annual_equip.ptags_pols
        self.toxicsEFs      = self.annual_equip.toxicsEFs
        self.flareEFs       = self.annual_equip.flareEFs
        
        self.PSAstack_annual= self.annual_eu.PSAstack_annual
        
        # fuel-sample lab results (pd.DataFrame)
        self.RFG_monthly    = ff.get_monthly_lab_results(self.annual_equip.RFG_annual, self.ts_interval)
        self.PSA_monthly    = ff.get_monthly_lab_results(self.annual_equip.PSA_annual, self.ts_interval)
        self.NG_monthly     = ff.get_monthly_lab_results(self.annual_equip.NG_annual, self.ts_interval)
        # fuel higher heating values (float)
        self.HHV_RFG        = ff.calculate_monthly_HHV(self.RFG_monthly)
        self.HHV_PSA        = ff.calculate_monthly_HHV(self.PSA_monthly)
        self.HHV_NG         = ff.calculate_monthly_HHV(self.NG_monthly)
        # fuel f-factors (floats) calculated using static chem data
        self.f_factor_PSA   = ff.calculate_monthly_f_factor(
                                            self.PSA_monthly,
                                            self.annual_equip.fpath_FG_chem,
                                            self.ts_interval)
        self.f_factor_NG    = ff.calculate_monthly_f_factor(
                                            self.NG_monthly,
                                            self.annual_equip.fpath_NG_chem,
                                            self.ts_interval)
        
        self.monthly_emis     = self.calculate_monthly_emissions()
        self.monthly_toxics   = self.calculate_monthly_toxics()
        self.monthly_emis_h2s = None

    ## emissions calc methods override parent methods
    def calculate_monthly_emissions(self):
        monthly = self.aggregate_hourly_to_monthly()
        # calculate all pollutants except for H2SO4
        efs = self.flareEFs.copy()
        NG_efs = (efs.loc[efs['flare_on']==False]
                     .loc[efs['pollutant'].isin(['pm', 'voc'])]
                     .set_index('pollutant'))
        for pol in ['pm', 'voc']:
            # use normal EFs for PSA offgas
            monthly.loc[pol] =  (
                                  monthly.loc['PSA_mscf']             # fuel value
                                * self.equip_EF[self.unit_key][pol]   # EF value
                                * self.get_conversion_multiplier(pol) # EF conversion
                                )
            # use flare EFs for NG
            if NG_efs.loc[pol, 'units'] == 'lb/mmscf':
                conv_multiplier = 1/1000
            else:
                raise ValueError('Emission Factor in unexpected units.')
            monthly.loc[pol] += (
                                  monthly.loc['NG_mscf']              # fuel value
                                * NG_efs.loc[pol, 'ef']               # EF value
                                * conv_multiplier                     # EF conversion
                                )
        # set other values in series
        monthly.loc['equipment'] = self.unit_key
        monthly.loc['month']     = self.month
        monthly.loc['fuel_rfg']  = monthly.loc['PSA_mscf']
        monthly.loc['fuel_ng']   = monthly.loc['NG_mscf']
        monthly.loc['pm25']      = monthly.loc['pm']
        monthly.loc['pm10']      = monthly.loc['pm']
        monthly.loc['h2so4']     = monthly.loc['so2'] * 0.026
        monthly = monthly.reindex(self.col_name_order)
        monthly.loc[self.col_name_order[4:-1]] = (
            monthly.loc[self.col_name_order[4:-1]] / 2000) # lbs --> tons
        return monthly
    
    def calculate_monthly_toxics(self):
        toxics = {}
        for fuel in self.get_fuel_type_for_toxics():
            base_ser = self.get_series_for_toxics(fuel)
            mult_fuel = base_ser.loc[fuel]
            mult_HHV = self.get_HHV_multiplier_for_toxics(fuel)
            tox = self.get_EFs_for_toxics(fuel).copy()
            tox.set_index('pollutant', inplace=True)
            tox.loc[tox['units']=='lb/mmscf', 'lbs'] = tox['ef'] * mult_fuel
            tox.loc[tox['units']=='lb/mmbtu', 'lbs'] = tox['ef'] * mult_fuel * mult_HHV
            tox['lbs'] = tox['lbs'] / 1000
            tox_ordered = tox['lbs'].reindex(self.get_reindexer_for_toxics())
            tox_ser = pd.concat([base_ser, tox_ordered])
            tox_ser.name = 'from' + fuel[4:]
            toxics[fuel] = tox_ser
        alltox = pd.concat(toxics, axis=1, sort=False).replace(np.nan, 0)
        alltox['total'] = alltox.sum(axis=1)
        alltox.loc['equipment','total'] = alltox.loc['equipment', 'fuel_ng']
        alltox.loc['month','total']     = alltox.loc['month', 'fuel_ng']
        reindexer = list(alltox.index)
        reindexer.insert(reindexer.index('fuel_ng')+1, reindexer.pop())
        cf.h2plant2_toxics_reindexer = reindexer
        alltox = alltox.reindex(reindexer)
        alltox.to_csv('./output/h2plant2_toxics_'
                     + str(self.year) + '_'
                     + str(self.month).zfill(2) + '.csv',
                     header=True)
        return alltox['total']
    
    def get_series_for_toxics(self, fuel_type):
        return pd.Series({'equipment':self.unit_key,
                          'month'    :self.month,
                          fuel_type  :self.monthly_emis.loc[fuel_type]})
    
    def get_fuel_type_for_toxics(self):
        return ['fuel_rfg', 'fuel_ng']

    def get_HHV_multiplier_for_toxics(self, fuel_type):
        if fuel_type == 'fuel_rfg':
            return self.HHV_PSA
        elif fuel_type == 'fuel_ng':
            return self.HHV_NG

    def get_EFs_for_toxics(self, fuel_type):
        return self.toxicsEFs

##============================================================================##

# print timestamp for checking import timing
# end_time_seconds = time.time()
# end_time = time.strftime("%H:%M:%S")
# print(end_time+'\tmodule \''+__name__+'\' finished reloading.')

# total_time = round(end_time_seconds - start_time_seconds)
# print('\t(\''+__name__+'\' total module load time: '
             # +str(total_time)+' seconds)')

import time
import pandas as pd
import numpy as np

# module-level imports
import config as cf
import equipClass

#print(time.strftime("%H:%M:%S")+'\tmodule \''+__name__+'\' reloaded.')

class AnnualParser(object):
    """Handles annual facility-wide emissions calculations."""
    """
    This class contains wrapper methods for handling data parsing and 
    emissions calculations for all emissions units, and methods for
    aggregating, slicing and dicing, formatting, and writing CSV output.
    """
    # output options
    write_csvs = True
    return_dfs = True
    
    def __init__(self, annual_equip):
        """Constructor for handling inputs, calculations, and outputs."""
        """
        Takes AnnualEquipment() instance as argument.
        """
        self.annual_equip       = annual_equip
        
        # out-directory paths
        self.out_dir_child      = cf.out_dir_child
        
        # parsed from config file
        self.year_to_calc       = cf.data_year
        self.months_to_calc     = cf.months_to_calculate
        self.equip_types        = cf.equip_types
        self.equip_to_calc      = cf.equip_to_calculate
        self.pollutants_to_calc = cf.pollutants_to_calculate
        self.pollutants_all     = cf.pollutants_all
        self.is_toxics          = cf.calculate_toxics
        self.is_FG_toxics       = cf.calculate_FG_toxics
        self.is_NG_toxics       = cf.calculate_NG_toxics
        self.is_calciner_toxics = cf.calculate_calciner_toxics
        self.is_h2plant2_toxics = cf.calculate_h2plant2_toxics
        self.write_month_names  = cf.write_month_names
        self.month_map          = cf.month_map
        self.verbose_logging    = cf.verbose_logging
    #    h2s_equips_to_calc     = cf.h2s_equips_to_calc
    #    EFs_to_check           = cf.EFs_to_check
        
        self.all_equip_dict     = {}
        self.all_equip_dict_h2s = {}

        self.toxics_text = ' '
        if self.is_toxics:
            if self.is_FG_toxics:
                self.toxics_text = ' fuel gas toxics '
            elif self.is_NG_toxics:
                self.toxics_text = ' natural gas toxics '
            elif self.is_calciner_toxics:
                self.toxics_text = ' calciner toxics '
            elif self.is_h2plant2_toxics:
                self.toxics_text = ' h2_plant_2 toxics '

        self.ordered_equip = self.annual_equip.ordered_equip
    
    def read_calculate_write_annual_emissions(self):
        """Parse data for specified equipment, calculate emissions, write CSVs."""
        if not self.is_toxics:
            format_str = ''
        elif self.is_toxics:
            if self.is_FG_toxics:
                format_str = '_TOXICS_FG'
            elif self.is_NG_toxics:
                format_str = '_TOXICS_NG'
            elif self.is_calciner_toxics:
                format_str = '_TOXICS_calciners'
            elif self.is_h2plant2_toxics:
                format_str = '_TOXICS_h2plant2'
        for df in self.groupby_annual():
            outname = (self.out_dir_child
                       +str(self.year_to_calc)+'_'
                       +df.name+'{}.csv')
            df.round(cf.round_decimals).to_csv(outname.format(format_str))
    
    def groupby_annual(self):
        """Aggregate data in multiple schemes, return pd.DataFrame list."""
        annual_df = self.format_annual_columns()
        if not self.is_toxics:
            annual_df = self.subtract_h2so4_if_output(annual_df)
        MI_col = self.return_MI_colnames(annual_df)
        print('Slicing and dicing emissions data for output.')
        
        # Groupby [equipment, month] --> [equipment, month] x pollutants
        eXm_gb = (annual_df.groupby(['WED Pt', 'Equipment', 'Month'],
                                    sort=False)
                                    .sum())
        eXm_gb.columns = MI_col
        eXm_gb.name = 'by_Equip_x_Month'
        
        # Groupby equipment --> equipment x pollutants
        e_gb = (annual_df.groupby(['WED Pt', 'Equipment'],
                                  sort=False)
                                  .sum()[list(annual_df.columns[3:])])
        e_gb.columns = MI_col
        e_gb.name = 'by_Equip'
        
        # Groupby month --> month x pollutants
        m_gb = (annual_df.groupby(['Month'],
                                  sort=False)
                                  .sum()[list(annual_df.columns[3:])])
        m_gb.columns = MI_col
        m_gb.name = 'by_Month'
        
        # Groupby [equipment, quarter] --> [equipment, quarter] x pollutants
        annQ = annual_df.copy()
        annQ['Month'].replace(cf.Qmap, inplace=True)
        annQ.rename(columns={'Month':'Quarter'}, inplace=True)
        
        eXq_gb = (annQ.groupby(['WED Pt', 'Equipment', 'Quarter'],
                               sort=False)
                               .sum())
        eXq_gb.columns = MI_col
        eXq_gb.name = 'by_Equip_x_Quarter'
        
        # Groupby [quarter, equipment] --> [quarter, equipment] x pollutants
        qXe_gb = eXq_gb.copy()
        qXe_gb.index = qXe_gb.index.swaplevel(2,1)
        qXe_gb.index = qXe_gb.index.swaplevel(1,0)
        qXe_gb.sort_index(level=0, sort_remaining=False, inplace=True)
        qXe_gb.name = 'by_Quarter_by_Equip'
        
        # Groupby quarter --> quarter x pollutants
        q_gb = (annQ.groupby(['Quarter'],
                             sort=False)
                             .sum())
        # q_gb.drop(columns=['WED Pt', 'Equipment'], inplace=True)
        q_gb.columns = MI_col
        q_gb.name = 'by_Quarter'
        
        if not self.is_toxics:
            self.groupby_annual_h2s()
        
        return [e_gb, m_gb, q_gb, eXm_gb, eXq_gb, qXe_gb]
    
# TODO: refactor to have same logic flow as for criteria pollutant values
    def groupby_annual_h2s(self):
        """Aggregate H2S output by year, write to files."""
        h2s_df = self.h2s_df_formatted.copy()
        h2s_df.drop(columns=['H2S_CEMS_src'], inplace=True)
        MI_col = self.MI_col_h2s

        # Groupby [equipment, month] --> [equipment, month] x pollutants
        eXm_gb = (h2s_df.groupby(['WED Pt', 'Equipment', 'Month'],
                                    sort=False)
                                    .sum())
        eXm_gb.columns = MI_col
        eXm_gb.name = 'by_Equip_x_Month'

        # Groupby equipment --> equipment x pollutants
        e_gb = (h2s_df.groupby(['WED Pt', 'Equipment'],
                                  sort=False)
                                  .sum()[list(h2s_df.columns[3:])])
        e_gb.columns = MI_col
        e_gb.name = 'by_Equip'

        self.h2s_eXm, self.h2s_e = eXm_gb, e_gb
        
        for df in [eXm_gb, e_gb]:
            outname = (cf.out_dir_child
                       +str(self.year_to_calc)+'_'
                       +df.name+'{}.csv')
            outname = outname.format('_H2S')
            df.round(cf.round_decimals).to_csv(outname)
    
    @staticmethod
    def subtract_h2so4_if_output(annual):
        """If H2SO4 is specified as a user output, subtract from SO2."""
        """
        H2SO4 is calculated as a fraction of SO2. If H2SO4 is specified
        as a user output, this value must be subtracted from the SO2 value
        so that it is not double counted. If H2SO4 is *not* going to written
        as an output, then the H2SO4 value is  *not* subtracted because
        that S portion should be accounted for in the output SO2 value.
        If H2SO4 is not specified as a user output but some other function
        is modified and it is still written, it will be written as zero so as
        not to be double counted.
        """
        if 'H2SO4' in cf.pollutants_to_calculate:
            annual['SO2'] = annual['SO2'] - (annual['H2SO4'] / 2000)
        else:
            annual['H2SO4'] = 0
        return annual
    
    def return_MI_colnames(self, annual_df):
        """Return MultiIndex with units of measurement for each column."""
#TODO: refactor to create MultiIndex from existing columns instead of
#      based on config file; less error-prone
        # if not calculating toxics
        if not self.is_toxics:
            drop_pollutants = [pol for pol in self.pollutants_all
                                   if pol not in self.pollutants_to_calc
                                   and pol in annual_df.columns]
            for pol in drop_pollutants:
                annual_df.drop(columns=pol, inplace=True)
            # create MultiIndex for renaming columns
            if 'CO2' not in cf.pollutants_to_calculate:
                arr_col_lev0 = ['Refinery Fuel Gas', 'Natural Gas']
                arr_col_lev1 = ['mscf'] * 2
                for pol in cf.pollutants_to_calculate:
                    if pol in ['NOx', 'CO', 'SO2', 'VOC', 'PM', 'PM25', 'PM10', 'CO2']:
                        arr_col_lev0 += [pol]
                        arr_col_lev1 += ['tons']
                    elif pol in ['H2SO4']:
                        arr_col_lev0 += [pol]
                        arr_col_lev1 += ['lbs']            
                arr_col = [arr_col_lev0, arr_col_lev1]

                # for H2S
                if len(self.annual_h2s) > 1:
                    arr_col_lev0_h2s = ['Refinery Fuel Gas']
                    arr_col_lev1_h2s = ['mscf'] * 1

                for pol in ['H2S']:
                    arr_col_lev0_h2s += [pol]
                    arr_col_lev1_h2s += ['lbs']
                    self.MI_col_h2s = pd.MultiIndex.from_arrays(
                        [arr_col_lev0_h2s, arr_col_lev1_h2s],
                         names=('Parameter', 'Units')
                         )

            elif 'CO2' in cf.pollutants_to_calculate:
                arr_col = [['Combined Fuel Gas', 'CO2'],
                           (['mscf'] * 1) + (['tons'] * 1)]

        elif self.is_toxics:
            if self.is_FG_toxics:
                arr_col_lev0 = ['Refinery Fuel Gas']
                arr_col_lev1 = ['mscf']
                for pol in cf.FG_toxics_with_EFs:
                    arr_col_lev0 += [pol]
                    arr_col_lev1 += ['lbs']
            elif self.is_NG_toxics:
                arr_col_lev0 = ['Natural Gas']
                arr_col_lev1 = ['mscf']
                for pol in cf.NG_toxics_with_EFs:
                    arr_col_lev0 += [pol]
                    arr_col_lev1 += ['lbs']
            elif self.is_calciner_toxics:
                arr_col_lev0 = ['Calcined Coke']
                arr_col_lev1 = ['tons']
                for pol in cf.calciner_toxics_with_EFs:
                    arr_col_lev0 += [pol]
                    arr_col_lev1 += ['lbs']
            elif self.is_h2plant2_toxics:
                arr_col_lev0 = ['PSA Offgas', 'Natural Gas']
                arr_col_lev1 = ['mscf'] * 2
                for pol in cf.h2plant2_toxics_reindexer[4:]:
                    arr_col_lev0 += [pol]
                    arr_col_lev1 += ['lbs']
            arr_col = [arr_col_lev0, arr_col_lev1]
        
        return pd.MultiIndex.from_arrays(arr_col, names=('Parameter', 'Units'))

    def format_annual_columns(self):
        """Format/reorder/subset columns for output."""
        annual_df = self.format_annual_labels()
        print('Formatting emissions data columns.')
        col_order = ['WED Pt', 'Equipment', 'Month']
        if not self.is_toxics:
            if 'CO2' in cf.pollutants_to_calculate:
                col_order += ['Combined Fuel Gas']
                col_order += ['CO2']
            elif 'CO2' not in cf.pollutants_to_calculate:
                col_order += ['Refinery Fuel Gas', 'Natural Gas']
                col_order += ['NOx', 'CO', 'SO2', 'VOC',
                              'PM', 'PM25', 'PM10', 'H2SO4']

            # for H2S
            if len(self.annual_h2s) > 1:
                h2s_df = self.h2s_df_labeled
                col_order_h2s = ['WED Pt', 'Equipment', 'Month']
                col_order_h2s += ['Refinery Fuel Gas']
                col_order_h2s += ['H2S', 'H2S_CEMS_src']
                h2s_df_formatted = h2s_df.rename(
                    columns=cf.output_colnames_map)[col_order_h2s]
                h2s_df_formatted.name = 'H2S_test'
                outfile_H2S = (self.out_dir_child+str(self.year_to_calc)+
                              '_'+h2s_df_formatted.name+'{}.csv'.format(''))
                self.h2s_df_formatted = h2s_df_formatted
        
        elif self.is_toxics:
            if self.is_FG_toxics:
                col_order += ['Refinery Fuel Gas']
                col_order += cf.FG_toxics_with_EFs
            elif self.is_NG_toxics:
                col_order += ['Natural Gas']
                col_order += cf.NG_toxics_with_EFs
            elif self.is_calciner_toxics:
                col_order += ['Calcined Coke']
                col_order += cf.calciner_toxics_with_EFs
            elif self.is_h2plant2_toxics:
                col_order += ['Refinery Fuel Gas', 'Natural Gas']
                col_order += cf.h2plant2_toxics_reindexer[4:]

        return annual_df.rename(columns=cf.output_colnames_map)[col_order]
    
    def format_annual_labels(self):
        """Format labels of annual emissions pd.DataFrame for CSV output."""
        annual_df = self.calculate_aggregate_all_to_annual()
        print('Formatting emissions data labels.')
        # change month integers to abbreviated names if specified
        if self.write_month_names:
            annual_df['month'] = annual_df['month'].astype(str)
            annual_df.replace({'month': self.month_map}, inplace=True)
        
        # change equipment names and WED Pt IDs to readable output names
        replace_WEDpt = dict((v,k) 
                             for k,v
                             in self.annual_equip.unitID_equip.items())
        replace_equip = self.annual_equip.unitkey_name
        annual_df['WED Pt'] = annual_df['equipment'] # make a copy to then replace
        annual_df = annual_df.replace({
                                       'WED Pt': replace_WEDpt,
                                       'equipment': replace_equip
                                      })
        
        # for H2S
        if len(self.annual_h2s) > 1:
            h2s_df = self.annual_h2s
            # change month integers to abbreviated names if specified
            if self.write_month_names:
                h2s_df['month'] = h2s_df['month'].astype(str)
                h2s_df.replace({'month': self.month_map}, inplace=True)
            h2s_df['WED Pt'] = h2s_df['equipment'] # make a copy to then replace
# TODO: refactor so that this isn't just a side effect setting as instance attribute 
            self.h2s_df_labeled = h2s_df.replace({
                                                   'WED Pt': replace_WEDpt,
                                                   'equipment': replace_equip
                                                 })
        return annual_df
    
    def calculate_aggregate_all_to_annual(self):
        """Return pd.DataFrame of annual emissions from listed equipment."""
        print('Calculating emissions for equipment and months specified...')
        if 'CO2' not in cf.pollutants_to_calculate:
            if 'heaterboiler' in cf.equip_types_to_calculate:
                annual_hb       = equipClass.AnnualHB(self.annual_equip)
            if 'coker_old' in cf.equip_types_to_calculate:
                annual_coker_old= equipClass.AnnualCokerOLD(self.annual_equip)
            if 'coker_new' in cf.equip_types_to_calculate:
                annual_coker    = equipClass.AnnualCoker(self.annual_equip)
            if 'calciner' in cf.equip_types_to_calculate:
                annual_calciner = equipClass.AnnualCalciner(self.annual_equip)
            if 'flare' in cf.equip_types_to_calculate:
                annual_flare    = equipClass.AnnualFlare(self.annual_equip)
            if 'h2plant' in cf.equip_types_to_calculate:
                annual_h2plant  = equipClass.AnnualH2Plant(self.annual_equip)
            ordered_equip_to_calculate = [
                                e for e in self.ordered_equip
                                if e in cf.equip_to_calculate
                                ]
            if self.is_toxics:
                if self.is_FG_toxics:
                    to_remove = ['h2_flare', 'h2_plant_2', 'calciner_1', 'calciner_2']
                    ordered_equip_to_calculate = [
                                e for e in ordered_equip_to_calculate
                                if e not in to_remove
                                ]
                elif self.is_NG_toxics:
                    ordered_equip_to_calculate = ['h2_plant_2']
                elif self.is_calciner_toxics:
                    ordered_equip_to_calculate = ['calciner_1', 'calciner_2']
                elif self.is_h2plant2_toxics:
                    ordered_equip_to_calculate = ['h2_plant_2']
            for unit_key in ordered_equip_to_calculate:
                each_equip_ser       = []
                each_equip_tuple_h2s = []
                for month in self.months_to_calc:
                    if self.verbose_logging:
                        print('\tCalculating month {:2d}{}emissions for {}...'
                                    .format(month, self.toxics_text,
                                            self.annual_equip.unitkey_name[unit_key]))
                    # eu_type --> 'flare', 'calciner', etc.
                    eu_type = self.equip_types[unit_key]
                    # instantiate annual equip_type class instances
                    if eu_type == 'heaterboiler':
                        eu = equipClass.MonthlyHB(unit_key, month, annual_hb)
                    elif eu_type == 'coker_new':
                        eu = equipClass.MonthlyCoker(unit_key, month, annual_coker)
                    elif eu_type == 'coker_old':
                        eu = equipClass.MonthlyCokerOLD(unit_key, month, annual_coker_old)
                    elif eu_type == 'calciner':
                        eu = equipClass.MonthlyCalciner(unit_key, month, annual_calciner)
                    elif eu_type == 'flare':
                        eu = equipClass.MonthlyFlare(unit_key, month, annual_flare)
                    elif eu_type == 'h2plant':
                        eu = equipClass.MonthlyH2Plant(unit_key, month, annual_h2plant)
                    
                    if not self.is_toxics:
                        emis = eu.monthly_emis
                        if eu.monthly_emis_h2s is None:
                            h2s_tuple = None
                        else:
                            h2s_tuple = eu.monthly_emis_h2s
                    elif self.is_toxics:
                        emis = eu.monthly_toxics
                    each_equip_ser.append(emis)
                    if not self.is_toxics:
                        each_equip_tuple_h2s.append(h2s_tuple)

                all_months = pd.concat(each_equip_ser, axis=1)
                self.all_equip_dict[unit_key] = all_months

                if each_equip_tuple_h2s:
                    tups_not_None = [tup for tup
                                     in each_equip_tuple_h2s
                                     if not tup is None]
                    all_months_h2s = pd.DataFrame(
                                        tups_not_None,
                                        columns=['month', 'equipment', 
                                                 'fuel_rfg', 'h2s', 'h2s_cems']
                                                 )
                else:
                    all_months_h2s = pd.DataFrame({'empty_col' : []})
                self.all_equip_dict_h2s[unit_key] = all_months_h2s
        
        elif 'CO2' in cf.pollutants_to_calculate:
            annual_coker_co2 = equipClass.AnnualCoker_CO2(self.annual_equip)
            for unit_key in ordered_equip_to_calculate:
                each_equip_ser = []
                for month in self.months_to_calc:
                    if self.verbose_logging:
                        print('\tCalculating month {:2d}{}emissions for {}...'
                                    .format(month, self.toxics_text,
                                            self.annual_equip.unitkey_name[unit_key]))
                    # eu_type --> 'flare' , 'calciner', etc.
                    eu_type = self.equip_types[unit_key]
                    # instantiate annual equip_type class instances
                    if eu_type == 'coker_new':
                        eu = equipClass.MonthlyCoker_CO2(unit_key, month, annual_coker_co2)
                        emis = eu.monthly_emis
                    each_equip_ser.append(emis)
                all_months = pd.concat(each_equip_ser, axis=1)
                self.all_equip_dict[unit_key] = all_months
        
        # transpose and concatenate data
        annual_dfs = []
        for v in self.all_equip_dict.values():
            v.index.name = None
            annual_dfs.append(v.T)
        annual = pd.concat(annual_dfs)
# TODO: refactor to actually return h2s df instead of side effect setting instance attribute
        if self.all_equip_dict_h2s:
            annual_dfs_h2s = []
            for v in self.all_equip_dict_h2s.values():
                v.index.name = None
                annual_dfs_h2s.append(v)
            self.annual_h2s = pd.concat(annual_dfs_h2s)
        
        # convert type "object" to type "float"
        annual[annual.columns[2:]] = annual[annual.columns[2:]].astype(float)
        return annual
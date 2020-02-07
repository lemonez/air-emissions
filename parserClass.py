import time
import pandas as pd

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
    #    h2s_equips_to_calc     = cf.h2s_equips_to_calc
    #    EFs_to_check           = cf.EFs_to_check
        self.is_toxics          = cf.we_are_calculating_toxics
        self.is_calciner_toxics = cf.calciner_toxics
        self.write_month_names  = cf.write_month_names
        self.month_map          = cf.month_map
        self.verbose_logging    = cf.verbose_logging
        
        self.all_equip_dict     = {}

        self.toxics_text = ' '
        if self.is_toxics:
            self.toxics_text = ' toxics '

        self.ordered_equip = self.annual_equip.ordered_equip
    
    def read_and_write_all(self):
        """Parse all data, calculate all emissions, write all CSVs."""
        self.groupby_and_write_annual()

    def groupby_and_write_annual(self):
        """Aggregate data in multiple schemes, write CSVs."""
        annual_df = self.format_annual_outnames()
        print('Slicing and dicing emissions data for output.')
#TODO: refactor to create MultiIndex from existing columns, not based on config file; less error-prone
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
                    #arr_col = [['Refinery Fuel Gas', 'Natural Gas',
                    #            'NOx', 'CO', 'SO2', 'VOC', 'PM', 'PM25', 'PM10', 'H2SO4'],
                    #           (['mscf'] * 2) + (['tons'] * 7) + (['lbs'] * 1)]
            elif 'CO2' in cf.pollutants_to_calculate:
                arr_col = [['Combined Fuel Gas', 'CO2'],
                           (['mscf'] * 1) + (['tons'] * 1)]
        # if calculating toxics
        else:
            # if calculating toxics but not calciner toxics
# refactor this part; stopped here 2/6/20
            if not self.is_calciner_toxics:
               arr_col = [['Refinery Fuel Gas', 'Natural Gas'] + cf.toxics_with_EFs,
                          (['mscf'] * 2) + (['lbs'] * len(cf.toxics_with_EFs))]
            # if calculating calciner toxics
            else:
               arr_col = [['Calcined Coke'] + cf.calciner_toxics_with_EFs,
                          (['tons'] * 1) + (['lbs'] * len(cf.calciner_toxics_with_EFs))]
        
        MI_col = pd.MultiIndex.from_arrays(arr_col, names=('Parameter', 'Units'))
            # df.index.names = (['Quarter', 'Equipment']) ; q_gb.head()
            # not sure if the tuple is necessary, can just pass a list
        
        # Groupby [equipment, month] --> [equipment, month] x pollutants
        eXm_gb = annual_df.groupby(['WED Pt', 'Equipment', 'Month'],
                                   sort=False).sum()
        eXm_gb.columns = MI_col
        eXm_gb.name = 'by_Equip_x_Month'
        
        # Groupby equipment --> equipment x pollutants
        e_gb = (annual_df.groupby(['WED Pt', 'Equipment'], sort=False)
                         .sum()[list(annual_df.columns[3:])])
        e_gb.columns = MI_col
        e_gb.name = 'by_Equip'
        
        # Groupby month --> month x pollutants
        m_gb = (annual_df.groupby('Month', sort=False)
                         .sum()[list(annual_df.columns[3:])])
        m_gb.columns = MI_col
        m_gb.name = 'by_Month'
        
        # Groupby [equipment, quarter] --> [equipment, quarter] x pollutants
        Qs = {
                1   : 'Q1',
                2   : 'Q1',
                3   : 'Q1',
                4   : 'Q2',
                5   : 'Q2',
                6   : 'Q2',
                7   : 'Q3',
                8   : 'Q3',
                9   : 'Q3',
                10  : 'Q4',
                11  : 'Q4',
                12  : 'Q4',
               
               'Jan': 'Q1',
               'Feb': 'Q1',
               'Mar': 'Q1',
               'Apr': 'Q2',
               'May': 'Q2',
               'Jun': 'Q2',
               'Jul': 'Q3',
               'Aug': 'Q3',
               'Sep': 'Q3',
               'Oct': 'Q4',
               'Nov': 'Q4',
               'Dec': 'Q4'
               }
        
        annQ = annual_df.copy()
        annQ['Month'].replace(Qs, inplace=True)
        annQ.rename(columns={'Month':'Quarter'}, inplace=True)
        
        eXq_gb = annQ.groupby(['WED Pt', 'Equipment', 'Quarter'], sort=False).sum()
        eXq_gb.columns = MI_col
        eXq_gb.name = 'by_Equip_x_Quarter'
        
        # Groupby [quarter, equipment] --> [quarter, equipment] x pollutants
        qXe_gb = eXq_gb.copy()
        qXe_gb.index = qXe_gb.index.swaplevel(2,1)
        qXe_gb.index = qXe_gb.index.swaplevel(1,0)
        qXe_gb.sort_index(level=0, sort_remaining=False, inplace=True)
        qXe_gb.name = 'by_Quarter_by_Equip'
        
        # Groupby quarter --> quarter x pollutants
        q_gb = annQ.groupby('Quarter', sort=False).sum()
        # q_gb.drop(columns=['WED Pt', 'Equipment'], inplace=True)
        q_gb.name = 'by_Quarter' 
        
        frames = [e_gb, m_gb, q_gb, eXm_gb, eXq_gb, qXe_gb]
        
        if self.write_csvs:
        
            print('Writing output to files.')
        
            for df in frames:        
                outname = (self.out_dir_child
                           +str(self.year_to_calc)+'_'
                           +df.name+'{}.csv')
                if not self.is_toxics:
                    outname = outname.format('')
                else:
                    if not self.is_calciner_toxics:
                        outname = outname.format('_TOXICS')
                    else:
                        outname = outname.format('_calciners_TOXICS')
                    
                df.round(cf.round_decimals).to_csv(outname)
        
        if self.return_dfs: # DEBUG
            return frames
    
    def format_annual_outnames(self):
        """Format labels of annual emissions pd.DataFrame for CSV output."""
        annual_df = self.aggregate_all_to_annual()
        print('Formatting emissions data.')
        # change month integers to abbreviated names if specified
        if self.write_month_names:
            annual_df['month'] = annual_df['month'].astype(str)
            annual_df.replace({'month': self.month_map}, inplace=True)
        
        # change equipment names and column headers to readable output names
        annual_df['WED Pt'] = annual_df['equipment'].replace(
                                            dict((v,k)
                                            for k,v
                                            in self.annual_equip
                                                   .unitID_equip.items()))

        col_order = ['WED Pt', 'Equipment', 'Month']
        if not self.is_toxics:
            if 'CO2' in cf.pollutants_to_calculate:
                col_order += ['Combined Fuel Gas']
                col_order += ['CO2']
            elif 'CO2' not in cf.pollutants_to_calculate:
                col_order += ['Refinery Fuel Gas', 'Natural Gas']
                col_order += ['NOx', 'CO', 'SO2', 'VOC',
                              'PM', 'PM25', 'PM10', 'H2SO4']
        elif self.is_toxics:
            if self.is_calciner_toxics:
                col_order += ['Calcined Coke']
                col_order += cf.calciner_toxics_with_EFs
            elif not self.is_calciner_toxics:
                col_order += ['Refinery Fuel Gas', 'Natural Gas']
                col_order += cf.toxics_with_EFs
        print(col_order)
        annual_df = (annual_df
                        .replace({'equipment': self.annual_equip.unitkey_name})
                        .rename(columns=cf.output_colnames_map)
                    )[col_order]
        return annual_df

    def aggregate_all_to_annual(self):
        """Return pd.DataFrame of annual emissions from listed equipment."""
        print('Calculating emissions for equipment and months specified...')
        if self.is_toxics:
            if self.is_calciner_toxics:
                ordered_equip_to_calculate = ['calciner_1', 'calciner_2']
            elif not self.is_calciner_toxics:
                to_remove = ['h2_flare', 'h2_plant_2', 'calciner_1', 'calciner_2']
                ordered_equip_to_calculate = [
                                    equip for equip in self.ordered_equip
                                    if equip in cf.equip_to_calculate
                                    and equip not in to_remove
                                    ]
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

            for unit_key in ordered_equip_to_calculate:
                each_equip_ser  = []
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
                    elif self.is_toxics:
                        emis = eu.monthly_toxics
                    each_equip_ser.append(emis)

                all_months = pd.concat(each_equip_ser, axis=1)
                self.all_equip_dict[unit_key] = all_months
        elif 'CO2' in cf.pollutants_to_calculate:
            annual_coker_co2 = equipClass.AnnualCoker_CO2(self.annual_equip)
            for unit_key in ordered_equip_to_calculate:
                each_equip_ser  = []
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
            
        # transpose and concatenate DataFrames
        annual_dfs = []
        for v in self.all_equip_dict.values():
            v.index.name = None
            annual_dfs.append(v.T)
        annual = pd.concat(annual_dfs)
        
        # convert type "object" to type "float"
        annual[annual.columns[2:]] = annual[annual.columns[2:]].astype(float)
        return annual
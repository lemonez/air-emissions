import pandas as pd

import config as cf
import equipclass

# print timestamp for checking import timing
import time
print(time.strftime("%H:%M:%S")+'\tmodule \''+__name__+'\' reloaded.')

class AnnualParser(object):
    """Class containing wrapper methods for handling data parsing and 
    emissions calculations for refinery equipment, and methods for
    aggregating, slicing and dicing, and writing CSV output"""
    
    # class variables from config file
    year_to_calc       = cf.data_year
    months_to_calc     = cf.months_to_calculate
    equips_to_calc     = cf.equips_to_calculate
#    h2s_equips_to_calc = cf.h2s_equips_to_calc
#    EFs_to_check       = cf.EFs_to_check
    is_toxics          = cf.we_are_calculating_toxics
    is_calciner_toxics = cf.calciner_toxics
    
    def __init__(self):
        pass
    
    def read_and_write_all(self):
        """Parse CEMS and fuel-use data for all equipment, calculate annual
        emissions, and write annual CSVs for various aggregation schemes."""
        
        a = self.aggregate_all_to_annual(self.equips_to_calc, self.months_to_calc)
        f = self.format_annual(a)
        self.groupby_and_write_annual(f, self.year_to_calc)
    
    def aggregate_all_to_annual(self, equip_list, months_to_parse, toxics=is_toxics):
        """Generate annual emissions DataFrame from list of equipment keys."""
        
        ordered_equips_to_calculate = self.equips_to_calc
        
        # calculate emissions for every month x equipment combo specified
        print('Calculating emissions for equipment and months specified...')
        
        all_equip_dict = {}
        
        toxics_text = ' '
        if toxics:
            toxics_text = ' toxics '
            
        for equip in ordered_equips_to_calculate:
            each_equip_ser = []
            for month in months_to_parse:
                if cf.verbose_calc_status:
                    print('\tCalculating month {:2d}{}emissions for {}...'
                                .format(month, toxics_text, equip))
                inst = equipclass.Equipment(equip, month) # instantiate indiv equip
                
                if not toxics:
                    p = inst.get_monthly_co2_emissions()
                    m = inst.calc_monthly_equip_emissions(p)
                else:
                    m = inst.calculate_monthly_toxics()
                each_equip_ser.append(m)
            all_months = pd.concat(each_equip_ser, axis=1)
            all_equip_dict[equip] = all_months
        
        # transpose and concatenate DataFrames
        annual_dfs = []
        for v in all_equip_dict.values():
            v.index.name = None
            annual_dfs.append(v.T)
        annual = pd.concat(annual_dfs)
        
        # convert type "object" to type "float"
        annual[annual.columns[2:]] = annual[annual.columns[2:]].astype(float)
        
        return annual
    
    @staticmethod
    def format_annual(annual_df, month_names=cf.month_names,
                      toxics=is_toxics, calciner_toxics=is_calciner_toxics):
        """Format data labels of annual emissions DataFrame
        ([equipment, month] x pollutants) for CSV output."""
        
        from equipclass import Equipment
        
        print('Formatting emissions data.')
        # change month integers to abbreviated names if desired
        if month_names:
            months_int  = list(range(1,13))  # [1, 2, 3... 12]
            months_str  = [str(i) for i in months_int]
            months_abrv = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                           'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

            month_map = {}
            for i, a in zip(months_str, months_abrv):
                month_map[i] = a
            
            annual_df['month'] = annual_df['month'].astype(str)
            annual_df.replace({'month': month_map}, inplace=True)
        
        # pretty up the names of equipment and column headers for output
        annual_df['WED Pt'] = annual_df['equipment'].replace(
                                            dict((v,k)
                                            for k,v
                                            in Equipment.unitID_equip.items()))

        # column names to pretty up for final output
        output_colnames = {
                'month'    : 'Month',
                'equipment': 'Equipment',
                'rfg_mscfh': 'Refinery Fuel Gas',
                'co2'      : 'CO2'
                # 'fuel_rfg' : 'Refinery Fuel Gas',
                # 'fuel_ng'  : 'Natural Gas',
                # 'coke_tons': 'Calcined Coke',
                # 'nox'      : 'NOx',
                # 'co'       : 'CO',
                # 'so2'      : 'SO2',
                # 'voc'      : 'VOC', 
                # 'pm'       : 'PM',
                # 'pm25'     : 'PM25',
                # 'pm10'     : 'PM10',
                # 'h2so4'    : 'H2SO4'
                }

        col_order = ['WED Pt', 'Equipment', 'Month', 'Refinery Fuel Gas', 'CO2']
                
        annual_df = (annual_df.replace({'equipment': Equipment.unitkey_name})
                              .rename(columns=output_colnames))
        
        annual_df = annual_df[col_order]
        
        return annual_df
        
    @staticmethod
    def groupby_and_write_annual(annual_df, year_to_parse,
                                 toxics=is_toxics,
                                 calciner_toxics=is_calciner_toxics,
                                 write_csvs=True, return_dfs=False):
        """Generate summary tables and write CSV output."""

        print('Slicing and dicing emissions data for output.')
        
        # create MultiIndex for renaming columns
        if not toxics:
            arr_col = [['Refinery Fuel Gas','CO'],
                       (['mscf'] * 1) + (['tons'] * 1)]
        else:
            if not calciner_toxics:
                arr_col = [['Refinery Fuel Gas', 'Natural Gas'] + cf.toxics_with_EFs,
                           (['mscf'] * 2) + (['lbs'] * len(cf.toxics_with_EFs))]
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
        
        if write_csvs:
        
            print('Writing output to files.')
        
            for df in frames:        
                outname = (cf.out_dir_child+str(year_to_parse)+'_'
                           +df.name+'{}.csv')
                if not toxics:
                    outname = outname.format('')
                else:
                    if not calciner_toxics:
                        outname = outname.format('_TOXICS')
                    else:
                        outname = outname.format('_calciners_TOXICS')
                    
                df.round(2).to_csv(outname)
        
        # option for returning DataFrames for debugging purposes
        if return_dfs:
            return frames
    

    
import time
import pandas as pd

#print(time.strftime("%H:%M:%S")+'\tmodule \''+__name__+'\' reloaded.')

""" 40 CFR Appendix A-7 to Part 60 - Test Method 19 -
    Determination of sulfur dioxide removal efficiency and 
    particulate, sulfur dioxide, and nitrogen oxides emission rates
    <https://www.law.cornell.edu/cfr/text/40/appendix-A-7_to_part_60#>"""

# constants for fuel f-factor calculation
ff_constants = {
        'mw_C' : 12,
        'mw_H' : 1.008,
        'mw_O' : 32,
        'mw_N' : 28,
        
        'K'  : 1000000    ,  # Conversion factor: 10^−5 (kJ/J)/(%) [106 Btu/million Btu]
        'Kc' : 1.53       ,  # Constant: (9.57 scm/kg)/% [(1.53 scf/lb)/%]
        'Khd': 3.64       ,  # Constant: (22.7 scm/kg)/% [(3.64 scf/lb)/%]
        'Ko' : 0.46       ,  # Constant: (2.85 scm/kg)/% [(0.46 scf/lb)/%]
        'Kn' : 0.14       ,  # Constant: (0.86 scm/kg)/% [(0.14 scf/lb)/%]
        'Ks' : 0.57       ,  # Constant: (3.54 scm/kg)/% [(0.57 scf/lb)/%]
        'Vm' : 385.326       # Calculated: Vm=R*Ts/Ps (ideal gas law at standard conditions)
                             #   (currently hardcoded but variables for T and P could
                             #   potentially be substituted if not at standard conditions)
        }

# map chemical compounds to raw strings from CEMS output

NG_compounds = {
        'Carbon dioxide- mol%'   : 'Carbon dioxide'   ,
        'Nitrogen- mol%'         : 'Nitrogen'         ,
        'Methane- mol%'          : 'Methane'          ,
        'Ethane- mol%'           : 'Ethane'           ,
        'Propane- mol%'          : 'Propane'          ,
        'Isobutane- mol%'        : 'Isobutane'        ,
        'n-Butane- mol%'         : 'n-Butane'         ,
        'Isopentane- mol%'       : 'Isopentane'       ,
        'n-Pentane- mol%'        : 'n-Pentane'        ,
        'C6 -  mol%'             : 'C6'               ,
        'C7- mol%'               : 'C7'               ,
        'C8- mol%'               : 'C8'               ,
        'C9- mol%'               : 'C9'               ,
        'C10- mol%'              : 'C10'              ,
        'C10+ - mol%'            : 'C10+'             ,
        'Hydrogen sulfide- mol%' : 'Hydrogen sulfide' ,
        'H2O lb'                 : 'H2O'               # remember this is in lbs
        }
          
FG_compounds = {          
        'Hydrogen- mol%'          : 'Hydrogen'          ,
        'Carbon dioxide- mol%'    : 'Carbon dioxide'    ,
        'Hydrogen sulfide- m%'    : 'Hydrogen sulfide'  ,
        'Oxygen- mol%'            : 'Oxygen'            ,
        'Nitrogen- mol%'          : 'Nitrogen'          ,
        'Carbon monoxide- mol%'   : 'Carbon monoxide'   ,
        'Methane- mol%'           : 'Methane'           ,
        'Ethane- mol%'            : 'Ethane'            ,
        'Ethylene- mol%'          : 'Ethylene'          ,
        'Propane- mol%'           : 'Propane'           ,
        'Cyclopropane- mol%'      : 'Cyclopropane'      ,
        'Propylene- mol%'         : 'Propylene'         ,
        'Acetylene- mol%'         : 'Acetylene'         ,
        'Isobutane- mol%'         : 'Isobutane'         ,
        'Propadiene- mol%'        : 'Propadiene'        ,
        'n-Butane- mol%'          : 'n-Butane'          ,
        'Cyclobutane- mol%'       : 'Cyclobutane'       ,
        't-2-Butene- mol%'        : 't-2-Butene'        ,
        '1-Butene- mol%'          : '1-Butene'          ,
        'Isobutene- mol%'         : 'Isobutene'         ,
        'c-2-Butene- mol%'        : 'c-2-Butene'        ,
        'Neopentane- mol%'        : 'Neopentane'        ,
        'Cyclopentane- mol%'      : 'Cyclopentane'      ,
        'Isopentane- mol%'        : 'Isopentane'        ,
        'Methylacetylene- mol%'   : 'Methylacetylene'   ,
        'n-Pentane- mol%'         : 'n-Pentane'         ,
        '1,3-Butadiene- mol%'     : '1,3-Butadiene'     ,
        '3-Methyl-1-Butene- mol%' : '3-Methyl-1-Butene' ,
        't-2-Pentene- mol%'       : 't-2-Pentene'       ,
        '2-Methyl-2-Butene- mol%' : '2-Methyl-2-Butene' ,
        '1-Pentene- mol%'         : '1-Pentene'         ,
        '2-Methyl-1-Butene- mol%' : '2-Methyl-1-Butene' ,
        'c-2-Pentene- mol%'       : 'c-2-Pentene'       ,
        'Hexanes Plus- mol%'      : 'Hexanes Plus'
        }

def read_chem_constants(path):
    """Read chemistry constants/parameters, return pd.DataFrame."""
    data = pd.read_csv(path)
    return data

def parse_annual_NG_lab_results(path, sheet):
    """Read natural-gas lab-test results and return pd.DataFrame."""
    """
    Fuel analysis tests completed several times monthly. Data is
    averaged on a monthly basis.
	"""
    data = pd.read_excel(path, sheet_name=sheet, skiprows=8)[:12]
    data['Sample Date'] = pd.to_datetime(data['Sample Date'])
    
    data = data.T.rename(columns=data.T.iloc[0],
                         index=NG_compounds)['GBTU/CF':'H2O']
    data.columns.name = 'test_date'
    data.index.name   = 'compound'
    
    # order like fuel gas dataframe
    data = (data.reindex(index=list(data.index[2:])
           + [data.index[1], data.index[0]]))
    return data

def parse_annual_FG_lab_results(path, sheet):
    """Read fuel-gas lab-test results, return pd.DataFrame."""
    """
    Fuel analysis tests completed several times monthly for RFG,
    cokerFG, CVTG, and H2 flare. Data is averaged on a monthly basis.
	"""
    data = pd.read_excel(path, sheet_name=sheet, skiprows=4, header=[0,1], index_col=0)
    # replace missing symbols with NaN
    data.replace(['****', '--'], pd.np.nan, inplace=True)
    
    # combine data and time MultiIndex rows to create one DatetimeIndex
            # if we wanted to drop the time label...but might be good to leave so that columns are unique
            # data.columns = data.columns.droplevel(1)
    # get date component; format as date string
    d_idx = data.columns.get_level_values(0).strftime('%Y-%m-%d')
    
    # get datetime.time component as string
    t = data.columns.get_level_values(1).astype(str)
    
    # convert to datetime, format as time string
    t_idx = pd.to_datetime(t).strftime('%H:%M:%S')
    
    # concatenate zipped lists elementwise to create datetime stamps
    dt_idx = pd.to_datetime(['{} {}'.format(date_, time_) 
                             for date_, time_
                             in zip(d_idx,t_idx)])
    
    # reassign and rename columns and indices
    data.columns = dt_idx
    data.columns.name = 'test_date'
    data.index.name = 'compound'
    data.rename(index=FG_compounds, inplace=True)
        # could also use: data.index = data.index.map(FG_compounds)
#    data.loc['Oxygen', :] = 0
    return data

def get_monthly_lab_results(annual_gas_test_df, ts_interval):
    """Return pd.DataFrame with gas lab-test results for specified month."""
    start = ts_interval[0]
    end   = ts_interval[1]
    cols = annual_gas_test_df.columns
    
    # subset dataframe for samples taken within time interval of interest
    sub = annual_gas_test_df.loc[:, cols[(cols >= start) & (cols <= end)]]
    return sub

def calculate_monthly_HHV(monthly_gas_test_results_df):
    """Calculate average higher heating value (HHV, gbtu/cf) for specified month."""
    HHV = monthly_gas_test_results_df.loc['GBTU/CF'].mean()
    return HHV
    
def calculate_monthly_f_factor(gas_test_results_df, gas_chem_path,
                                ts_interval, ff_terms=ff_constants):
    """Calculate refinery fuel gas Fd-factor for specified month."""
    """
    Uses chemistry constants and gas lab-analysis data, adhering to:
    "EPA Test Method 19 - 40 CFR Appendix A-7 to Part 60 -
    Determination of sulfur dioxide removal efficiency and
    particulate, sulfur dioxide and nitrogen oxides emission rates."
    """
    chem   = read_chem_constants(gas_chem_path)
    ftable = (pd.merge(gas_test_results_df, chem, how='left',
                       left_on='compound', right_on='compound')
                .melt(id_vars=['compound', 'mw', 'atoms_C',
                               'atoms_H', 'atoms_O', 'atoms_N'],
                      var_name='test_date', value_name='mol%')
                .set_index(['test_date', 'compound'])
             )
    
    # calculate proportions of chemical components
    ftable['lb/mol']   = ftable['mw']* ftable['mol%'] / 100
    ftable['lb/mol_C'] = (ftable['mol%'] / 100 * ftable['atoms_C']
                                               * ff_terms['mw_C'])
    ftable['lb/mol_H'] = (ftable['mol%'] / 100 * ftable['atoms_H']
                                               * ff_terms['mw_H'])
    ftable['lb/mol_O'] = (ftable['mol%'] / 100 * ftable['atoms_O']
                                               * ff_terms['mw_O'])
    ftable['lb/mol_N'] = (ftable['mol%'] / 100 * ftable['atoms_N']
                                               * ff_terms['mw_N'])
    
    # re-select columns in more appropriate order
    ftable = ftable[[
            'mw', 'mol%', 'lb/mol',
            'atoms_C', 'lb/mol_C',
            'atoms_H', 'lb/mol_H',
            'atoms_O', 'lb/mol_O',
            'atoms_N', 'lb/mol_N'
            ]]
    
    # cast as float b/c some columns are created as 'objects' for some reason
    ftable = ftable.astype(float)
    
    ftable = ftable.groupby('compound').mean()
    
    # calculate terms for fuel f-factor calculation
    ff_terms['fuel']     = ftable['lb/mol'].sum()
    ff_terms['lb/mol_C'] = ftable['lb/mol_C'].sum()
    ff_terms['lb/mol_H'] = ftable['lb/mol_H'].sum()
    ff_terms['lb/mol_O'] = ftable['lb/mol_O'].sum()
    ff_terms['lb/mol_N'] = ftable['lb/mol_N'].sum()
    ff_terms['lb/mol_S'] = ftable.loc['Hydrogen sulfide', 'mol%'] / 100 * 32.06
    
    # Calculated: Conc of C from an ultimate analysis of fuel, weight percent
    ff_terms['C'] = ff_terms['lb/mol_C'] / ff_terms['fuel'] * 100
    # Calculated: Conc of H from an ultimate analysis of fuel, weight percent
    ff_terms['H'] = ff_terms['lb/mol_H'] / ff_terms['fuel'] * 100
    # Calculated: Conc of O from an ultimate analysis of fuel, weight percent
    ff_terms['O'] = ff_terms['lb/mol_O'] / ff_terms['fuel'] * 100
    # Calculated: Conc of N from an ultimate analysis of fuel, weight percent
    ff_terms['N'] = ff_terms['lb/mol_N'] / ff_terms['fuel'] * 100
    # Based on H2S: Conc of S from an ultimate analysis of fuel, weight percent
    ff_terms['S'] = ff_terms['lb/mol_S'] / ff_terms['fuel'] * 100
    # GBTU*Vm/Fuel (lb/mol)
    ff_terms['GCV'] = (calculate_monthly_HHV(gas_test_results_df)
                       * (520/528) * ff_terms['Vm'] / ff_terms['fuel'])
        
    # calculate fuel 'f-factor' (dscf/MMbtu) (see: EPA Test Method 19)
    f_factor = ( ff_terms['K'] * (   ( ff_terms['Kc']  * ff_terms['C'] )
                                   + ( ff_terms['Khd'] * ff_terms['H'] )
                                   - ( ff_terms['Ko']  * ff_terms['O'] )
                                   + ( ff_terms['Kn']  * ff_terms['N'] )
                                   + ( ff_terms['Ks']  * ff_terms['S'] )
                                 )
                               / ff_terms['GCV']
               )
    return f_factor
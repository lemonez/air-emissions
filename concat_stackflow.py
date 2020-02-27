# concat monthly stack flows to one file for Shannon for GHG calcs
import glob
import pandas as pd
import config as cf

def concat_monthly_flows():
    """Concatenate monthly flow logs into one file for reporting and QA."""
    data_dir = cf.out_dir+cf.out_dir_child+'/'
    print('Data directory: '+data_dir)
    west = sorted(glob.glob(data_dir+str(cf.data_year)+'_*_stackflow*w.csv'))
    east = sorted(glob.glob(data_dir+str(cf.data_year)+'_*_stackflow*e.csv'))

    print('Stacking stack flows...')
    for coker in list(zip([east, west], ['east', 'west'])):
        months = []
        for f in coker[0]:
            month = pd.read_csv(f, index_col=0)
            months.append(month)
        year = pd.concat(months)
        year.index.name = 'Timestamp'
        year.name = coker[1]
        year.to_csv(data_dir+str(cf.data_year)+'_stackflow_coker_'+year.name+'.csv')

concat_monthly_flows()
print('Done.')
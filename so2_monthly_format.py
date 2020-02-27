# pivot monthly data to get format for SO2 attainment master spreadsheet
import pandas as pd

for data_year in [2018, 2019]:

    em = pd.read_csv('dev/'+str(data_year)+'_by_Equip_x_Month.csv', skiprows=2)
    em.columns = ['WED Pt', 	'Equipment',	'Month', 'Refinery Fuel Gas',	'Natural Gas',	'SO2']
    em['SO2'] = em['SO2'] * 2000
    em.drop(columns=['Refinery Fuel Gas', 'Natural Gas'], inplace=True)
    print(em.head())
    wed_order_map = {
    '10 VTG' : 1 ,
    '10 RFG' : 2 ,
    '11'     : 3 ,
    '12'     : 4 ,
    '20'     : 5 ,
    '21'     : 6 ,
    '22'     : 7 ,
    '23'     : 8 ,
    '27'     : 9 ,
    '30'     : 10,
    '31'     : 11,
    '32'     : 12,
    '33'     : 13,
    '40'     : 14,
    '41'     : 15,
    '40 E'   : 16,
    '41 W'   : 17,
    '46'     : 18,
    '50'     : 19,
    '51'     : 20,
    '52'     : 21,
    '60'     : 22,
    '61'     : 23,
    '70'     : 24,
    '71'     : 25,
    '80'     : 26,
    '104'    : 27,
    '105'    : 28,
    '106'    : 29,
    '107'    : 30,
    '112'    : 31
    }

    wed_name_map = em.set_index('WED Pt')['Equipment'].to_dict()

    month_map = cf.generate_month_map() ; month_map
    month_map = {int(k):v for k, v in month_map.items()} ; month_map
    month_map_rev = {v:int(k) for k, v in month_map.items()} ; month_map_rev

    me_piv = em.pivot(index='WED Pt', columns='Month', values='SO2')

    me_piv.columns = me_piv.columns.map(month_map_rev) ; me_piv.head()

    me_piv.sort_index(axis=1, inplace=True) ; me_piv.head()

    me_piv.columns = me_piv.columns.map(month_map_rev_rev) ; me_piv.head()

    me_piv['wed_order'] = me_piv.index.map(wed_map)

    me_piv = me_piv.sort_values('wed_order').drop(columns='wed_order') ; me_piv

    equip = pd.DataFrame(index=me_piv.index, columns={'Equipment'})
    equip['Equipment'] = equip.index.map(wed_name_map)

    final = pd.concat([equip, me_piv], sort=False, axis=1).round(2)
    print(final.head())
    final.to_csv('output/'+str(data_year)+'_SO2.csv', index=True)
    print('wrote: '+'output/'+str(data_year)+'_SO2.csv')
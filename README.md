# air-emissions
tool to calculate refinery emissions for government reporting



#########
##USAGE##
#########

1)  Edit config.py to specify months, emissions units, and equipment
    of interest, filepaths, etc.
    By default all months are parsed for all equipment,
    emissions are calculated, and annual aggregated
    summary results are written to CSVs in './output/'.

2)  Run the following from the command line:
 $ calcbp.csh [-h]
 $ python3 __main.py__
 $ %run __main__.py # or run it this way from Jupyter


############################
##CALCULATIONS / DATA FLOW##
############################

    For each type of fuel gas, the following values are calculated
        - monthly average HHVs from weekly lab analyses
        - monthly average f-factors from weekly lab analyses
    For each refinery equipment unit, emissions are calculated from:
        - hourly CEMS from PI tags
        - hourly fuel usage from PI tags
        - monthly emission factors (for equipment without CEMS)
    Emissions are calculated for the following pollutants:
        - NOx, CO, SO2, VOC, PM, PM25, PM10, H2SO4, H2S; CO2 can be calculated for E/W cokers (2019+)
    Emissions are calculated and summed on a monthly basis and expressed as:
        - monthly totals
        - quarterly totals
        - annual totals
    H2S emissions for boilers and heaters are based on H2S CEMS from the following systems:
        - RFG       --> 30AI568A
        - Coker     --> 12AI55A, 12AI3751.PV (new cokers 2019+)
        - Crude VTG --> 10AI136A


#######################
##DIRECTORY STRUCTURE##
#######################

./...
    README.txt      # this document; outlines code structure and provides basic usage instructions
    __main__.py     # control module called from command line
    config.py       # user edits; specify equipment, months, pollutants to calculate
    equipClass.py   # class and methods for parsing data and refinery equipment emissions calcs
    parserClass.py  # wrapper module to calculate, format, and output emissions values
    ffactor.py      # calculations for refinery-fuel f-factors
    descrips.txt    # example data structures with explanations
    raw_BP.ipynb    # Jupyter iPython Notebook; working notebook, contains qa and other functions
    
./Data/...
    Static/                 # data that does not change monthly/annually (except with additions/modifications to equipment)
        (files)
    Annual/                 # monthly/annual time-series data (fuel, lab analyses) and emission-factor data
        (files)
        CEMS/
            01-YYYY_...             # monthly CEMS data files; must be named as 'MM-YYYY_...'
            02-YYYY_...

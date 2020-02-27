# global import external libraries
import pandas as pd

# module-level imports
import config as cf

def main():
    """main script controller function"""
    import os, time

    # unpack arguments as dict and push back to config module
    args_dict = vars(get_args())
    
    #if args_dict['log_suffix'] != '':
    #    args_dict['log_suffix'] = '_' + args_dict['log_suffix']
    append_slash_to_dir(args_dict, ['out_dir', 'out_dir_child'])
    args_dict['out_dir_child'] = args_dict['out_dir'] + args_dict['out_dir_child']
    if args_dict['quiet']:
        args_dict['verbose_logging'] = False
    
    update_config(cf, args_dict)

    if cf.verbose_logging or cf.view_config:
        print('\nConfiguration options specified:\n')
        for k,v in args_dict.items():
            if k == 'months_to_calculate':
                mos = []
                for mo in args_dict['months_to_calculate']:
                    mos.append(str(mo))
                mo_start = cf.month_map.get(mos[0])
                mo_end   = cf.month_map.get(mos[-1])
                if mo_start != mo_end:
                    print('    {:<28}: {} to {}'.format(k, mo_start, mo_end))
                else:
                    print('    {:<28}: {}'.format(k, mo_start))
            elif k == 'equip_to_calculate':
                print('    {:<28}: {}'.format(k, args_dict[k][0]))
                for eq in args_dict[k][1:]:
                    print('    {:<28}: {}'.format('', eq))
            else:
                print('    {:<28}: {}'.format(k,v))
        print('\n')

    cf.verify_pollutants_to_calc(cf.pollutants_to_calculate)

    # ensure output directories exist
    for dir in [cf.out_dir, cf.out_dir_child, cf.log_dir]:
        if not os.path.exists(dir):
            os.makedirs(dir)
            print('Created directory \''+dir+'\' for output files.\n')
    
    if cf.view_config:
        import sys
        sys.exit(0)
    
    # print start timestamp for checking script runtime
    start_time_seconds = time.time()
    start_time = time.strftime("%H:%M:%S")
    print('\n'+start_time+'\tmodule \''+__name__+'\' began running.')
    
    # parse input, calculate emissions, write output
    from parserClass import AnnualParser
    from equipClass import AnnualEquipment
    
    ae = AnnualEquipment()
    
    if cf.calculate_criteria:
        print('\n')
        AnnualParser(
            ae, calculation='criteria').read_calculate_write_annual_emissions()
    if cf.calculate_FG_toxics:
        print('\n')
        AnnualParser(
            ae, calculation='FG_toxics').read_calculate_write_annual_emissions()
    if cf.calculate_calciner_toxics:
        print('\n')
        AnnualParser(
            ae, calculation='calciner_toxics').read_calculate_write_annual_emissions()
    if cf.calculate_h2plant2_toxics:
        print('\n')
        AnnualParser(
            ae, calculation='h2plant2_toxics').read_calculate_write_annual_emissions()
    
    # print total time for script runtime
    end_time_seconds = time.time()
    end_time = time.strftime("%H:%M:%S")
    total_time = round(end_time_seconds - start_time_seconds)
    print('\n'+end_time+'\tmodule \''+__name__+'\' completed.')
    print('\t(\''+__name__+'\' total script runtime: '
                 +str(round((total_time / 60),1))+' minutes)')

def get_args():
    """parse arguments from command line"""
    
    import argparse, textwrap
    
    class BlankLinesHelpFormatter (argparse.HelpFormatter):
        def _split_lines(self, text, width):
            return super()._split_lines(text, width) + ['']
        
    parser = argparse.ArgumentParser(
                formatter_class=BlankLinesHelpFormatter,
                prog='BP: cokerghg',
                add_help=True)#,
                # formatter_class = argparse.RawDescriptionHelpFormatter,
                # description = textwrap.dedent('''\
                # Calculate CO2 emissions for BP E/W coker heaters. 
                # '''),
                # epilog=textwrap.dedent('''\
                # can put text here for further explanation

                # more text here possibly
                      # maybe some bullet point-like things here
                      # and here
                # '''))

    group1 = parser.add_argument_group('File I/O')
    group1.add_argument('-d', '--inpath', '--data', 
                        dest='data_dir', metavar='InDir',
                        default=cf.data_dir,
                        help='Path to data (default: \'%(default)s\'.')
    group1.add_argument('-o', '--outpath',
                        dest='out_dir', metavar='OutDir',
                        default=cf.out_dir,
                        help='Path to save output (default: \'%(default)s\').')
    group1.add_argument('-c', '--outpath_child',
                        dest='out_dir_child', metavar='OutChild',
                        default=cf.out_dir_child,
                        help='Path to save output iteration (default: \'%(default)s\').')
    group1.add_argument('-L', '--logpath',
                        dest='log_dir', metavar='LogDir',
                        default=cf.log_dir,
                        help='Path to save logfiles (default: \'%(default)s\').')
    group1.add_argument('-x', '--logsuffix',
                        dest='log_suffix', metavar='LogSuf',
                        default=cf.log_suffix,
                        help='Suffix to append to logfile names (default: \'%(default)s\').')

    group2 = parser.add_argument_group('Data / Calc Options')
    group2.add_argument('-e', '--equip',
                        dest='equip_to_calculate', metavar='Equips',
                        default=cf.equip_to_calculate,
                        help='Equipment units to calculate (default: %(default)s).')
    group2.add_argument('-y', '--year',
                        dest='data_year', metavar='DataYear',
                        default=cf.data_year,
                        help='Year at end of met dataset (default: %(default)s).')
    group2.add_argument('-m', '--months',
                        dest='months_to_calculate', metavar='Months',
                        default=cf.months_to_calculate,
                        help='Months of data to parse (default: %(default)s).')
    group2.add_argument('--criteria',
                        dest='calculate_criteria', metavar='T/F',
                        default=cf.calculate_criteria,
                        help='Whether or not to calculate criteria pollutants (default: %(default)s).')
    group2.add_argument('--ftoxics',
                        dest='calculate_FG_toxics', metavar='T/F',
                        default=cf.calculate_FG_toxics,
                        help='Whether or not to calculate fuel gas toxics (default: %(default)s).')
    group2.add_argument('--ctoxics',
                        dest='calculate_calciner_toxics', metavar='T/F',
                        default=cf.calculate_calciner_toxics,
                        help='Whether or not to calculate calciner toxics (default: %(default)s).')
    group2.add_argument('--htoxics',
                        dest='calculate_h2plant2_toxics', metavar='T/F',
                        default=cf.calculate_h2plant2_toxics,
                        help='Whether or not to calculate toxics for WED Pt. #46 H2 Plant #2 (default: %(default)s).')
                        
    group3 = parser.add_argument_group('Console Output / QA')
    # maybe change verbosity options; this may be confusing
    group3.add_argument('-q', '--quiet',
					    action='store_true',
					    help='Suppress verbose console logging.')
    group3.add_argument('-v', '--view_config',
					    action='store_true',
					    help='Only view configuration parameters; do not parse.')

    args = parser.parse_args()
    #parser.print_help()

    return args

def update_config(dst, src):
    """generic function to update attributes in module"""
    for key, val in src.items():
        setattr(dst, key, val)

def append_slash_to_dir(di, kys):
    """Append forward slash to dict value (if necessary) to create directory."""
    for ky in kys:
        if not di[ky].endswith('/'):
            di[ky] += '/'

if __name__ == '__main__':
    main()
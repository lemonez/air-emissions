# global import external libraries
import pandas as pd

# module-level imports
import config as cf

def main():
    """main script controller function"""
    import os, time

    # unpack arguments as dict and push back to config module
    args_dict = vars(get_args())

    update_config(cf, args_dict)

    if not cf.quiet:
        print('\nConfiguration options specified:\n')
        for k,v in args_dict.items():
            print('    {:<22}: {}'.format(k,v))
        print('\n')

    if cf.view_config:
        import sys
        sys.exit(0)

    # print start timestamp for checking script runtime
    start_time_seconds = time.time()
    start_time = time.strftime("%H:%M:%S")
    print('\n'+start_time+'\tmodule \''+__name__+'\' began running.')
    
    # ensure output directories exist
    for dir in [cf.out_dir, cf.log_dir]:
        if not os.path.exists(dir):
            os.makedirs(dir)
            print('Created directory \''+dir+'\' for output files.\n')
    
    # parse input, calculate, write output
    from parser import AnnualParser
    ap = AnnualParser()
    ap.read_and_write_all()
    
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
                        dest='data_dir', metavar='InDataDir',
                        default=cf.data_dir,
                        help='Path to data (default: \'%(default)s\'.')
    group1.add_argument('-o', '--outpath',
                        dest='out_dir', metavar='OutDataDir',
                        default=cf.out_dir,
                        help='Path to save output (default: \'%(default)s\').')
    group1.add_argument('-L', '--logpath',
                        dest='log_dir', metavar='LogDir',
                        default=cf.log_dir,
                        help='Path to save logfiles (default: \'%(default)s\').')
    group1.add_argument('-x', '--logsuffix',
                        dest='log_suffix', metavar='LogSuf',
                        default=cf.log_suffix,
                        help='Suffix to append to logfile names (default: \'%(default)s\').')

    group2 = parser.add_argument_group('Data / Cloud Options')
    group2.add_argument('-e', '--equip',
                        dest='equips_to_calculate', metavar='Equips',
                        default=cf.equips_to_calculate,
                        help='Equipment units to calculate (default: %(default)s).')
    group2.add_argument('-y', '--year',
                        dest='data_year', metavar='DataYear',
                        default=cf.data_year,
                        help='Year at end of met dataset (default: %(default)s).')
    group2.add_argument('-m', '--months',
                        dest='months_to_calculate', metavar='Months',
                        default=cf.months_to_calculate,
                        help='Months of data to parse(default: %(default)s).')
                        
    group3 = parser.add_argument_group('Console Output / QA')
    # maybe change verbosity options; this may be confusing
    group3.add_argument('-q', '--quiet',
					    action='store_true',
#                        default=cf.verbose_logging, # currently only specified at CL
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

if __name__ == '__main__':
    main()
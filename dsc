#!/usr/bin/python3

import argparse
import sys
import os
import multiprocessing

from conversiontask import Worker, ConversionTask


def scan_dir(dir):
    """Return list of files in the given directory."""
    files = []
    zipfiles = []
    if not os.path.exists(dir):
        sys.stdout.write('Directory "%s" does not exist! ' % (dir))
        return []

    for (dirpath, dirnames, filenames) in os.walk(dir):
        files.extend(filenames)
        break

    # only return files with .zip extension
    for f in files:
        if f.endswith('.zip'):
            zipfiles.append(f)

    return zipfiles


def main():
    parser = argparse.ArgumentParser(prog='dsc',
                                     description='BEEDeL dataset converter: Converts .zip datasets to CSV or MAT files',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('in_dir', metavar='<in dir>',
                        help='directory containing the zip files')
    parser.add_argument('out_dir', metavar='<out dir>',
                        help='directory to which the output files will be saved')
    parser.add_argument('-m','--move_dir', metavar='<move dir>',
                        help='directory to which the processed dataset files will be moved')
    parser.add_argument('-l', '--logger', metavar='<logger>', default='0',
                        help='logger number for the input files')
    parser.add_argument('-o', '--format', metavar='<out format>', choices=['csv', 'mat'], default='csv',
                        help='output format: "csv" or "mat" is accepted')
    parser.add_argument('-d', '--dbstore', action='store_true', default=False, dest='db_toggle',
                        help='insert data into database')
    parser.add_argument('-i', '--dbip', metavar='<db ip', action='store', default='192.168.10.1', dest='db_ip',
                        help='IP address of the database')
    parser.add_argument('-p', '--dbport', metavar='<db port>', action='store', default='27017', dest='db_port',
                        help='port for the database')
    parser.add_argument('-n', '--dbname', metavar='<db name>', action='store', default='beedel_data', dest='db_name',
                        help='name of the database to store the data in')
    args = parser.parse_args()

    if args.move_dir is None:
        args.move_dir = args.in_dir

    print(args)

    input_dir = args.in_dir
    output_dir = args.out_dir
    processed_dir = args.move_dir
    logger = args.logger
    out_format = args.format

    db_settings = {}
    db_settings['enabled'] = args.db_toggle
    db_settings['ip'] = args.db_ip
    db_settings['port'] = int(args.db_port)
    db_settings['database'] = str(args.db_name);

    sys.stdout.write('-------------------------------------------------------------\n')
    sys.stdout.write('BEEDeL dataset converter\n')
    sys.stdout.write('-------------------------------------------------------------\n')
    sys.stdout.write('input dir:     %s\n' % (input_dir))
    sys.stdout.write('output dir:    %s\n' % (output_dir))
    sys.stdout.write('output format: %s\n' % (out_format))
    sys.stdout.write('logger number: %s\n' % (logger))
    sys.stdout.write('store in DB:   %s\n\n' % (db_settings['enabled']))

    # scan input directory
    files = scan_dir(input_dir)
    if len(files) is 0:
        sys.stdout.write('No input files! Exiting\n')
        return 1

    # create output and processed directories
    if not os.path.exists(processed_dir):
        os.mkdir(processed_dir)
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    # Establish queue
    tasks = multiprocessing.JoinableQueue()

    # Start workers
    if len(files) < multiprocessing.cpu_count():
        num_workers = len(files)
    else:
        num_workers = multiprocessing.cpu_count()

    print('Starting %d workers' % num_workers)
    workers = [Worker(tasks) for i in range(num_workers)]
    for w in workers:
        w.start()

    # Enqueue jobs
    for file in files:
        tasks.put(
            ConversionTask(input_file=input_dir+'/'+file,
                           output_dir=output_dir,
                           processed_dir=processed_dir,
                           extract_dir='tmp-'+file,
                           output_format=out_format,
                           logger=logger,
                           db_settings=db_settings)
        )

    # Add a poison pill for each worker
    for i in range(num_workers):
        tasks.put(None)

    # Wait for all of the tasks to finish
    tasks.join()

    sys.stdout.write('done\n')

    return 0

if __name__ == "__main__":
    try:
        ret = main()
    except Exception:
        ret = 1
        import traceback
        traceback.print_exc(5)
    sys.exit(ret)

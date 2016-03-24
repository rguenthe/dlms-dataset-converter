#!/usr/bin/python3

import argparse
import sys
import os
import multiprocessing

from conversiontask import Worker, ConversionTask


def scan_dir(dir):
    """Return list of files in the given directory."""
    files = []
    if not os.path.exists(dir):
        raise OSError('Directory "%s" does not exist!' % (dir))

    for (dirpath, dirnames, filenames) in os.walk(dir):
        files.extend(filenames)
        break

    return files


def main():
    parser = argparse.ArgumentParser(prog='dsc',
                                     description='DLM dataset converter: Converts .zip datasets to Json/Xml/Csv',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('input', metavar='<in dir>', help='directory containing the zip files')
    parser.add_argument('output', metavar='<out dir>', help='directory to which the output files will be saved')
    parser.add_argument('processed', metavar='<processed dir>',
                        help='directory to which the processed dataset files will be moved')
    parser.add_argument('format', metavar='<out format>', choices=['json', 'xml', 'csv'],
                        help='output format (json or xml)')
    parser.add_argument('--serial', metavar='<serial>', help='serial number of the client of the datasets',
                        default='123456')
    args = parser.parse_args()

    input_dir = args.input
    output_dir = args.output
    processed_dir = args.processed
    serial = args.serial
    out_format = args.format

    sys.stdout.write('-------------------------------------------------------------\n')
    sys.stdout.write('DLM dataset converter\n')
    sys.stdout.write('-------------------------------------------------------------\n')
    sys.stdout.write('input dir:     %s\n' % (input_dir))
    sys.stdout.write('output dir:    %s\n' % (output_dir))
    sys.stdout.write('output format: %s\n\n' % (out_format))

    # scan input dir
    files = scan_dir(input_dir)
    if len(files) is 0:
        sys.stdout.write('No input files. Exiting\n')
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
                           serial=serial)
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

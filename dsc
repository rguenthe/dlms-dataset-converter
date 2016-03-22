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
    parser = argparse.ArgumentParser(prog='dataset2json',
                                     description='Dataset to Json/Xml Converter: Converts .zip datasets to Json/Xml datasets',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('input', metavar='<in dir>', help='directory containing the zip files')
    parser.add_argument('output', metavar='<out dir>', help='directory to which the output files will be saved')
    parser.add_argument('format', metavar='<out format>', choices=['json', 'xml', 'csv'],
                        help='output format (json or xml)')
    parser.add_argument('--serial', metavar='<serial>', help='serial number of the client of the datasets',
                        default='123456')
    args = parser.parse_args()

    input_dir = args.input
    output_dir = args.output
    serial = args.serial
    out_format = args.format

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    sys.stdout.write('-------------------------------------------------------------\n')
    sys.stdout.write('DLM dataset converter\n')
    sys.stdout.write('-------------------------------------------------------------\n')
    sys.stdout.write('input dir:     %s\n' % (input_dir))
    sys.stdout.write('output dir:    %s\n' % (output_dir))
    sys.stdout.write('output format: %s\n\n' % (out_format))

    # Establish communication queues
    tasks = multiprocessing.JoinableQueue()

    # Start workers
    num_workers = multiprocessing.cpu_count()
    print('Creating %d workers' % num_workers)
    workers = [Worker(tasks) for i in range(num_workers)]
    for w in workers:
        w.start()

    # Enqueue jobs
    files = scan_dir(input_dir)
    for file in files:
        tasks.put(
            ConversionTask(input_file=input_dir+'/'+file,
                           output_dir=output_dir,
                           output_format=out_format,
                           extract_dir='tmp-'+file,
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

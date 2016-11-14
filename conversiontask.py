import multiprocessing
import zipfile
import shutil
import os.path
import time

from dataconverter import DataConverter
from databaseconnector import MongoDBConnector


class ConversionTask(object):

    def __init__(self, input_file, output_dir, processed_dir, extract_dir, output_format, logger='0', db_settings={'enabled':False}):
        self.input_file = input_file
        self.output_dir = output_dir
        self.processed_dir = processed_dir
        self.extract_dir = extract_dir
        self.output_format = output_format
        self.logger = logger
        self.db_settings = db_settings

    def __call__(self):
        start_time = time.time()
        try:
            zf = zipfile.ZipFile(self.input_file, 'r')
            zf.extractall(self.extract_dir)
            zf.close()

            # data conversion start
            converter = DataConverter(in_dir=self.extract_dir,
                                      out_dir=self.output_dir,
                                      zipfilename=os.path.basename(self.input_file),
                                      logger=self.logger)
            data = converter.run(output_format=self.output_format)

            # insert in database if settings were provided (IP and Port)
            if self.db_settings['enabled'] is True:
                dbconnection = MongoDBConnector(self.db_settings['ip'], self.db_settings['port'], self.db_settings['database'])
                dbconnection.insert_dataset(data)

            shutil.rmtree(self.extract_dir)
            shutil.move(self.input_file, self.processed_dir + '/' + os.path.basename(self.input_file))
        except Exception as err:
            print('  error: executing conversion task of %s failed: %s' %(os.path.basename(self.input_file), err))
            return

        end_time = time.time()
        print('converted %s to %s (%s sec)' % (os.path.basename(self.input_file), self.output_format, round((end_time - start_time), 2)))
        return


class Worker(multiprocessing.Process):

    def __init__(self, task_queue):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue

    def run(self):
        while True:
            next_task = self.task_queue.get()
            if next_task is None:
                # Poison pill means shutdown
                self.task_queue.task_done()
                break
            next_task()
            self.task_queue.task_done()
        return

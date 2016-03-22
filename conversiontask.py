import multiprocessing
import zipfile
import shutil
import os.path

from dataconverter import DataConverter


class ConversionTask(object):

    def __init__(self, input_file, output_dir, output_format, extract_dir, serial='0'):
        self.input_file = input_file
        self.output_dir = output_dir
        self.output_format = output_format
        self.serial = serial
        self.processed_dir = 'processed'
        self.extract_dir = extract_dir

    def __call__(self):
        zf = zipfile.ZipFile(self.input_file, 'r')
        zf.extractall(self.extract_dir)
        zf.close()

        converter = DataConverter(in_dir=self.extract_dir, out_dir=self.output_dir, zipfilename=os.path.basename(self.input_file), serial=self.serial)
        converter.run(output_format=self.output_format)

        shutil.rmtree(self.extract_dir)
        shutil.move(self.input_file, self.processed_dir + '/' + os.path.basename(self.input_file))

        return


class Worker(multiprocessing.Process):

    def __init__(self, task_queue):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue

    def run(self):
        proc_name = self.name
        while True:
            next_task = self.task_queue.get()
            if next_task is None:
                # Poison pill means shutdown
                print('%s: Exiting' % proc_name)
                self.task_queue.task_done()
                break
            next_task()
            self.task_queue.task_done()
        return

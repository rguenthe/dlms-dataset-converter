import scipy.io as sio
import csv


class CsvGenerator(object):
    """generate sorted CSV file."""

    def __init__(self, filename):
        self.filename = filename
        self.fp = open(self.filename, 'w')

    def write_data(self, data):
        fieldnames = [key for (key, value) in sorted(data[0].items())]

        csv_writer = csv.DictWriter(self.fp, fieldnames=fieldnames, quoting=csv.QUOTE_NONE, lineterminator='\n')
        csv_writer.writeheader()

        for datapoint in data:
            csv_writer.writerow(datapoint)

    def finish(self):
        self.fp.close()


class MatGenerator(object):
    """generate MATLAB compatible file."""

    def __init__(self, filename):
        self.filename = filename

    @staticmethod
    def __convert_to_mval(key, value):
        # convert all strings to float to get 'double' type in mat file (except time)
        if key is 'time':
            return value
        else:
            return float(value)

    def write_data(self, data):

        # convert list of datapoint dicts to dict containing value lists
        mdict = dict()
        fieldnames = [key for (key, value) in sorted(data[1].items())]
        for key in fieldnames:
            mdict[key] = list()

        for datapoint in data:
            for (key, value) in datapoint.items():
                mval = self.__convert_to_mval(key, value)
                mdict[key].append(mval)

        # generate mat file
        sio.savemat(self.filename, mdict, oned_as='column')

    def finish(self):
        pass


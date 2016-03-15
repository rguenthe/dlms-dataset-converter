import struct
import numpy
import time

from progressbar import ProgressBar
from filegenerator import XmlGenerator, JsonGenerator


def read_binary_file(file, type='d'):
    """Return content of binary file using double or integer type intepretation."""
    fp = open(file, 'rb')
    data = fp.read()
    if type is 'i':
        num_bytes = int(len(data) / 4)
    elif type is 'd':
        num_bytes = int(len(data) / 8)
    else:
        raise ValueError('type must be either i or d!')

    tuple = struct.unpack(str(num_bytes) + type, data)
    fp.close()

    return tuple


class DataConverter(object):

    def __init__(self, in_dir, out_dir, zipfilename=''):
        self.in_dir = in_dir
        self.out_dir = out_dir
        self.zipfilename = zipfilename
        self.output_prefix = out_dir + '/' + 'dataset_'
        self.files = {
            'ACC': 'ACC.txt',
            'GYR': 'GYR.txt',
            'MAG': 'MAG.txt',
            'PR_TE': 'PR_TE.txt',
            'GPS': 'GPS_GGA.nmea',
            'STOP': 'Halt.txt',
            'TACHO': 'Tacho.bin',   # int
            'TIME': 'Time.bin',     # double
            'POINTS': 'POINTS.bin'  # int
        }
        self.datapoints = self.get_points() - 1
        self.starttime_unix = self.get_starttime()
        self.progress = ProgressBar(self.datapoints, zipfilename)

    def get_points(self):
        """Return number of datapointlist in the current dataset."""
        points_tuple = read_binary_file(self.in_dir + '/' + self.files['POINTS'], 'i')
        datapointlist = points_tuple[0]
        return datapointlist

    def get_starttime(self):
        """Return start time for the dataset as string."""
        time_tuple = read_binary_file(self.in_dir + '/' + self.files['TIME'], 'd')
        # YYYY-MM-DD HH-MM-SS.MS
        starttime = str(int(time_tuple[0])) + '-' + \
                    str(int(time_tuple[1])) + '-' + \
                    str(int(time_tuple[2])) + ' ' + \
                    str(int(time_tuple[3])) + ':' + \
                    str(int(time_tuple[4])) + ':' + \
                    str(int(time_tuple[5]))

        # convert to unix time           
        time_struct = time.strptime(starttime, '%Y-%m-%d %H:%M:%S')
        starttime_unix = time.mktime(time_struct)

        return starttime_unix

    def get_increased_time(self, deci_sec=0):
        """Return time that is increased by X deci seconds starting from the start time of the dataset."""
        decis_remain = int(deci_sec % 10)
        #decis -= decis_remain
        seconds = int(deci_sec / 10)

        time_unix = self.starttime_unix + seconds
        time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time_unix))
        time_str = time_str + '.' + str(decis_remain)

        return time_str

    def get_gps_data(self, fp):
        """Return one line of 'GPGGA' and 'GPRMC' NMEA strings in the given file."""
        gps_data = {}

        line = fp.readline()
        while not b'$GPGGA' in line:
            line = fp.readline()

        gpgga_str = line.decode("utf-8")
        line = fp.readline()
        gprmc_str = line.decode("utf-8")

        gps_data['GPGGA'] = gpgga_str.split(',')
        gps_data['GPRMC'] = gprmc_str.split(',')

        return gps_data

    def run(self, output_format):
        """Return keys (string, comma delimited) and list of values (strings, comma delimited) of the dataset."""
        filename = self.output_prefix + self.zipfilename.replace('zip', output_format)
        datapointlist = []

        self.progress.start()

        if output_format == 'json':
            file_gen = JsonGenerator(filename, root='datapoint')
        elif output_format == 'xml':
            file_gen = XmlGenerator(filename, root='datapoint')
        else:
            print('unknown output format "%s"' %(output_format))
            return 1

        file_gen.start()

        fp_ACC = open(self.in_dir + '/' + self.files['ACC'])
        fp_GYR = open(self.in_dir + '/' + self.files['GYR'])
        fp_MAG = open(self.in_dir + '/' + self.files['MAG'])
        fp_PR_TE = open(self.in_dir + '/' + self.files['PR_TE'])
        fp_STOP = open(self.in_dir + '/' + self.files['STOP'])
        fp_GPS = open(self.in_dir + '/' + self.files['GPS'], 'rb')

        tacho_data = read_binary_file(self.in_dir + '/' + self.files['TACHO'], 'i')
        speed_data = numpy.diff(tacho_data)
        speed_data = speed_data * 9 # 10Hz sampling, 4 ticks/m, m/s->km/h

        # get one complete datapoint and write it to the file
        for i in range(0, self.datapoints):

            self.progress.update(i)
            data = dict()

            data['time'] = self.get_increased_time(i)
            data['tacho'] = str(tacho_data[i])
            data['speed'] = str(speed_data[i])
            data['height'] = '0'

            gps_data = self.get_gps_data(fp_GPS)
            data['direction'] = gps_data['GPRMC'][8]
            data['gps_y'] = gps_data['GPGGA'][2]
            data['gps_x'] = gps_data['GPGGA'][4]
            data['sattelites'] = gps_data['GPGGA'][7]

            acc_data = fp_ACC.readline().split(';')
            data['acc_x'] = acc_data[0].replace(' ','').strip()
            data['acc_y'] = acc_data[1].replace(' ','').strip()
            data['acc_z'] = acc_data[2].replace(' ','').strip()

            gyr_data = fp_GYR.readline().split(';')
            data['gyr_x'] = gyr_data[0].replace(' ','').strip()
            data['gyr_y'] = gyr_data[1].replace(' ','').strip()
            data['gyr_z'] = gyr_data[2].replace(' ','').strip()

            mag_data = fp_MAG.readline().split(';')
            data['mag_x'] = mag_data[0].replace(' ','').strip()
            data['mag_y'] = mag_data[1].replace(' ','').strip()
            data['mag_z'] = mag_data[2].replace(' ','').strip()

            prte_data = fp_PR_TE.readline().split(';')
            data['temperature'] = prte_data[0].replace(' ','').strip()
            data['pressure'] = prte_data[1].replace(' ','').strip()

            stop_data = fp_STOP.readline().replace(' ','').strip()
            data['stop'] = str(stop_data)

            # generate file on-the-fly
            file_gen.add_entry_list(dict=data)

            if i < (self.datapoints - 1):
                file_gen.add_separator()

        fp_GPS.close()
        fp_ACC.close()
        fp_GYR.close()
        fp_MAG.close()
        fp_PR_TE.close()
        fp_STOP.close()

        file_gen.finish()
        self.progress.finish()

        return datapointlist

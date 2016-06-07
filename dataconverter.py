import struct
import time

import numpy

from progressbar import ProgressBar
from filegenerator import CsvGenerator


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

    filecontent = struct.unpack(str(num_bytes) + type, data)
    fp.close()

    return filecontent
    
def is_number(s):
    try:
        float(s)
        return True
    except Exception:
        return False


class DataConverter(object):

    def __init__(self, in_dir, out_dir, zipfilename='', serial='00'):
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
        self.serial = serial

    def get_points(self):
        """Return number of datapoints in the current dataset."""
        points_tuple = read_binary_file(self.in_dir + '/' + self.files['POINTS'], 'i')
        if points_tuple is not ():
            datapoints = points_tuple[0]
        else:
            # Fallback: estimate number of datapoints by reading number of lines in the tacho file
            data = read_binary_file(self.in_dir + '/' + self.files['TACHO'], 'i')
            datapoints = 0
            for line in data:
                datapoints += 1

        return datapoints

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
        seconds = int(deci_sec / 10)

        time_unix = self.starttime_unix + seconds
        time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time_unix))
        time_str = time_str + '.' + str(decis_remain)

        return time_str

    def get_gps_data(self, fp):
        """Return one line of 'GPGGA' and 'GPRMC' NMEA strings in the given file."""
        gps_data = {}
        zero_list = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

        try:
            line = fp.readline()
            while b'$GPGGA' not in line and line is not b'':
                line = fp.readline()

            gpgga_str = line.decode("utf-8")
            line = fp.readline()
            gprmc_str = line.decode("utf-8")

            gps_data['GPGGA'] = gpgga_str.split(',')
            gps_data['GPRMC'] = gprmc_str.split(',')

            if len(gps_data['GPGGA']) < 10:
                gps_data['GPGGA'] = zero_list
            if len(gps_data['GPRMC']) < 10:
                gps_data['GPRMC'] = zero_list

        except Exception as err:
            print('\nget_gps_data: Error while reading gps data: %s' %(err))
            gps_data['GPGGA'] = zero_list
            gps_data['GPRMC'] = zero_list

        return gps_data

    def run(self, output_format):
        """Return keys (string, comma delimited) and list of values (strings, comma delimited) of the dataset."""
        filename = self.output_prefix + self.zipfilename.replace('zip', output_format)
        datapointlist = []

        self.progress.start()

        if output_format == 'csv':
            file_gen = CsvGenerator(filename)
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

        try:
            tacho_data = read_binary_file(self.in_dir + '/' + self.files['TACHO'], 'i')
            speed_data = numpy.diff(tacho_data)

            # get one complete datapoint and write it to the file
            for i in range(0, self.datapoints):

                self.progress.update(i)
                data = dict()

                data['serial'] = self.serial

                data['time'] = self.get_increased_time(i)
                data['tacho'] = str(tacho_data[i])
                data['speed'] = str(speed_data[i])
                data['height'] = '0'

                gps_data = self.get_gps_data(fp_GPS)
                gps_direction = gps_data['GPRMC'][8]
                gps_lat = gps_data['GPGGA'][2]
                gps_long = gps_data['GPGGA'][4]
                gps_sat = gps_data['GPGGA'][7]
                
                # check if all values are numeric
                if is_number(gps_direction) and is_number(gps_lat) and is_number(gps_long) and is_number(gps_sat):
                    data['direction'] = gps_direction
                    data['gps_lat'] = gps_lat
                    data['gps_long'] = gps_long
                    data['satellites'] = gps_sat
                else:
                    data['direction'] = '0'
                    data['gps_lat'] = '0'
                    data['gps_long'] = '0'
                    data['satellites'] = '0'

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
                file_gen.add_entry_list(data=data)

                if i < (self.datapoints - 1):
                    file_gen.add_separator()
        except Exception as err:
            print('\nrun: Error while gathering data: %s' %(err))

        fp_GPS.close()
        fp_ACC.close()
        fp_GYR.close()
        fp_MAG.close()
        fp_PR_TE.close()
        fp_STOP.close()

        file_gen.finish()
        self.progress.finish()

        return datapointlist

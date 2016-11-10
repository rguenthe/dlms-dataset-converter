import struct
import time
import csv

import numpy

from filegenerator import CsvGenerator, MatGenerator


def read_binary_file(file, type='d'):
    """Return content of binary file using double or integer type interpretation."""
    fp = open(file, 'rb')
    data = fp.read()
    if type is 'i': # integer
        num_bytes = int(len(data) / 4)
    elif type is 'd': # double
        num_bytes = int(len(data) / 8)
    else:
        raise ValueError('type must be either i or d!')

    filecontent = struct.unpack(str(num_bytes) + type, data)
    fp.close()

    return filecontent


def degree_to_decimal(input_degree):
    """convert coordinates in degree/minute/second to decimal degree"""
    degree_float = float(input_degree)
    degree = int(degree_float/100)
    minutes = degree_float - degree*100
    decimal = degree + minutes/60

    return round(decimal, 6)


def is_number(s):
    try:
        float(s)
        return True
    except Exception:
        return False


class DataConverter(object):

    def __init__(self, in_dir, out_dir, zipfilename='', logger='0'):
        self.in_dir = in_dir
        self.out_dir = out_dir
        self.zipfilename = zipfilename
        self.output_prefix = out_dir + '/' + 'dataset_'
        self.src_files = {
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
        self.datapoints = self.get_points()
        self.starttime_unix = self.get_starttime()
        self.logger = logger
        self.busnumfile = 'busnum.csv'

    def get_points(self):
        """Return number of datapoints in the current dataset."""
        points_tuple = read_binary_file(self.in_dir + '/' + self.src_files['POINTS'], 'i')
        if points_tuple is not ():
            datapoints = points_tuple[0]
        else:
            # Fallback: estimate number of datapoints by reading number of lines in the tacho file
            data = read_binary_file(self.in_dir + '/' + self.src_files['TACHO'], 'i')
            datapoints = 0
            for line in data:
                datapoints += 1

        return datapoints

    def get_starttime(self):
        """Return start time for the dataset as string."""
        time_tuple = read_binary_file(self.in_dir + '/' + self.src_files['TIME'], 'd')
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

    def get_timestamp(self, deci_sec=0, type='str'):
        """Return time that is increased by X deci seconds starting from the start time of the dataset."""
        decis_remain = int(deci_sec % 10)
        seconds = int(deci_sec / 10)

        time_unix = self.starttime_unix + seconds
        time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time_unix))
        time_str = time_str + '.' + str(decis_remain)

        if type is 'unix':
            return int(time_unix)
        else:
            return time_str

    def get_gps_data(self, fp):
        """Return one line of 'GPGGA' and 'GPRMC' NMEA strings in the given file."""
        gps_data = {}
        gpgga_str_zero = '$GPGGA, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0'
        gprmc_str_zero = '$GPRMC, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0'

        try:
            # read until 'GPGGA' is found
            line = fp.readline()
            while b'$GPGGA' not in line and line is not b'':
                line = fp.readline()

            # read two lines: GPGGA and GPRMC
            gpgga_str = line.decode("utf-8")
            line = fp.readline()
            gprmc_str = line.decode("utf-8")

        except Exception as err:
            print('  error: could not read gps data from %s: %s' % (self.zipfilename, err))
            gpgga_str = gpgga_str_zero
            gprmc_str = gprmc_str_zero

        gps_data['GPGGA'] = gpgga_str.split(',')
        gps_data['GPRMC'] = gprmc_str.split(',')

        # sanity checks on NMEA strings
        # GPGGA
        gga_sanity_checks = dict()
        if len(gps_data['GPGGA']) is 15:
            gga_sanity_checks['gga_time'] = is_number(gps_data['GPGGA'][1])
            gga_sanity_checks['gga_lat'] = is_number(gps_data['GPGGA'][2])
            gga_sanity_checks['gga_lon'] = is_number(gps_data['GPGGA'][4])
            gga_sanity_checks['gga_fix'] = is_number(gps_data['GPGGA'][6])
            gga_sanity_checks['gga_sat'] = is_number(gps_data['GPGGA'][7])
            gga_sanity_checks['gga_hdop'] = is_number(gps_data['GPGGA'][8])
            gga_sanity_checks['gga_alt'] = is_number(gps_data['GPGGA'][9])
            gga_sanity_checks['gga_geoid_sep'] = is_number(gps_data['GPGGA'][11])
        else:
            gga_sanity_checks['str_length'] = False

        if False in gga_sanity_checks.values():
            print('  warning: corrupt GPGGA string in %s: %s' % (self.zipfilename, gpgga_str.strip('\r\n')))
            gps_data['GPGGA'] = gpgga_str_zero.split(',')

        # GPRMC
        rmc_sanity_checks = dict()
        if len(gps_data['GPRMC']) is 13:
            rmc_sanity_checks['rmc_speed'] = is_number(gps_data['GPRMC'][7])
            rmc_sanity_checks['rmc_dir'] = is_number(gps_data['GPRMC'][8])
        else:
            rmc_sanity_checks['str_length'] = False

        if False in rmc_sanity_checks.values():
            print('  warning: corrupt GPRMC string in %s: %s' % (self.zipfilename, gprmc_str.strip('\r\n')))
            gps_data['GPRMC'] = gprmc_str_zero.split(',')

        return gps_data

    def get_busnum(self):
        """read bus number from csv file"""

        dataset_date = self.zipfilename[0:8]
        logger = 'logger%s' %(self.logger)

        fp_busnum = open(self.busnumfile)
        reader = csv.DictReader(fp_busnum)
        for line in reader:
            if line['date'] == dataset_date:
                return line[logger]

        return '0'

    def run(self, output_format):
        """Convert the dataset into the given output format"""
        i=0
        datapointlist = list()

        try:
            fp_ACC = open(self.in_dir + '/' + self.src_files['ACC'])
            fp_GYR = open(self.in_dir + '/' + self.src_files['GYR'])
            fp_MAG = open(self.in_dir + '/' + self.src_files['MAG'])
            fp_PR_TE = open(self.in_dir + '/' + self.src_files['PR_TE'])
            fp_DOOR = open(self.in_dir + '/' + self.src_files['STOP'])
            fp_GPS = open(self.in_dir + '/' + self.src_files['GPS'], 'rb')

            tacho_data = read_binary_file(self.in_dir + '/' + self.src_files['TACHO'], 'i')
            speed_ndarray = numpy.diff(tacho_data) * 10 / 4     # conversion to m/s
            speed_data = speed_ndarray.tolist()                 # convert ndarray to list
            speed_data.append(0)                                # add 0 at end to match vector lengths
            busnum_data = self.get_busnum()

            # collect data for one complete datapoint
            for i in range(0, self.datapoints):

                data = dict()

                data['log_num'] = self.logger
                data['bus_num'] = busnum_data

                data['abs_time'] = self.get_timestamp(i)
                data['unix_time'] = self.get_timestamp(i, type='unix')

                data['tacho'] = str(tacho_data[i])
                data['speed'] = str(speed_data[i])

                gps_data = self.get_gps_data(fp_GPS)
                data['gps_direction'] = gps_data['GPRMC'][8]
                data['gps_speed'] = gps_data['GPRMC'][7]
                data['gps_lat'] = degree_to_decimal(gps_data['GPGGA'][2])
                data['gps_lon'] = degree_to_decimal(gps_data['GPGGA'][4])
                data['gps_satellites'] = gps_data['GPGGA'][7]
                data['gps_altitude'] = gps_data['GPGGA'][9]
                data['gps_hdop'] = gps_data['GPGGA'][8]
                data['gps_geoid_separation'] = gps_data['GPGGA'][8]
                data['gps_fix_quality'] = gps_data['GPGGA'][6]

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

                door_data = fp_DOOR.readline().replace(' ','').strip()
                data['door'] = str(door_data)

                datapointlist.append(data)

            fp_GPS.close()
            fp_ACC.close()
            fp_GYR.close()
            fp_MAG.close()
            fp_PR_TE.close()
            fp_DOOR.close()

        except Exception as err:
            print('  error: reading dataset %s failed at point %d: %s' % (self.zipfilename, i, err))

        # generate output file
        filename = self.output_prefix + self.zipfilename.replace('zip', output_format)
        if output_format == 'csv':
            file_gen = CsvGenerator(filename)
        elif output_format == 'mat':
            file_gen = MatGenerator(filename)
        else:
            print('  error: unknown output format "%s"' % (output_format))
            return 1

        file_gen.write_data(datapointlist)
        file_gen.finish()

        return datapointlist

import json
import csv


class CsvGenerator(object):
    """generate sorted CSV file."""

    def __init__(self, filename):
        self.filename = filename
        self.fp = open(self.filename, 'w')
        self.csv_writer = csv.writer(self.fp, quoting=csv.QUOTE_NONE, lineterminator='\n')
        self.first_line_written = False

    def start(self):
        pass

    def add_entry_list(self, data={}):
        if not self.first_line_written:
            self.csv_writer.writerow([key for (key, value) in sorted(data.items())])
            self.first_line_written = True
        self.csv_writer.writerow([value for (key, value) in sorted(data.items())])

    def add_separator(self):
        pass

    def finish(self):
        self.fp.close()

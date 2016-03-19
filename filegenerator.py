import ujson as json
import csv


class CsvGenerator(object):
    """generate sorted CSV file."""

    def __init__(self, filename):
        self.filename = filename
        self.fp = open(self.filename, 'w')
        self.csv_writer = csv.writer(self.fp, quoting=csv.QUOTE_NONE)
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


class XmlGenerator(object):
    """manually generate XML file"""

    def __init__(self, filename, root='dataset'):
        self.filename = filename
        self.root = root
        self.fp = None
        self.custom_first_line = False

    def open_tag(self, tag):
        return str('<' + tag + '>')

    def close_tag(self, tag):
        return str('</' + tag + '>')

    def item(self, key, value):
        return self.open_tag(key) + str(value) + self.close_tag(key)

    def start(self):
        self.fp = open(self.filename, 'w')
        self.fp.write(self.open_tag('dataset'))

    def add_entry_list(self, data={}, last_entry=False):
        self.fp.write(self.open_tag(self.root))
        for key in data.keys():
            self.fp.write(self.item(key,data[key]))
        self.fp.write(self.close_tag(self.root))

    def add_separator(self):
        pass

    def finish(self):
        self.fp.write(self.close_tag('dataset'))
        self.fp.close()


class JsonGenerator(object):
    """manually generate JSON file"""

    def __init__(self, filename, root='dataset'):
        self.filename = filename
        self.root = root
        self.fp = None
        self.custom_first_line = False

    def start(self):
        self.fp = open(self.filename, 'w')
        self.fp.write('{' + json.dumps(self.root) + ':[')

    def add_entry_list(self, data={}):
        self.fp.write(json.dumps(data))

    def add_separator(self):
        self.fp.write(',')

    def finish(self):
        self.fp.write(']}')
        self.fp.close()

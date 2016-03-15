import ujson as json


class XmlGenerator(object):
    """manually generate XML file"""

    def __init__(self, filename, root='dataset'):
        self.filename = filename
        self.root = root
        self.fp = None

    def open_tag(self, tag):
        return str('<' + tag + '>')

    def close_tag(self, tag):
        return str('</' + tag + '>')

    def item(self, key, value):
        return self.open_tag(key) + str(value) + self.close_tag(key)

    def start(self):
        self.fp = open(self.filename, 'w')
        self.fp.write(self.open_tag('dataset'))

    def add_entry_list(self, dict={}, last_entry=False):
        self.fp.write(self.open_tag(self.root))
        for key in dict.keys():
            self.fp.write(self.item(key,dict[key]))
        self.fp.write(self.close_tag(self.root))

    def finish(self):
        self.fp.write(self.close_tag('dataset'))
        self.fp.close()


class JsonGenerator(object):
    """manually generate JSON file"""

    def __init__(self, filename, root='dataset'):
        self.filename = filename
        self.root = root
        self.fp = None

    def start(self):
        self.fp = open(self.filename, 'w')
        self.fp.write('{' + json.dumps(self.root) + ':[')

    def add_entry_list(self, dict={}):
        self.fp.write(json.dumps(dict))

    def add_separator(self):
        self.fp.write(',')

    def finish(self):
        self.fp.write(']}')
        self.fp.close()

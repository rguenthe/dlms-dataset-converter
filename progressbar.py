import sys


class ProgressBar():

    def __init__(self, maxval, info):
        self._pct = 0
        self.maxval = maxval
        self.info = info

    def update(self, value):
        if self.maxval is 0:
            self.maxval = 1
        pct = int((value / self.maxval) * 100.0)
        if self._pct != pct:
            self._pct = pct
            self.display()

    def display(self):
        sys.stdout.write('\r|%-50s| %d%%  %s' % ('#' *int(self._pct/2), self._pct, self.info))
        sys.stdout.flush()

    def start(self):
        self.update(0)

    def finish(self):
        self.update(self.maxval)
        sys.stdout.write(' (' + str(self.maxval) + ' points)\n')

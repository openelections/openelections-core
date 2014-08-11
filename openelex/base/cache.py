import os
import shutil

from openelex import PROJECT_ROOT


class StateCache(object):

    def __init__(self, state):
        self.state = state.lower()
        self.path = os.path.join(PROJECT_ROOT, 'us', self.state, 'cache')
        try:
            os.makedirs(self.path)
        except OSError:
            pass

    @property
    def abspath(self):
        return os.path.abspath(self.path)

    def list_dir(self, datefilter='', full_path=False):
        if full_path:
            filtered = [os.path.join(PROJECT_ROOT, self.path, f)
                        for f in os.listdir(self.path)
                        if datefilter.strip() in f]
        else:
            files = os.listdir(self.path)
            filtered = filter(lambda path: datefilter.strip() in path, files)
        filtered.sort()
        return filtered

    def clear(self, datefilter=''):
        files = self.list_dir(datefilter)

        for f in files:
            try:
                os.remove(os.path.join(self.path, f))
            except OSError:
                shutil.rmtree(os.path.join(self.path, f))

        remaining = self.list_dir()
        print "%s files deleted" % len(files)
        print "%s files still in cache" % len(remaining)

from os.path import exists, join
import urllib


class BaseScraper(urllib.FancyURLOpener):
    """
    Base class for downloading result files.
    Intended to be subclassed in state-specific fetch.py modules.

    Caches resources inside each state directory.

    """

    def __init__(self, state):
        self.state = state

    def run(self):
        msg = "You must implement the %s.run method" % self.__class__.__name__
        raise NotImplementedError(msg)


    def fetch(self, url, fname=None, overwrite=False):
        """Fetch and cache web page or data file

        ARGS

            url - link to download
            fname - file name with relative path. E.g. 20121106_md_general.csvrelative path which will be appended to cache dir)
            overwrite - if True, overwrite cached copy with fresh donwload

        """
        local_file_name = self.standardized_filename(fname)
        if overwrite:
            name, response = self.retrieve(url, local_file_fname)
        else:
            if exists(local_file_name):
                print "File is cached: %s" % local_file_name
            else:
                name, response = self.retrieve(url, local_file_fname)
                print "Added to cache: %s" % local_file_name

    def standardized_filename(self, fname)
        """A standardized, fully qualified path name"""
        self.cache_dir = join((os.getcwd(),'cache'))
        import pdb;pdb.set_trace()
        print self.cache_dir
        return join((self.cache_dir, fname))

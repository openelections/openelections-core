import glob
import os.path

from openelex import COUNTRY_DIR

class ResultFileFinder(object):
    """Discover result files"""

    @classmethod
    def results_dir(cls):
        """
        Retrieve the path where result files are located.

        Returns:
            String containing the path to the directory containing result files.
        """
        return os.path.join(COUNTRY_DIR, 'bakery')

    @classmethod
    def get_filenames(cls, state, datefilter=None, raw=False,
            search_dir=None):
        """
        Retrieve results filenames based on filters.

        Args:
            state: Two-letter state-abbreviation, e.g. NY
            datefilter: Portion of a YYYYMMDD date, e.g. YYYY, YYYYMM, etc.
                Result files will only be published if the date portion of the
                filename matches the date string. Default is to publish all result
                files for the specified state.
            raw: Search for raw result files.  Default is to search for 
                cleaned/standardized result files.
            search_dir: Directory to search for files.  Default is the
                return value of the ``results_dir()`` method.

        Returns:
            A list of strings containing paths to result files matching
            the specified filters.
            
        """
        filenames = []
        extensions = (".csv", ".json")

        if search_dir is None:
            search_dir = self.results_dir()

        for ext in extensions: 
            glob_s = self.build_glob(state, ext=ext, search_dir=search_dir,
                datefilter=datefilter, raw=raw)
            filenames.extend(glob.glob(glob_s))

        return filenames

    @classmethod
    def build_glob(cls, state, search_dir, ext, datefilter=None, raw=False):
        """
        Retrieve a path name string to pass to glob.glob() 

        Args:
            state: Two-letter state-abbreviation, e.g. NY
            datefilter: Portion of a YYYYMMDD date, e.g. YYYY, YYYYMM, etc.
                Return value will only match results if the date portion of the
                filename matches the date string. Default is to match all 
                result files for the specified state.
            raw: Match raw result files.  Default is to match
                cleaned/standardized result files.
            ext: Filename extension of result files, including leading '.'.
                For example, ".csv".
            search_dir: Directory containing result files.  

        Returns:
            A list of strings containing paths to result files matching
            the specified filters.
            
        """
        filename_bits = []

        if datefilter:
            datefilter_bit = datefilter
            if len(datefilter_bit) < 6:
                datefilter_bit += "*"
        else:
            datefilter_bit = "*"
        filename_bits.append(datefilter_bit)

        filename_bits.append(state)
        filename_bits.append('*')

        if raw:
            filename_bits.append('raw')

        filename_glob = "{}".format("__".join(filename_bits)) + ext 
        pathname = os.path.join(search_dir, filename_glob)

        return pathname


class BasePublisher(object):
    """
    Publishes result files to a remote location
    """
    def publish(self, state, datefilter=None, raw=False):
        """
        Publish baked result files
        
        Args:
            state: Two-letter state-abbreviation, e.g. NY
            datefilter: Portion of a YYYYMMDD date, e.g. YYYY, YYYYMM, etc.
                Result files will only be published if the date portion of the
                filename matches the date string. Default is to publish all result
                files for the specified state.
            raw: Publish raw result files.  Default is to publish
                cleaned/standardized result files.
            
        """
        raise NotImplemented("You must implement this method in a subclass")




class GitHubPublisher(BasePublisher):
    """
    Publisher that pushes files to GitHub pages
    """

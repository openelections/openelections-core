import os.path
import urlparse
from zipfile import ZipFile

from openelex.base.fetch import BaseFetcher
from openelex.us.wa.datasource import Datasource

class FetchResults(BaseFetcher):
    def __init__(self):
        super(FetchResults, self).__init__()
        self._fetched = set()
        # We need access to the state datasource to be able to retrieve
        # mappings for a specific URL in the case of zip files since multiple
        # extracted files will come from the same URL.
        self._datasource = Datasource()

    def fetch(self, url, fname=None, overwrite=False):
        # We keep track of URLs we've already fetched in this run since
        # there will be multiple output files mapped to a single zip
        # file.  If we've already fetched this URL, exit early.
        if url in self._fetched:
            return

        if url.endswith('.zip'):
            # Fetch the zip file, using the automatically generated filename
            zip_fname = self._local_zip_file_name(url)
            super(FetchResults, self).fetch(url, zip_fname, overwrite)
            self._extract_zip(url, zip_fname, overwrite)
        else:
            super(FetchResults, self).fetch(url, fname, overwrite)

        self._fetched.add(url)

    def _local_zip_file_name(self, url):
        """
        Return a normalized local file name for a results zip file.

        We don't care too much about the format because we can delete the
        zip file later.
        """
        parsed = urlparse.urlsplit(url)
        fname = parsed.path.split('/')[-1]
        return os.path.join(self.cache.abspath, fname)

    def _extract_zip(self, url, zip_fname=None, overwrite=False, remove=True):
        if zip_fname is None:
            zip_fname =  self._local_zip_file_name(url)

        with ZipFile(zip_fname, 'r') as zipf:
            for mapping in self._datasource.mappings_for_url(url):
                local_file_name = os.path.join(self.cache.abspath,
                    mapping['generated_filename'])
                if overwrite or not os.path.exists(local_file_name):
                    if mapping['parent_zipfile']:
                        # The downloaded ZIP archive contains zip files. We
                        # need to extract the nested zip file.
                        zipf.extract(mapping['parent_zipfile'],
                            self.cache.abspath)
                        parent_zipfile_path = os.path.join(self.cache.abspath,
                            mapping['parent_zipfile'])
                        with ZipFile(parent_zipfile_path, 'r') as parent_zipf:
                            parent_zipf.extract(mapping['raw_extracted_filename'],
                                    self.cache.abspath)
                        if remove:
                            # Remove the parent zipfile
                            os.remove(parent_zipfile_path)

                            parent_zipfile_dir = os.path.dirname(mapping['parent_zipfile'])
                            # If the extracted parent zipfile lives in a
                            # subdirectory, we'll want to remove the directory
                            # as well
                            if parent_zipfile_dir:
                                os.rmdir(os.path.join(self.cache.abspath,
                                    parent_zipfile_dir))
                       
                    else:
                        zipf.extract(mapping['raw_extracted_filename'],
                            self.cache.abspath)
                    extracted_file_name = os.path.join(self.cache.abspath,
                        mapping['raw_extracted_filename'])
                    os.rename(extracted_file_name, local_file_name)
                    print "Added to cache: %s" % local_file_name
                else:
                    print "File is cached: %s" % local_file_name

        if remove:
            os.remove(zip_fname)

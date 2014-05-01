import os
import os.path
import urllib
import urlparse
from zipfile import ZipFile

from bs4 import BeautifulSoup
import requests

from openelex.base.fetch import BaseFetcher
from openelex.us.ar.datasource import Datasource

class FetchResults(BaseFetcher):
    def __init__(self):
        super(FetchResults, self).__init__()
        self._datasource = Datasource()
        self._fetched = set()
        self._results_portal_url = self._datasource.RESULTS_PORTAL_URL

    def fetch(self, url, fname=None, overwrite=False):
        # We keep track of URLs we've already fetched in this run since
        # there will be multiple output files mapped to a single zip
        # file.  If we've already fetched this URL, exit early.
        if url in self._fetched:
            return

        if url.startswith(self._results_portal_url):
            self._fetch_portal(url, fname, overwrite)
        elif url.endswith('.zip'):
            # Fetch the zip file, using the automatically generated filename
            zip_fname = self._local_zip_file_name(url)
            super(FetchResults, self).fetch(url, zip_fname, overwrite)
            self._extract_zip(url, zip_fname, overwrite)
        else:
            super(FetchResults, self).fetch(url, fname, overwrite)

        self._fetched.add(url)

    def _fetch_portal(self, url, fname, overwrite=False):
        """
        Fetch a results file from the reporting portal.
        """
        local_file_name = os.path.join(self.cache.abspath, fname)
        # The call to the parent class' fetch() method will duplicate the
        # check for the local file, but that's less expensive than building
        # the report URL, since that requires scraping an HTTP request to
        # fetch the form HTML and scraping it.
        if overwrite or not os.path.exists(local_file_name):
            report_url = self._get_report_url(url)
            # Now that we have the URL, delegate to the parent class' fetch
            # to grab the file.
            super(FetchResults, self).fetch(report_url, fname, overwrite)
        else:
            print "File is cached: %s" % local_file_name

    def _get_report_url(self, url):
        """
        Build the download URL for a results file from the election portal.
        """
        query_params = self._get_report_query_params(url)
        qs = urllib.urlencode(query_params)
        return 'http://www.sos.arkansas.gov/electionresults/index.php?' + qs

    def _get_report_query_params(self, url):
        """
        Build the query string parameters to retrieve a results file from the
        election portal.

        Return a list of key, value pairs.
        """
        params = []
        resp = requests.get(url) 
        resp.raise_for_status()
        contests = self._scrape_contests(resp.text)
        # County ids are consecutive integer values
        for county_id in range(1, 76):
            params.append(('counties[]', county_id)) 
        for contest_id, contest_name in contests:
            params.append(('contests[]', contest_id))
        # Show vote counts in report rather than percentages
        params.append(('votes', 'counts'))
        # Include unopposed contests in report
        params.append(('show_unopp', '1'))
        # Show results by polling location
        params.append(('group', 'poll'))
        # Download the file as delimited text rather than output in HTML
        params.append(('DOWNLOAD', '1'))
        params.append(('elecid', self._elec_id(url)))
        params.append(('ac:show:reports:extra:makereport:1', "Create Report"))
        return params

    def _scrape_contests(self, html):
        """
        Scrape the contests from the results portal form.

        Return a list of contest id, office name tuples.
        """
        soup = BeautifulSoup(html)
        return [(o['value'], o.get_text()) for o in soup.select("select#contests option")]

    def _elec_id(self, url):
        """
        Parse reporting portal election ID from the url
        """
        parsed = urlparse.urlsplit(url)
        query_params = urlparse.parse_qs(parsed.query)
        return int(query_params['elecid'][0])

    def _local_zip_file_name(self, url):
        """
        Return a normalized local file name for a results zip file.

        We don't care too much about the format because we can delete the
        zip file later.
        """
        parsed = urlparse.urlsplit(url)
        fname = parsed.path.split('/')[-1]
        return os.path.join(self.cache.abspath, fname)

    def _extract_zip(self, url, zip_fname=None, overwrite=False):
        if zip_fname is None:
            zip_fname =  self._local_zip_file_name(url)

        with ZipFile(zip_fname, 'r') as zipf:
            for mapping in self._datasource.mappings_for_url(url):
                local_file_name = os.path.join(self.cache.abspath,
                    mapping['generated_filename'])
                if overwrite or not os.path.exists(local_file_name):
                    zipf.extract(mapping['raw_extracted_filename'],
                        self.cache.abspath)
                    extracted_file_name = os.path.join(self.cache.abspath,
                        mapping['raw_extracted_filename'])
                    os.rename(extracted_file_name, local_file_name)
                    print "Added to cache: %s" % local_file_name
                else:
                    print "File is cached: %s" % local_file_name

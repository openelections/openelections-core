import logging
import unicodecsv

from openelex.base.load import BaseLoader
from openelex.lib.text import slugify, ocd_type_id
from openelex.models import RawResult

from .datasource import Datasource

class LoadResults(BaseLoader):
    """
    Loads Florida election results.

    Florida results are provided in tab-delimited text files.
    
    A description of fields is available at 
    https://doe.dos.state.fl.us/elections/resultsarchive/downloadresults.asp?ElectionDate=11/6/2012

    Notes:

    Some elections appear to have multiple rows representing the same data
    e.g. ``20120814__fl__primary.tsv``. 

    Results with an ``OfficeDesc`` value of "U.S. President by Congressional
    District" are by county and congressional district.  The county is in
    the ``CountyName`` field and the district is in the ``Jurs1num`` field.

    The ``CanNameMiddle`` field also includes nicknames, but not in a standard
    format.  Examples include "Anne 'Libby'" and "(Doc)".

    Name suffixes are in the ``CanNameLast`` field, e.g. "Braynon,, II"

    Write-in candidates are identified by a value of "Write-In" in the 
    ``PartyName`` field.

    "No Party Affiliation" is also a possibility.  This is different than
    "Independent Party".

    Some contests force the last names of the governor and lieutenant
    governor into the ``CanNameLast`` and ``CanNameFirst`` fields.
    For these records, the value of ``CanNameMiddle`` is '/'.
 
    """
    datasource = Datasource()

    target_offices = set([
        "U.S. President by Congressional District",
        "President of the United States",
        "United States Senator",
        "United States Representative",
        "State Representative",
        "State Senate",
        "State Senator",
        "Governor",
        "Governor and Lieutenant Governor",
        "Attorney General",
        "Chief Financial Officer",
        "Commissioner of Agriculture",
    ])

    district_offices = set([
        "United States Representative",
        "State Representative",
        "State Senate",
        "State Senator",
    ])

    def load(self):
        with self._file_handle as csvfile:
            results = []
            seen = set()
            self._common_kwargs = self._build_common_election_kwargs()
            reader = unicodecsv.DictReader(csvfile, delimiter='\t',
                encoding='latin-1')
            for row in reader:
                # Skip non-target offices
                if self._skip_row(row): 
                    office_name = row['OfficeDesc'].strip()
                    # Log skipped office names in case we forgot to add them
                    # to our list of target offices.  Ignore long office names
                    # because these are probably ballot initiatives that we
                    # definitely want to ignore
                    if len(office_name) < 100:
                        logging.info("Skipping result for office '%s'" %
                            office_name)
                    continue

                result = self._prep_result(row)
                # Only add non-duplicate results.  This is needed because
                # there are duplicate results in some data files, e.g.
                # 20120814__fl__primary.tsv
                key = self._key(result)
                if not key in seen:
                    results.append(result)
                    seen.add(key)

            RawResult.objects.insert(results)

    def _skip_row(self, row):
        return row['OfficeDesc'].strip() not in self.target_offices

    def _prep_result(self, row):
        """
        Creates a RawResult model instance for a row of data.
        """
        # Copy fields that are common to this source file
        result_kwargs = self._common_kwargs.copy()
        # Extract remaining fields from the row of data
        result_kwargs.update(self._build_contest_kwargs(row))
        result_kwargs.update(self._build_candidate_kwargs(row)) 
        result_kwargs.update(self._build_result_kwargs(row))
        return RawResult(**result_kwargs)

    def _build_contest_kwargs(self, row):
        kwargs = {
            'office': row['OfficeDesc'].strip(),
        }
        if kwargs['office'] in self.district_offices:
            kwargs['district'] = row['Juris1num'].strip()
        return kwargs

    def _build_candidate_kwargs(self, row):
        # TODO: Figure out how/if suffix is stored
        return {
            'family_name': row['CanNameLast'].strip(),
            'given_name': row['CanNameFirst'].strip(),
            'additional_name': row['CanNameMiddle'].strip(),
        }

    def _build_result_kwargs(self, row):
        jurisdiction = row['CountyName'].strip()
        kwargs = {
            'party': row['PartyName'].strip(),
            'jurisdiction': jurisdiction,
            'ocd_id': "{}/county:{}".format(self.mapping['ocd_id'],
                ocd_type_id(jurisdiction)),
            'votes': row['CanVotes'].strip()
        }
        if row['OfficeDesc'].strip() == "U.S. President by Congressional District":
            # Primary results for some contests provide the results
            # by congressional district in each county
            kwargs['reporting_level'] = 'congressional_district_by_county' 
            kwargs['reporting_district'] = row['Juris1num'].strip()
        else:
            kwargs['reporting_level'] = 'county'

        return kwargs

    def _key(self, rawresult):
        """
        Returns a string that uniquely identifies a raw result from a particular
        source.
        """
        bits = [rawresult.contest_slug, rawresult.candidate_slug,
                slugify(rawresult.jurisdiction)]

        if rawresult.district:
            bits.append(rawresult.district)

        try:
            bits.append(rawresult.reporting_district)
        except AttributeError:
            pass

        return '-'.join(bits)

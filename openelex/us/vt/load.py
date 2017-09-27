from __future__ import print_function
from builtins import str
from builtins import object
import re
import csv
import xlrd
import unicodecsv

from datetime import datetime

from openelex.base.load import BaseLoader
from openelex.models import RawResult
from openelex.lib.text import ocd_type_id, slugify
from .datasource import Datasource

"""
Vermont files are all csv.
"""

class LoadResults(object):
    """Entry point for data loading.

    Determines appropriate loader for file and triggers load process.

    """

    def run(self, mapping):
        loader = VTBaseLoader()

        print(("loading: ", mapping))
        loader.run(mapping)

class VTBaseLoader(BaseLoader):
    datasource = Datasource()

    def load(self):
        print((str(datetime.now()), "load begin"))
        results = []
        self._common_kwargs = self._build_common_election_kwargs()

        self._common_kwargs['reporting_level'] = 'precinct' if self.mapping['isPrecinct'] else ''
        with self._file_handle as csvfile:
            reader = unicodecsv.reader(csvfile, delimiter=',', encoding='latin-1')
            readerData = list(reader)
            candListRow = readerData[0]
            if not self._isValidHeaderRow(candListRow):
                print(("Error: Header not valid: ", candListRow))
                return []
            partyAffil = readerData[1]
            kwargs = self._build_common_election_kwargs()
            contest_kwargs = self._build_contest_kwargs(kwargs['primary_type'])
            for row in readerData[2:]:
                if self._skip_row(row):
                    continue
                cityLocation = row[0]
                wardLocation = row[1]
                precinctLocation = row[2]

                jurisdiction_kwargs = {}
                if self.mapping['isPrecinct']:
                    jurisdiction_kwargs = self._generatePrecinctJurisdiction(cityLocation, precinctLocation)
                else:
                    jurisdiction_kwargs = self._generateParrishJurisdiction(cityLocation)

                for colInd, res in enumerate(row):
                    if colInd < 3:
                        continue
                    if candListRow[colInd] == "Total Votes Cast":
                        continue
                    if candListRow[colInd] == "No Nomination":
                        continue
                    votes_kwargs = {'votes' : int(res.replace(',', ''))}
                    candidate_kwargs = {}
                    if colInd < len(partyAffil):
                        candidate_kwargs = self._build_candidate_kwargs(candListRow[colInd], partyAffil[colInd])
                        # print (cityLocation, precinctLocation, candListRow[colInd], partyAffil[colInd], res, kwargs)
                    else:
                        candidate_kwargs = self._build_candidate_kwargs(candListRow[colInd], "")
                    curResult = {}
                    curResult.update(kwargs)
                    curResult.update(contest_kwargs)
                    curResult.update(jurisdiction_kwargs)
                    curResult.update(candidate_kwargs)
                    curResult.update(votes_kwargs)
                    results.append(RawResult(**curResult))

        # Store result instances for bulk loading
        if len(results) > 0:
            RawResult.objects.insert(results, { 'writeConcern': {'w':0, 'j':False}, 'ordered': False })

    def _generatePrecinctJurisdiction(self, cityLocation, precinctLocation):
        jurisdiction_kwargs = {
            'jurisdiction' : precinctLocation,
            'parent_jurisdiction' : cityLocation,
            'ocd_id' : ""
        }
        return jurisdiction_kwargs
    def _generateParrishJurisdiction(self, cityLocation):
        jurisdiction_kwargs = {
            'jurisdiction' : cityLocation,
            'parent_jurisdiction' : "",
            'ocd_id' : ""
        }
        return jurisdiction_kwargs

    def _isValidHeaderRow(self, row):
        #TODO
        return True
    def _skip_row(self, row):
        if row == []:
            return True
        elif row[0] == '' and row[1] == '':
            return True
        elif row[0] == ' ' and row[1] == '':
            return True
        elif row[0].upper() == 'TOTAL':
            return True
        elif row[0].upper() == 'TOTALS':
            return True
        else:
            return False

    def _build_contest_kwargs(self, primary_type):
        kwargs = {
            'office': self.mapping['office'].strip(),
            'district': self.mapping['officeDistrict'].strip(),
        }
        # Add party if it's a primary
        if primary_type != '':
            kwargs['primary_party'] = self.mapping['primaryParty']
        kwargs['reporting_level'] = "precinct" if self.mapping['isPrecinct'] else "parish"
        return kwargs

    def _build_candidate_kwargs(self, candidateName, partyName):
        slug = slugify(candidateName, substitute='-')
        kwargs = {
            'party': partyName,
            'full_name': candidateName,
            #TODO: QUESTION: Do we need this? if so, needs a matching model field on RawResult
            'name_slug': slug,
        }
        return kwargs



#

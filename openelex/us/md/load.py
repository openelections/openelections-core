from os.path import join
import datetime
import re
import unicodecsv

from openelex.base.load import BaseLoader
from openelex.models import RawResult
from openelex.lib.text import slugify
from .datasource import Datasource

class LoadResults(object):
    """Entry point for data loading.

    Determines appropriate loader for file and triggers load process.

    """

    def run(self, mapping):
        election_id = mapping['election']
        if '2002' in election_id:
            loader = MDLoader2002()
        elif '2000' in election_id and 'primary' in election_id:
            loader = MDLoader2000Primary()
        else:
            loader = MDLoaderAfter2002()
        loader.run(mapping)


class MDBaseLoader(BaseLoader):
    target_offices = set([
        'President - Vice Pres',
        'U.S. Senator',
        'U.S. Congress',
        'Representative in Congress',
        'Governor / Lt. Governor',
        'Comptroller',
        'Attorney General',
        'State Senator',
        'House of Delegates',
    ])

    def _skip_row(self, row):
        """
        Should this row be skipped?

        This should be implemented in subclasses.
        """
        return False


    #TODO: QUESTION: Move to BaseLoader?
    def run(self, mapping):
        self.mapping = mapping
        self.source = mapping['generated_filename']
        self.timestamp = datetime.datetime.now()
        self.datasource = Datasource()
        self.election_id = mapping['election']

        self.delete_previously_loaded()
        self.load()

    #TODO: QUESTION: Move to BaseLoader?
    def delete_previously_loaded(self):
        print("LOAD: %s" % self.source)
        # Reload raw results fresh every time
        result_count = RawResult.objects.filter(source=self.source).count()
        if result_count > 0:
            print("\tDeleting %s previously loaded raw results" % result_count)
            RawResult.objects.filter(source=self.source).delete()

    # Private methods
    #TODO: QUESTION: Move to BaseLoader?
    @property
    def _file_handle(self):
        return open(join(self.cache.abspath, self.source), 'rU')

    #TODO: QUESTION: Move to BaseLoader? Should be able to provide
    # the meta fields for free on all states.
    def _build_common_election_kwargs(self):
        """These fields are derived from OpenElex API and common to all RawResults"""
        year = int(re.search(r'\d{4}', self.election_id).group())
        elecs = self.datasource.elections(year)[year]
        # Get election metadata by matching on election slug
        elec_meta = [e for e in elecs if e['slug'] == self.election_id][0]
        kwargs = {
            'created':  self.timestamp,
            'updated': self.timestamp,
            'source': self.source,
            'election_id': self.election_id,
            'state': self.state.upper(),
            'start_date': datetime.datetime.strptime(elec_meta['start_date'], "%Y-%m-%d"),
            'end_date': datetime.datetime.strptime(elec_meta['end_date'], "%Y-%m-%d"),
            'election_type': elec_meta['race_type'],
            'primary_type': elec_meta['primary_type'],
            'result_type': elec_meta['result_type'],
            'special': elec_meta['special'],
        }
        return kwargs


class MDLoaderAfter2002(MDBaseLoader):

    def load(self):
        with self._file_handle as csvfile:
            results = []
            reader = unicodecsv.DictReader(csvfile, encoding='latin-1')
            for row in reader:
                # Skip non-target offices
                if self._skip_row(row): 
                    continue
                elif 'state_legislative' in self.source:
                    results.extend(self._prep_state_leg_results(row))
                elif 'precinct' in self.source:
                    results.append(self._prep_precinct_result(row))
                else:
                    results.append(self._prep_county_result(row))
            RawResult.objects.insert(results)

    def _skip_row(self, row):
        return row['Office Name'].strip() not in self.target_offices

    def _build_contest_kwargs(self, row, primary_type):
        kwargs = {
            'office': row['Office Name'].strip(),
            'district': row['Office District'].strip(),
        }
        # Add party if it's a primary
        #TODO: QUESTION: Should semi-closed also have party?
        if primary_type == 'closed':
            kwargs['primary_party'] = row['Party'].strip()
        return kwargs

    def _build_candidate_kwargs(self, row):
        full_name = row['Candidate Name'].strip()
        slug = slugify(full_name, substitute='-')
        kwargs = {
            'full_name': full_name,
            #TODO: QUESTION: Do we need this? if so, needs a matching model field on RawResult
            'name_slug': slug,
        }
        return kwargs

    def _base_kwargs(self, row):
        "Build base set of kwargs for RawResult"
        # TODO: Can this just be called once?
        kwargs = self._build_common_election_kwargs()
        contest_kwargs = self._build_contest_kwargs(row, kwargs['primary_type'])
        candidate_kwargs = self._build_candidate_kwargs(row)
        kwargs.update(contest_kwargs)
        kwargs.update(candidate_kwargs)
        return kwargs

    def _prep_state_leg_results(self, row):
        kwargs = self._base_kwargs(row)
        kwargs.update({
            'reporting_level': 'state_legislative',
            'winner': row['Winner'].strip(),
            'write_in': self._writein(row),
            'party': row['Party'].strip(),
        })
        try:
            kwargs['write_in'] = row['Write-In?'].strip() # at the contest-level
        except KeyError as e:
            pass
        results = []
        for field, val in row.items():
            # Legislative fields prefixed with LEGS
            if not field.startswith('LEGS'):
                continue
            kwargs.update({
                'jurisdiction': field,
                'votes': self._votes(val),
            })
            results.append(RawResult(**kwargs))
        return results

    def _prep_county_result(self, row):
        kwargs = self._base_kwargs(row)
        vote_brkdown_fields = [
           ('election_night_total', 'Election Night Votes'),
           ('absentee_total', 'Absentees Votes'),
           ('provisional_total', 'Provisional Votes'),
           ('second_absentee_total', '2nd Absentees Votes'),
        ]
        vote_breakdowns = {}
        for field, key in vote_brkdown_fields:
            try:
                vote_breakdowns[field] = row[key].strip()
            except KeyError:
                pass
        kwargs.update({
            'reporting_level': 'county',
            'jurisdiction': self.mapping['name'],
            'party': row['Party'].strip(),
            'votes': self._votes(row['Total Votes']),
        })
        return RawResult(**kwargs)

    def _prep_precinct_result(self, row):
        kwargs = self._base_kwargs(row)
        vote_breakdowns = {
            'election_night_total': int(float(row['Election Night Votes']))
        }
        precinct = str(row['Election Precinct'])
        kwargs.update({
            'reporting_level': 'precinct',
            'jurisdiction': precinct,
            # In Maryland, precincts are nested below counties.
            #
            # The mapping ocd_id will be for the precinct's county.
            # We'll save it as an expando property of the raw result because
            # we won't have an easy way of looking up the county in the 
            # transforms.
            'county_ocd_id': self.mapping['ocd_id'],
            'party': row['Party'].strip(),
            'votes': self._votes(row['Election Night Votes']),
            'winner': row['Winner'],
            'write_in': self._writein(row),
            'vote_breakdowns': vote_breakdowns,
        })
        return RawResult(**kwargs)

    def _votes(self, val):
        if val.strip() == '':
            total_votes = 0
        else:
            total_votes = int(float(val))
        return total_votes

    def _writein(self, row):
        # sometimes write-in field not present
        try:
            write_in = row['Write-In?'].strip()
        except KeyError:
            write_in = None
        return write_in


class MDLoader2002(MDBaseLoader):
    """
    Loads Maryland results for 2002.

    Format:

    Maryland results for 2002 are in a delimited text file where the delimiter
    is '|'.

    Fields:

     0: Office
     1: Office District - '-' is used to denote null values 
     2: County
     3: Last Name - "zz998" is used for write-in candidates
     4: Middle Name - "\N" is used to denote null values
     5: First Name - "Other Write-Ins" is used for write-in candidates
     6: Party
     7: Winner - Value is 0 or 1
     8: UNKNOWN - Values are "(Vote for One)", "(Vote for No More Than Three)", etc.
     9: Votes 
    10: UNKNOWN - Values are "\N" for every row
    
    Sample row:

    House of Delegates                                                  |32 |Anne Arundel County                               |Burton                                                      |W.              |Robert                                                      |Republican                                        |              0|(Vote for No More Than Three)                     |           1494|\N

    Notes:

    In the general election file, there are rows for judges and for
    "Statewide Ballot Questions".  The columns in these rows are shifted over,
    but we can ignore these rows since we're not interested in these offices.

    """

    def load(self):
        headers = [
            'office',
            'district',
            'jurisdiction',
            'family_name',
            'additional_name',
            'given_name',
            'party',
            'winner',
            'vote_type',
            'votes',
            'fill2'
        ]
        self._common_kwargs = self._build_common_election_kwargs()
        self._common_kwargs['reporting_level'] = 'county'
        # Store result instances for bulk loading
        results = []

        with self._file_handle as csvfile:
            reader = unicodecsv.DictReader(csvfile, fieldnames = headers, delimiter='|', encoding='latin-1')
            for row in reader:
                if self._skip_row(row):
                    continue
                
                rr_kwargs = self._common_kwargs.copy()
                if rr_kwargs['primary_type'] == 'closed':
                    rr_kwargs['primary_party'] = row['party'].strip()
                rr_kwargs.update(self._build_contest_kwargs(row))
                rr_kwargs.update(self._build_candidate_kwargs(row))
                rr_kwargs.update({
                    'party': row['party'].strip(),
                    'jurisdiction': row['jurisdiction'].strip(),
                    'office': row['office'].strip(),
                    'district': row['district'].strip(),
                    'votes': int(row['votes'].strip()),
                })
                results.append(RawResult(**rr_kwargs))
        RawResult.objects.insert(results)

    def _skip_row(self, row):
        return row['office'].strip() not in self.target_offices

    def _build_contest_kwargs(self, row):
        return {
            'office': row['office'].strip(),
            'district': row['district'].strip(),
        }

    def _build_candidate_kwargs(self, row):
        return {
            'family_name': row['family_name'].strip(),
            'given_name': row['given_name'].strip(),
            'additional_name': row['additional_name'].strip(),
        }


class MDLoader2000Primary(MDBaseLoader):

    def load(self):
        candidates = {}
        results = []
        with self._file_handle as csvfile:
            lines = csvfile.readlines()
            #TODO: use Datasource.mappings instead of jurisdiction_mappings
            #jurisdictions = self.jurisdiction_mappings(('ocd','fips','urlname','name'))
            #jurisdictions = self.datasource.mappings()
            for line in lines[0:377]:
                if line.strip() == '':
                    continue # skip blank lines
                else: # determine if this is a row with an office
                    #TODO: push party and office to transform step
                    cols = line.strip().split(',')
                    if cols[0][1:29] == 'President and Vice President':
                        office_name = 'President - Vice Pres'
                        if 'Democratic' in cols[0]:
                            raw_party = "Democratic"
                            party = 'DEM'
                        else:
                            raw_party = "Republican"
                            party = 'REP'
                        continue
                    elif cols[0][1:13] == 'U.S. Senator':
                        office_name = 'U.S. Senator'
                        if 'Democratic' in cols[0]:
                            raw_party = "Democratic"
                            party = 'DEM'
                        else:
                            raw_party = "Republican"
                            party = 'REP'
                        continue
                    elif cols[0][1:27] == 'Representative in Congress':
                        district = int(cols[0][71:73])
                        office_name = 'U.S. Congress ' + str(district)
                        if 'Democratic' in cols[0]:
                            raw_party = "Democratic"
                            party = 'DEM'
                        else:
                            raw_party = "Republican"
                            party = 'REP'
                        continue
                    # skip offices we don't want
                    elif cols[0][1:21] == 'Judge of the Circuit':
                        continue
                    elif cols[0][1:31] == 'Female Delegates and Alternate':
                        continue
                    elif cols[0][1:29] == 'Male Delegates and Alternate':
                        continue
                    elif cols[0][1:28] == 'Delegates to the Republican':
                        continue
                    elif cols[0] == '""':
                        candidates = [x.replace('"','').strip() for x in cols if x.replace('"','') != '']
                        winner_index = [i for i, j in enumerate(candidates) if 'Winner' in j][0] # index of winning candidate
                        candidates[winner_index], raw_winner = candidates[winner].split(' Winner') # trims "Winner" from candidate name; we can save the remainder for raw_winner
                        # handle name_parse and Uncommitted candidate
                    else: # has to be a county result
                        result = [x.replace('"','').strip() for x in cols if x != '']
                        #juris = [j for j in jurisdictions if j['name'] == result[0].strip()][0]
                        juris = [j for j in self.datasource.mappings() if j['name'] == result[0].strip()][0]
                        cand_results = zip(candidates, result[1:])
                        for cand, votes in cand_results:
                            name = self.parse_name(cand)
                            #cand_kwargs = {
                                #'given_name': name.first,
                                #'additional_name': name.middle,
                                #'family_name': name.last,
                                #'suffix': name.suffix,
                                #'name': name.full_name
                            #}
                            if office_name == 'President - Vice Pres':
                                cand_kwargs['state'] = 'US'
                            else:
                                cand_kwargs['state'] = self.state.upper()

                            #TODO Get or create candidate
                            candidate = Candidate(**cand_kwargs)
                            result_kwargs = {
                                'candidate': candidate,
                                'ocd_id': juris['ocd'],
                                'jurisdiction': juris['name'],
                                #TODO: verify __init__ election_id logic works here
                                'election_id': self.election_id,
                                'slug': 'TODO',
                                #'raw_office': office_name,
                                'reporting_level': 'county',
                                'raw_party': raw_party,
                                'party': party,
                                'write_in': False,
                                'total_votes': int(votes),
                                'vote_breakdowns': {}
                            }
                            result = RawResult(**result_kwargs)
                            results.append(result)

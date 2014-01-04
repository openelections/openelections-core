from os.path import exists, join
import datetime
import re
import unicodecsv

from openelex.base.load import BaseLoader
from openelex.models import Candidate, Result, Contest
from openelex.lib.text import slugify

from .datasource import Datasource


class LoadResults(BaseLoader):

    def run(self, mapping):
        self.source = mapping['generated_filename']

        self.timestamp = datetime.datetime.now()
        self.datasource = Datasource()
        self.election_id = mapping['election']
        # Unlike Results, Contest and Candidate metadata is not deleted and reloaded each
        # time, since this metadata is required for multiple file types.
        # We build lookups for this data to optimize Results data loading.
        self.contest_lkup = self._build_lkup(Contest)
        self.cand_lkup = self._build_lkup(Candidate)

        print("LOAD: %s" % self.source)
        # Reload results fresh every time
        result_count = Result.objects.filter(source=self.source).count()
        if result_count > 0:
            print("\tDeleting %s previously loaded results" % result_count)
            Result.objects.filter(source=self.source).delete()

        # Load results based on file type
        if '2002' in self.election_id:
            self.load_2002_file(mapping)
        # special case for 2000 primary
        elif '2000' in self.election_id and 'primary' in self.election_id:
            self.load_2000_primary_file(mapping)
        else:
            self.load_non2002_file(mapping)

    # Private methods
    def _build_lkup(self, doc_klass):
        """Build lkup for Contests/Candiates

        self_build_lkup(Candidate, mapping['generated_filename'])

        """
        # Fetch all contests or candidates for given election
        lkup = {}
        objs = doc_klass.objects.filter(election_id = self.election_id)
        for obj in objs:
            lkup[obj.key] = obj
        return lkup

    @property
    def _file_handle(self):
        return open(join(self.cache.abspath, self.source), 'rU')

    def _get_or_create_contest(self, row, mapping):
        year = int(re.search(r'\d{4}', self.election_id).group())
        elecs = self.datasource.elections(year)[year]
        # Get election metadata by matching on election slug
        elec_meta = [e for e in elecs if e['slug'] == self.election_id][0]
        party = row['Party'].strip()
        slug = self._build_contest_slug(row)
        key = (self.election_id, slug)
        try:
            contest = self.contest_lkup[key]
        except KeyError:
            kwargs = {
                'created':  self.timestamp,
                'updated': self.timestamp,
                'source': self.source,
                'election_id': self.election_id,
                'slug': slug,
                'state': self.state.upper(),
                'start_date': datetime.datetime.strptime(elec_meta['start_date'], "%Y-%m-%d"),
                'end_date': datetime.datetime.strptime(elec_meta['end_date'], "%Y-%m-%d"),
                'election_type': elec_meta['race_type'],
                'result_type': elec_meta['result_type'],
                'special': elec_meta['special'],
                'raw_office': row['Office Name'].strip(),
                'raw_district': row['Office District'].strip(),
            }
            contest = Contest(**kwargs)
            # Add party if it's a primary
            if 'primary' in self.election_id:
                kwargs['raw_party'] = party
            contest.save()
            self.contest_lkup[key] = contest
        return contest

    def _get_or_create_candidate(self, row, contest):
        raw_full_name = row['Candidate Name'].strip()
        slug = slugify(raw_full_name, substitute='-')
        key = (self.election_id, contest.slug, slug)
        try:
            candidate = self.cand_lkup[key]
        except KeyError:
            cand_kwargs = {
                'source': self.source,
                'election_id': self.election_id,
                'contest': contest,
                'contest_slug': contest.slug,
                'state': self.state.upper(),
                'raw_full_name': raw_full_name,
                'slug': slug,
            }
            candidate = Candidate.objects.create(**cand_kwargs)
            candidate.save()
            self.cand_lkup[key] = candidate
        return candidate

    def _build_contest_slug(self, row):
        office = row['Office Name'].strip()
        bits = [office]
        district = row['Office District'].strip()
        if district and 'pres' not in office.lower():
            bits.append(district)
        if 'primary' in self.source:
            bits.append(row['Party'].lower())
        return slugify(" ".join(bits), substitute='-')

    def load_non2002_file(self, mapping):
        with self._file_handle as csvfile:
            results = []
            target_offices = set([
                'President - Vice Pres',
                'U.S. Senator',
                'U.S. Congress',
                'Governor / Lt. Governor',
                'Comptroller',
                'Attorney General',
                'State Senator',
                'House of Delegates'
            ])
            reader = unicodecsv.DictReader(csvfile, encoding='latin-1')
            for row in reader:
                # Skip non-target offices
                if not row['Office Name'].strip() in target_offices:
                    continue
                elif 'state_legislative' in self.source:
                    results.extend(self._prep_non2002_state_leg_results(row, mapping))
                elif 'precinct' in self.source:
                    results.append(self._prep_non2002_precinct_result(row, mapping))
                else:
                    results.append(self._prep_non2002_county_result(row, mapping))
            Result.objects.insert(results)

    def _result_kwargs_non2002(self, row, mapping):
        contest = self._get_or_create_contest(row, mapping)
        candidate = self._get_or_create_candidate(row, contest)
        kwargs = {
            'source': self.source,
            'election_id': self.election_id,
            'state': self.state.upper(),
            'contest': contest,
            'contest_slug': contest.slug,
            'candidate': candidate,
            'candidate_slug': candidate.slug,
        }
        return kwargs

    def _prep_non2002_state_leg_results(self, row, mapping):
        kwargs = self._result_kwargs_non2002(row, mapping)
        kwargs.update({
            'reporting_level': 'state_legislative',
            'raw_winner': self._non2002_winner(row), # at the contest-level
            'write_in': self._non2002_writein(row),
        })
        try:
            kwargs['raw_write_in'] = row['Write-In?'].strip(), # at the contest-level
        except KeyError as e:
            pass
        self._update_non2002_candidate_parties(row, kwargs['candidate'])
        results = []
        for field, val in row.items():
            # Legislative fields prefixed with LEGS
            if not field.startswith('LEGS'):
                continue
            district = field.split()[1].strip()
            kwargs.update({
                'ocd_id': "ocd-division/country:us/state:md/sldl:%s" % district,
                'jurisdiction': district,
                'raw_jurisdiction': field,
                'raw_total_votes': self._non2002_total_votes(val),
            })
            results.append(Result(**kwargs))
        return results

    def _prep_non2002_county_result(self, row, mapping):
        kwargs = self._result_kwargs_non2002(row, mapping)
        vote_brkdwon_fields = [
           ('election_night_total', 'Election Night Votes'),
           ('absentee_total', 'Absentees Votes'),
           ('provisional_total', 'Provisional Votes'),
           ('second_absentee_total', '2nd Absentees Votes'),
        ]
        vote_breakdowns = {}
        for field, key in vote_brkdwon_fields:
            try:
                vote_breakdowns[field] = row[key].strip()
            except KeyError:
                pass
        kwargs.update({
            'ocd_id': mapping['ocd_id'],
            'reporting_level': 'county',
            'jurisdiction': mapping['name'],
            'party': row['Party'].strip(),
            'raw_total_votes': self._non2002_total_votes(row['Total Votes']),
            'raw_winner': self._non2002_winner(row),
            'raw_write_in': self._non2002_writein(row),
            'raw_vote_breakdowns': vote_breakdowns,
            #TODO: Move to transforms step?
            #'total_votes': self._non2002_total_votes(row['Total Votes']),
            #'winner': self._non2002_winner(row),
            #'write_in': self._non2002_writein(row),
        })
        return Result(**kwargs)

    def _prep_non2002_precinct_result(self, row, mapping):
        kwargs = self._result_kwargs_non2002(row, mapping)
        vote_breakdowns = {
            'election_night_total': int(float(row['Election Night Votes']))
        }
        raw_district = str(row['Election District'])
        raw_precinct = str(row['Election Precinct'])
        kwargs.update({
            #'ocd_id': mapping['ocd_id'],
            'reporting_level': 'precinct',
            'raw_jurisdiction':raw_precinct,
            'jurisdiction': mapping['name'] + ' ' + raw_district  + "-" + raw_precinct,
            'party': row['Party'].strip(),
            'raw_total_votes': int(float(row['Election Night Votes'])),
            'raw_winner': self._non2002_winner(row),
            'raw_write_in': self._non2002_writein(row),
            'raw_vote_breakdowns': vote_breakdowns,
            #TODO: Move total votes to transform step?
            #'total_votes': int(float(row['Election Night Votes'])),
            #'winner': self._non2002_winner(row),
            #'write_in': self._non2002_writein(row),
        })
        return Result(**kwargs)

    def _update_non2002_candidate_parties(self, row, candidate):
        raw_party = row['Party'].strip()
        if raw_party not in candidate.raw_parties:
            candidate.raw_parties.append(raw_party)
        candidate.save()

    def _non2002_total_votes(self, val):
        if val.strip() == '':
            total_votes = 0
        else:
            total_votes = int(float(val))
        return total_votes

    def _non2002_winner(self, row):
        if row['Winner'] == 'Y':
            winner = True
        else:
            winner = False
        return winner

    def _non2002_writein(self, row):
        # sometimes write-in field not present
        try:
            if row['Write-In?'] == 'Y':
                write_in = True
            else:
                write_in = False
        except KeyError:
            write_in = None
        return write_in

    def load_county_2002(self, row):
        total_votes = int(float(row['votes']))
        #TODO: replace above 2 lines with below
        reporting_level = 'precinct'
        ocd_id = mapping['ocd_id']
        total_votes = int(float(row['votes']))
        kwargs = {
            #TODO: generate ocd_id
            #'ocd_id': ocd_id,
            #'jurisdiction': jurisdiction,
            'reporting_level': reporting_level,
            'raw_office': row['office'].strip(),
            'candidate': candidate,
            'party': row['party'].strip(),
            'write_in': write_in,
            'raw_total_votes': total_votes,
            'vote_breakdowns': {}
        }
        return Result(**kwargs)

    def load_2002_file(self, mapping):
        headers = [
            'office',
            'district',
            'jurisdiction',
            'last',
            'middle',
            'first',
            'party',
            'winner',
            'vote_type',
            'votes',
            'fill2'
        ]
        # Store result instances for bulk loading
        results = []
        with self._file_handle as csvfile:
            reader = unicodecsv.DictReader(csvfile, fieldnames = headers, delimiter='|', encoding='latin-1')
            reporting_level = 'county'
            #TODO: replace jurisidctions with datasource.mappings
            #geographies = self.jurisdiction_mappings(('ocd','fips','urlname','name'))
            geographies = self.mappings()
            for row in reader:
                if row['jurisdiction'].strip() == 'Baltimore City':
                    geo_obj = [x for x in geographies if x['name'] == "Baltimore City"][0]
                else:
                    geo_obj = [x for x in geographies if x['name']+" County" == row['jurisdiction'].strip()][0]

                # Winner
                #TODO: make this raw_winner
                #TODO: push this to transform step
                raw_winner = row['winner']
                #if row['winner'] == '1':
                #    raw_winner = True
                #else:
                #    raw_winner = False

                #TODO: push name parsing to transform step
                # zz998 is for write-ins
                if row['last'].strip() == 'zz998':
                #    name = row['last'].strip()
                #    #candidate = Candidate(state=self.state.upper(), family_name=name)
                     write_in=True
                else:
                #    cand_name = self.combine_name_parts([row['first'], row['middle'], row['last']])
                #    name = self.parse_name(cand_name)
                     write_in=False
                #    candidate = Candidate(state=self.state.upper(), given_name=name.first, additional_name=name.middle, family_name=name.last, suffix=name.suffix, name=name.full_name)

                #TODO: handle below using the self._get_candidate_whatever(row)
                #TODO: need a new function/method to handle 2002?
                cand_kwargs = {
                    'raw_family_name': row['last'].strip(),
                    'raw_given_name': row['first'].strip(),
                    'raw_additional_name': row['middle'].strip(),
                }
                 #TODO: add raw_party to candidate.raw_parties

                result_kwargs = {
                    #'election':
                    #'candidate': candidate,
                    'ocd_id': geo_obj['ocd_id'],
                    #TODO: make this the county number from file
                    'raw_jurisdiction': row['jurisdiction'],
                    'jurisdiction': geo_obj['name'],
                    'raw_office': row['office'].strip(),
                    'raw_district': row['district'].strip(),
                    'reporting_level': 'county',
                    'raw_party': row['party'], # needs to be a member of a list
                    'write_in': write_in,
                    'raw_total_votes': row['votes'],
                }
                result = Result(**result_kwargs)
        Result.objects.insert(results)

    def load_2000_primary_file(self, mapping):
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
                            party = 'DEM'
                        else:
                            party = 'REP'
                        continue
                    elif cols[0][1:13] == 'U.S. Senator':
                        office_name = 'U.S. Senator'
                        if 'Democratic' in cols[0]:
                            party = 'DEM'
                        else:
                            party = 'REP'
                        continue
                    elif cols[0][1:27] == 'Representative in Congress':
                        district = int(cols[0][71:73])
                        office_name = 'U.S. Congress ' + str(district)
                        if 'Democratic' in cols[0]:
                            party = 'DEM'
                        else:
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
                                'party': party,
                                'write_in': False,
                                'total_votes': int(votes),
                                'vote_breakdowns': {}
                            }
                            result = Result(**result_kwargs)
                            results.append(result)

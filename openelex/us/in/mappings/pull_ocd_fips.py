#!/usr/bin/env python2

from future import standard_library
standard_library.install_aliases()
import contextlib
import unicodecsv
import re
import urllib.request, urllib.error, urllib.parse

OCD_URL = 'https://raw.githubusercontent.com/opencivicdata/ocd-division-ids/master/identifiers/country-us/state-in-local_gov.csv'
FIPS_URL = 'https://www2.census.gov/geo/docs/reference/codes/files/st18_in_cou.txt'
OUT_FILE = 'in.csv'

OCD_REGEX = re.compile('(ocd-division/country:us/state:in/county:[^/]*)')


def main():
    ocd_data = {}
    with contextlib.closing(urllib.request.urlopen(OCD_URL)) as ocd_csv:
        reader = unicodecsv.reader(ocd_csv, encoding='utf-8')
        for row in reader:
            match = OCD_REGEX.match(row[0])
            if match:
                ocd_id = match.group(0)
                county = ocd_id.split(':')[-1]
                if county not in ocd_data:
                    ocd_data[county] = ocd_id

    with open(OUT_FILE, 'w') as out_csv:
        writer = unicodecsv.writer(out_csv, encoding='utf-8')
        writer.writerow(['county', 'fips', 'ocd_id'])
        with contextlib.closing(urllib.request.urlopen(FIPS_URL)) as fips_csv:
            reader = unicodecsv.reader(fips_csv, encoding='utf-8')
            for row in reader:
                fips = int(row[2])
                county = re.sub(' County$', '', row[3]).replace('.', '')
                ocd_county = county.lower().replace(' ', '_')
                ocd_id = ocd_data[ocd_county]
                writer.writerow([county, fips, ocd_id])


if __name__ == "__main__":
    main()

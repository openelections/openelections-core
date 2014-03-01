### West Virginia Datasource Process

For elections from 2008-onward, West Virginia provides statewide county-level and county-specific precinct-level CSV files for statewide, congressional and state legislative contests. These are loaded directly by datasource.py. For elections prior to 2007, the state provides PDF files containing county-level results, not precinct-level, for each office and election. These are converted into CSV files using the following headers:

	year, election, office, district, party, county, candidate, votes, winner

District only appears in legislative contests. This results in a single row per election result, with the winner column representing a boolean on the "Totals" row. Datasource.py creates the mappings for these PDF and CSV files using the url_paths.csv file in /mappings.

#### Issues

* The 2006 House of Delegates primary results PDF is missing from the WV site; I have emailed to ask them to post it.
* The 2004 State Senate primary results PDF appears to be missing results for district 8 and district 17; Email sent asking state to post results.
* The 2004 House of Delegates Democratic and Republican primary results for District 1 appears to be incorrect; email sent to confirm.
* The 2004 House of Delegates Democratic primary results for District 5 appear to be incorrect.
* The 2004 House of Delegates Republican primary results for District 42 appear to be incorrect.
* The 2004 House of Delegates Democratic primary results for District 43 appear to include Republican candidates.

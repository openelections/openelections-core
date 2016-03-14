"""
New Mexico elections have xlsx files with county and precinct-level data.

start with election http://electionresults.sos.state.nm.us/resultsSW.aspx?eid=1&type=FED&map=CTY, then scrape county-level result links.
example: http://electionresults.sos.state.nm.us/resultsCTY.aspx?eid=1&type=FED&rid=83&pty=DEM&osn=100&map=CTY
then grab precinct-level results files; the first sheet has county-level totals. results begin on row 7 of sheet

"""

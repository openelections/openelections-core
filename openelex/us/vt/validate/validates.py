import re

from openelex.models import Contest, Candidate, Office, Result
import logging
import time
import os

# if not os.path.isdir("logs"):
#     os.makedirs("logs")
# logging.basicConfig(filename=time.strftime("logs/%Y%m%d-%H%M%S-validate.log"),level=logging.DEBUG)


# Generic validation helpers

def _validate_candidate_votes(election_id, reporting_level, contest_slug,
        candidate_slug, expected_votes):
    """Sum sub-contest level results and compare them to known totals"""
    msg = "Expected {} votes for contest {} and candidate {}, found {}"
    votes = Result.objects.filter(election_id=election_id,
        contest_slug=contest_slug, candidate_slug=candidate_slug,
        reporting_level=reporting_level).sum('votes')
    if votes != expected_votes:
        logging.debug("db.getCollection('result').find({election_id:\"%s\", \
            contest_slug:\"%s\", candidate_slug:\"%s\", \
            reporting_level:\"%s\"})", election_id, contest_slug, candidate_slug, reporting_level)
    assert votes == expected_votes, msg.format(expected_votes, contest_slug,
            candidate_slug, votes)

def _validate_many_candidate_votes(election_id, reporting_level,
        candidates):
    """
    Sum sub-contest level results and compare them to known totals for
    multiple contests and candidates.

    Arguments:

    election_id - Election ID of the election of interest.
    reporting_level - Reporting level to use to aggregate results.
    candidates - Tuple of contests slug, candidate slug and expected votes.

    """
    for candidate_info in candidates:
        contest, candidate, expected = candidate_info
        _validate_candidate_votes(election_id, reporting_level,
            contest, candidate, expected)



def validate_results_2012_president_general():
    """Sum some county-level results for 2012 general presidential and compare with known totals"""
    election_id = 'vt-2012-11-06-general'
    known_results = [
        ('president', 'barack-obama', 199053),
    ]
    _validate_many_candidate_votes(election_id, 'parish', known_results)
    _validate_many_candidate_votes(election_id, 'precinct', known_results)


def validate_results_2014_house_general():
    """Sum some county-level results for 2014 general and compare with known totals"""
    election_id = 'vt-2014-11-04-general'
    known_results = [
        ('us-house-of-representatives', 'peter-welch', 123349),
        ('us-house-of-representatives', 'mark-donka', 59432),
        ('us-house-of-representatives', 'cris-ericson', 2750),
    ]
    _validate_many_candidate_votes(election_id, 'parish', known_results)
    _validate_many_candidate_votes(election_id, 'precinct', known_results)

def validate_results_2014_house_primary():
    """Sum some county-level results for 2014 house primary and compare with known totals"""
    election_id = 'vt-2014-08-26-primary'
    known_results = [
        ('us-house-of-representatives-d', 'peter-welch', 19248),
        ('us-house-of-representatives-d', 'writeins', 224),
        ('us-house-of-representatives-r', 'mark-donka', 4340),
        ('us-house-of-representatives-r', 'donald-russell', 4026),
    ]
    _validate_many_candidate_votes(election_id, 'parish', known_results)
    _validate_many_candidate_votes(election_id, 'precinct', known_results)

def validate_results_2002_lt_gov_general():
    """Sum some county-level results for 2002 lt-gov general  and compare with known totals"""
    election_id = 'vt-2002-11-05-general'
    known_results = [
        ('lieutenant-governor', 'peter-shumlin', 73501),
        ('lieutenant-governor', 'brian-e-dubie', 94044),
        ('lieutenant-governor', 'anthony-pollina', 56564),
        ('lieutenant-governor', 'sally-ann-jones', 4310),
    ]
    _validate_many_candidate_votes(election_id, 'parish', known_results)
    _validate_many_candidate_votes(election_id, 'precinct', known_results)

def validate_results_2002_lt_gov_primary():
    """Sum some county-level results for 2002 lt-gov  primary and compare with known totals"""
    election_id = 'vt-2002-09-10-primary'
    known_results = [
        ('lieutenant-governor-d', 'peter-shumlin', 22633),
        ('lieutenant-governor-r', 'brian-e-dubie', 22584),
    ]
    _validate_many_candidate_votes(election_id, 'parish', known_results)
    _validate_many_candidate_votes(election_id, 'precinct', known_results)

def validate_results_2004_misc_results_general():
    """Sum some state specific results for 2004 general and compare with known totals"""
    election_id = 'vt-2004-11-02-general'
    known_results = [
        ('treasurer', 'jeb-spaulding', 273705),
        ('secretary-of-state', 'deb-markowitz', 270744),
        ('auditor', 'randy-brock', 152848),
        ('auditor', 'elizabeth-m-ready', 122498),
        ('auditor', 'jerry-levy', 17685),
        ('attorney-general', 'william-h-sorrell', 169726),
        # there is an error on the vermont website, I talked to the VT Sec state and the real result should be 81,285
        # ('attorney-general', 'dennis-carver', 90285),
        ('attorney-general', 'susan-a-davis', 14351),
        ('attorney-general', 'james-mark-leas', 8769),
        ('attorney-general', 'karen-kerin', 6357),
        ('attorney-general', 'boots-wardinski', 2944),
    ]
    _validate_many_candidate_votes(election_id, 'parish', known_results)
    _validate_many_candidate_votes(election_id, 'precinct', known_results)

def validate_results_2008_state_senate_primary():
    """Sum some county-level results for 2008 state senate  primary and compare with known totals"""
    election_id = 'vt-2008-09-08-primary'
    known_results = [
        ('state-senate-orange-d', 'mark-a-macdonald', 557),
        ('state-senate-franklin-r', 'randy-brock', 879),
        ('state-senate-franklin-r', 'willard-rowell', 782),
        ('state-senate-essexorleans-d', 'robert-a-starr', 748),
        ('state-senate-essexorleans-d', 'writeins', 112),
    ]
    _validate_many_candidate_votes(election_id, 'parish', known_results)
    _validate_many_candidate_votes(election_id, 'precinct', known_results)

def validate_results_2010_state_senate_general():
    """Sum some county-level results for 2010 state senate general and compare with known totals"""
    election_id = 'vt-2010-11-02-general'
    known_results = [
        ('state-senate-orange', 'mark-a-macdonald', 4524),
        ('state-senate-orange', 'stephen-w-webster', 3517),
        ('state-senate-franklin', 'randy-brock', 9014),
        ('state-senate-franklin', 'peter-d-moss', 793),
        ('state-senate-essexorleans', 'robert-a-starr', 9902),
        ('state-senate-essexorleans', 'vincent-illuzzi', 9231),
    ]
    _validate_many_candidate_votes(election_id, 'parish', known_results)
    _validate_many_candidate_votes(election_id, 'precinct', known_results)


def validate_results_2012_state_house_primary():
    """Sum some county-level results for 2012 state house  primary and compare with known totals"""
    election_id = 'vt-2012-03-06-primary'
    known_results = [
        ('house-of-representatives-addison-5-d', 'edward-v-mcguire', 220),
        ('house-of-representatives-addison-5-r', 'harvey-smith', 75),

        ('house-of-representatives-addison-1-d', 'betty-a-nuovo', 486),
        ('house-of-representatives-addison-1-d', 'paul-ralston', 446),

        ('house-of-representatives-bennington-1-d', 'bill-botzow', 152),
        ('house-of-representatives-caledonia-1-r', 'leigh-b-larocque', 72),

        ('house-of-representatives-chittenden-61-d', 'joanna-cole', 658),
        ('house-of-representatives-chittenden-61-d', 'bill-aswad', 619),
        ('house-of-representatives-chittenden-61-d', 'robert-hooper', 536),
        ('house-of-representatives-chittenden-61-r', 'kurt-wright', 116),

    ]
    _validate_many_candidate_votes(election_id, 'parish', known_results)
    _validate_many_candidate_votes(election_id, 'precinct', known_results)

def validate_results_2012_state_house_general():
    """Sum some county-level results for 2012 state house general and compare with known totals"""
    election_id = 'vt-2012-11-06-general'
    known_results = [
        ('house-of-representatives-addison-5', 'edward-v-mcguire', 982),
        ('house-of-representatives-addison-5', 'harvey-smith', 1151),

        ('house-of-representatives-addison-1', 'betty-a-nuovo', 2601),
        ('house-of-representatives-addison-1', 'paul-ralston', 2378),

        ('house-of-representatives-bennington-1', 'bill-botzow', 1613),
        ('house-of-representatives-caledonia-1', 'leigh-b-larocque', 1143),

        ('house-of-representatives-chittenden-61', 'joanna-cole', 2008),
        ('house-of-representatives-chittenden-61', 'bill-aswad', 1987),
        ('house-of-representatives-chittenden-61', 'kurt-wright', 2332),

    ]
    _validate_many_candidate_votes(election_id, 'parish', known_results)
    _validate_many_candidate_votes(election_id, 'precinct', known_results)

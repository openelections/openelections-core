from datetime import datetime
from unittest import TestCase

from openelex.models import Contest, Office, Party
from openelex.tests.mongo_test_case import MongoTestCase

        
class TestOffice(TestCase):
    def test_slug(self):
        office = Office(state="MD", name= "House of Delegates", district="35B",
                        chamber="lower")
        self.assertEqual(office.slug, "house-of-delegates-35b")

        pres = Office(state="US", name="President")
        self.assertEqual(pres.slug, "president")


class TestParty(TestCase):
    def test_slug(self):
        dem = Party(name="Democrat", abbrev="DEM")
        self.assertEqual(dem.slug, "dem")


class TestContest(MongoTestCase):
    def assert_times_almost_equal(self, first, second, difference=1):
        assert abs(first - second).total_seconds() < difference

    def test_auto_slug(self):
        office = Office(state="MD", name= "House of Delegates", district="35B",
                        chamber="lower")
        dem = Party(name="Democrat", abbrev="DEM")
        contest = Contest(office=office, primary_party=dem)
        self.assertEqual(contest.slug, "house-of-delegates-35b-dem") 

    def test_auto_timestamps(self):
        office = Office.objects.create(state="MD", name= "House of Delegates", 
            district="35B", chamber="lower")

        contest = Contest(
            result_type='certified',
            election_type='general',
            start_date=datetime.strptime("2012-11-06", "%Y-%m-%d"),
            end_date=datetime.strptime("2012-11-06", "%Y-%m-%d"),
            source='20121106__md__general__state_legislative.csv',
            state='MD',
            election_id='md-2012-11-06-general',
            office=office,
        )
        # Check that created and updated timestamps are auto-set when a new
        # instance is created
        self.assertIsNotNone(contest.created)
        self.assertIsNotNone(contest.updated)
        self.assert_times_almost_equal(contest.created, contest.updated)

        # Test that the timestamp is updated when the contest is saved
        updated = contest.updated
        contest.save()
        self.assertGreater(contest.updated, updated)

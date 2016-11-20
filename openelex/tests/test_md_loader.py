
from openelex.tests.mongo_test_case import MongoTestCase
from openelex.models import RawResult
from openelex.us.md.load import MDLoader2008Special

class TestMDLoader2008Special(MongoTestCase):
    def setUp(self):
        super(TestMDLoader2008Special, self).setUp()

        self.loader = MDLoader2008Special()
        self.mapping = self._get_mapping()
        # HACK: set loader's mapping attribute 
        # so we can test if loader._file_handle exists.  This
        # usually happens in the loader's run() method.
        self.loader.source = self.mapping['generated_filename']
        try:
            fh = self.loader._file_handle
        except IOError:
            self.skipTest("Cached file for 2008 special election not found. "
                "Run 'openelex fetch --state=md --datefilter=2008' first.")

    def _get_mapping(self):
        for mapping in self.loader.datasource.mappings('2008'): 
            if 'special' in mapping['election']:
                return mapping
        else:
            raise Exception("Mapping for 2008 special election expected")

    def test_parse_html_table(self):
        table = self.loader._get_html_table()
        rows = self.loader._parse_html_table(table)
        self.assertEqual(len(rows), 4)
        for row in rows:
            self.assertEqual(len(row), 7)

    def test_run(self):
        election_id = self.mapping['election']
        self.loader.run(self.mapping)
        self.assertEqual(
            RawResult.objects.filter(election_id=election_id).count(), 12)

import os
import sys
from unittest import TestCase

from openelex.config import Settings

TEST_SETTING_1 = "foo"
TEST_SETTING_2 = "bar"

class TestSettings(TestCase):
    def tearDown(self):
        try:
            os.environ = self.env
        except AttributeError:
            pass

    def _test_object_keys(self, settings_obj):
       self.assertEqual(settings_obj.TEST_SETTING_1, "foo")
       self.assertEqual(settings_obj.TEST_SETTING_2, "bar")

    def _fix_filename(self, filename):
        return filename.replace('.pyc', '.py')

    def test_from_object(self):
        settings = Settings()
        settings.from_object(sys.modules[__name__])
        self._test_object_keys(settings)

    def test_from_module_name(self):
        settings = Settings()
        settings.from_module_name(__name__)
        self._test_object_keys(settings)

    def test_from_file(self):
        settings = Settings()
        print(__file__)
        settings.from_file(self._fix_filename(os.path.realpath(__file__)))
        self._test_object_keys(settings)

    def test_from_envvar(self):
        self._env = os.environ
        settings = Settings()
        os.environ = {'MOCK_OPENELEX_SETTINGS':
            self._fix_filename(os.path.realpath(__file__))}
        settings.from_envvar('MOCK_OPENELEX_SETTINGS')
        self._test_object_keys(settings)

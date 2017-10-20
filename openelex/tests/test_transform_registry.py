from unittest import TestCase

from mock import Mock

from openelex.base.transform import registry

class TestTransformRegistry(TestCase):
    def test_register_with_validators(self):
        mock_transform = Mock(return_value=None)
        mock_transform.__name__ = 'mock_transform'
        mock_validator1 = Mock(return_value=None)
        mock_validator1.__name__ = 'mock_validator1'
        mock_validator2 = Mock(return_value=None)
        mock_validator2.__name__ = 'mock_validator2'

        validators = [mock_validator1, mock_validator2]

        registry.register("XX", mock_transform, validators) 

        transform = registry.get("XX", "mock_transform")
        self.assertEqual(list(transform.validators.values()), validators)

        transform()
        mock_transform.assert_called_once_with()

    def test_register_raw(self):
        mock_transform = Mock(return_value=None)
        mock_transform.__name__ = 'mock_transform'

        registry.register("XX", mock_transform, raw=True)
        transform = registry.get("XX", "mock_transform", raw=True)
        transform()
        mock_transform.assert_called_once_with()

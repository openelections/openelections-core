from collections import OrderedDict

from .state import StateBase


class Transform(object):
    auto_reverse = False
    """
    Set to True to automatically run the reverse() method before running the
    transform.
    """

    """Base class that defines API for transforms."""
    def __init__(self):
        self._validators = OrderedDict()

    def add_validation(self, *validators):
        """
        Associate validation functions with this transform.
        """
        for v in validators:
            self._validators[v.__name__] = v

    @property
    def validators(self):
        return self._validators

    def __str__(self):
        return self.name

    def __call__(self):
        """
        Run the transform.

        This should be implemented in a subclass.
        """
        raise NotImplemented("You must implement the __call__() method in your "
                             "subclass")
    run = __call__

    def reverse(self):
        """
        Undo the transform.

        This should be implemented in a subclass.
        """
        raise NotImplemented


class FunctionWrappingTransform(Transform):
    """Transform class that wraps a function"""

    def __init__(self, func):
        super(FunctionWrappingTransform, self).__init__()
        self._func = func

    @property
    def name(self):
        return self._func.__name__

    def __call__(self):
        self._func()

    
class Registry(StateBase):

    _registry = {}

    def register(self, state, transform, validators=[]):
        try:
            state_xforms = self._registry[state]
        except KeyError:
            self._registry[state] = OrderedDict()
            state_xforms = self._registry[state]

        if issubclass(transform, Transform):
            # Transform is a class. Instantiate it.
            transform_obj = transform()
        else:
            # Transform is a function. Wrap it in a class instance.
            transform_obj = FunctionWrappingTransform(transform)

        transform_obj.add_validation(*validators)
        state_xforms[transform_obj.name] = transform_obj 

    def get(self, state, name):
        try:
            transform = self._registry[state][name]
        except KeyError:
            err_msg = "Transform (%s) not registered for %s" % (name, state)
            raise KeyError(err_msg)
        return transform

    def all(self, state):
        return self._registry[state].values()


# Global object for registering transform functions
registry = Registry()

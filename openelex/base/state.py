import os


from .cache import StateCache

class StateBase(object):
    """Base class with common functionality for working 
    with state modules. 

    Intended to be subclassed by other base modules or
    state specific modules
    """

    def __init__(self, state=''):
        if not state:
            self.state = self.__module__.split('.')[-2]
        else:
            self.state = state
        self.cache = StateCache(self.state)
        # Create mappings directory if it doesn't exist
        self.mappings_dir = os.path.join('us', self.state, 'mappings')
        try:
            os.makedirs(self.mappings_dir)
        except OSError:
            pass
        # Create ocd mappings csv if it doesn't exist
        open(os.path.join(self.mappings_dir, self.state + '.csv'), 'a').close()


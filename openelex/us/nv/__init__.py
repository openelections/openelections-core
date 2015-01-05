import sys

# This is a pythonpath hack to avoid shadowing third-party
# module with our openelex.us directory
from openelex import PROJECT_ROOT
try:
    sys.path.remove(PROJECT_ROOT)
except ValueError:
    pass

from us import STATES
STATE_POSTALS = map(lambda state: state.abbr, STATES)

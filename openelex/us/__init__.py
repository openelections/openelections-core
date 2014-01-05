import sys

# This is a pythonpath hack to avoid shadowing third-party 
# module with our openelex.us directory
from openelex import PROJECT_ROOT
sys.path.remove(PROJECT_ROOT)

from us import STATES
STATE_POSTALS = map(lambda state: state.abbr, STATES)

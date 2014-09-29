from openelex.base.transform import Transform, registry
from openelex.models import RawResult

class FixVanBurenTransform(Transform):
    """
    s/VanBuren/Van Buren in RawResults from the source file
    20001107__ia__general__state_senate__county.csv
    """
    name = 'fix_van_buren'

    def __call__(self):
        results = RawResult.objects.filter(
            source="20001107__ia__general__state_senate__county.csv",
            jurisdiction="VanBuren")
        msg = "Changing 'VanBuren' to 'Van Buren' in {} raw results.".format(
            results.count())
        print(msg)
        results.update(set__jurisdiction="Van Buren",
            set__ocd_id="ocd-division/country:us/state:ia/county:van_buren")

    def reverse(self):
        results = RawResult.objects.filter(
            source="20001107__ia__general__state_senate__county.csv",
            jurisdiction="Van Buren")
        msg = "Reverting 'Van Buren' to 'VanBuren' in {} raw results".format(
            results.count())
        print(msg)
        results.update(set__jurisdiction="VanBuren", set__ocd_id="")
           
registry.register('ia', FixVanBurenTransform, raw=True)

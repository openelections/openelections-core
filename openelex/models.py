from mongoengine import Document, EmbeddedDocument
from mongoengine.fields import DateTimeField, DictField, StringField


#TODO: change name fields to reflect i18n-friendly conventions
class Candidate(EmbeddedDocument):
    uuid = StringField()
    raw_party = StringField()
    raw_name = StringField()
    raw_first_name = StringField()
    raw_middle_name = StringField()
    raw_lastname = StringField()

    #TODO: validation - raw_name or last_name required


class Referendum(EmbeddedDocument):
    raw_name = StringField(help_text="e.g. yes/no")


class Contest(Document):
    RESULT_LEVEL_CHOICES = (
        'cong_district',
        'state_leg_district',
        'precinct',
        'parish',
        'precinct',
        'county',
        'state',
    )
    created= DateTimeField()
    source = StringField(help_text="slugified data source from dashboard db")
    raw_name = StringField()
    options = DictField(help_text="UUID as keys and candidate or ballot measure choices")
    results = DictField()
    """
    results = {
        'precinct': {
            "total_votes": <or any other top-level figures providied in results>,
            <cand_uuid>: <vote_count>,
            <cand2_uuid>: <vote_count>,
            ...
    }

    """

"""
Maryland political jurisdictions
"""

counties = [
    "Allegany",
    "Anne Arundel",
    "Baltimore City",
    "Baltimore",
    "Calvert",
    "Caroline",
    "Carroll",
    "Cecil",
    "Charles",
    "Dorchester",
    "Frederick",
    "Garrett",
    "Harford",
    "Howard",
    "Kent",
    "Montgomery",
    "Prince George's",
    "Queen Anne's",
    "St. Mary's",
    "Somerset",
    "Talbot",
    "Washington",
    "Wicomico",
    "Worcester",
]

congressional_districts = range(1, 9)

congressional_district_to_county = {
    1: [
        "Anne Arundel",
        "Baltimore City",
        "Caroline",
        "Cecil",
        "Dorchester",
        "Kent",
        "Queen Anne's",
        "Somerset",
        "Talbot",
        "Wicomico",
        "Worcester",
    ],
    2: [
        "Anne Arundel",
        "Baltimore",
        "Harford",
    ],
    3: [
        "Anne Arundel",
        "Baltimore City",
        "Baltimore",
        "Howard",
    ],
    4: [
        "Montgomery",
        "Prince George's",
    ],
    5: [
        "Anne Arundel",
        "Calvert",
        "Charles",
        "Prince George's",
        "St. Mary's",
    ],
    6: [
        "Allegany",
        "Carroll",
        "Frederick",
        "Garrett",
        "Howard",
        "Washington",
    ],
    7: [
        "Baltimore City",
        "Baltimore",
    ],
    8: [
        "Montgomery",
    ],
}

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

congressional_districts = [str(i) for i in range(1, 9)]

state_senate_districts = [str(i) for i in range(1, 48)]

state_legislative_districts = [
    "1A",
    "1B",
    "1C",
    "2A",
    "2B",
    "2C",
    "3A",
    "3B",
    "4A",
    "4B",
    "5A",
    "5B",
    "6",
    "7",
    "8",
    "9A",
    "9B",
    "10",
    "11",
    "12A",
    "12B",
    "13",
    "14",
    "15",
    "16",
    "17",
    "18",
    "19",
    "20",
    "21",
    "22",
    "23A",
    "23B",
    "24",
    "25",
    "26",
    "27A",
    "27B",
    "28",
    "29A",
    "29B",
    "29C",
    "30",
    "31",
    "32",
    "33A",
    "33B",
    "34A",
    "34B",
    "35A",
    "35B",
    "36",
    "37A",
    "37B",
    "38A",
    "38B",
    "39",
    "40",
    "41",
    "42",
    "43",
    "44",
    "45",
    "46",
    "47",
]

congressional_district_to_county_pre_2002 = {
    '1': [
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
    '2': [
        "Anne Arundel",
        "Baltimore",
        "Harford",
    ],
    '3': [
        "Anne Arundel",
        "Baltimore City",
        "Baltimore",
        "Howard",
    ],
    '4': [
        "Montgomery",
        "Prince George's",
    ],
    '5': [
        "Anne Arundel",
        "Calvert",
        "Charles",
        "Prince George's",
        "St. Mary's",
    ],
    '6': [
        "Allegany",
        "Carroll",
        "Frederick",
        "Garrett",
        "Howard",
        "Washington",
    ],
    '7': [
        "Baltimore City",
        "Baltimore",
    ],
    '8': [
        "Montgomery",
    ],
}

congressional_district_to_county_2002 = {
    '1': [
        "Anne Arundel",
        "Baltimore",
        "Caroline",
        "Cecil",
        "Dorchester",
        "Harford",
        "Kent",
        "Queen Anne's",
        "Somerset",
        "Talbot",
        "Wicomico",
        "Worcester",
    ],
    '2': [
        "Anne Arundel",
        "Baltimore City",
        "Baltimore",
        "Harford",
    ],
    '3': [
        "Anne Arundel",
        "Baltimore City",
        "Baltimore",
        "Howard",
    ],
    '4': [
        "Montgomery",
        "Prince George's",
    ],
    '5': [
        "Anne Arundel",
        "Calvert",
        "Charles",
        "Prince George's",
        "St. Mary's",
    ],
    '6': [
        "Allegany",
        "Baltimore",
        "Carroll",
        "Frederick",
        "Garrett",
        "Harford",
        "Montgomery",
        "Washington",
    ],
    '7': [
        "Baltimore City",
        "Baltimore",
        "Howard",
    ],
    '8': [
        "Montgomery",
        "Prince George's",
    ],
}

congressional_districts_to_county_2011 = {
    '1': [
        "Baltimore",
        "Caroline",
        "Carroll",
        "Cecil",
        "Dorchester",
        "Harford",
        "Kent",
        "Queen Anne's",
        "Somerset",
        "Talbot",
        "Wicomico",
        "Worcester",
    ],
    '2': [
        "Anne Arundel",
        "Baltimore City",
        "Baltimore",
        "Harford",
        "Howard",
    ],
    '3': [
        "Anne Arundel",
        "Baltimore City",
        "Baltimore",
        "Harford",
        "Howard",
    ],
    '4': [
        "Anne Arundel",
        "Prince George's",
    ],
    '5': [
        "Anne Arundel",
        "Calvert",
        "Charles",
        "Prince George's",
        "St. Mary's",
    ],
    '6': [
        "Allegany",
        "Frederick",
        "Garrett",
        "Montgomery",
        "Washington",
    ],
    '7': [
        "Baltimore City",
        "Baltimore",
        "Howard",
    ],
    '8': [
        "Carroll",
        "Frederick",
        "Montgomery",
    ]
}
"""
Map of congressional districts to counties after the 2010 redistricting
process.

This was used as of October 20, 2011.

Source: http://planning.maryland.gov/redistricting/2010/congDist.shtml

"""

state_senate_district_to_county = {
    '1': [
        "Allegany",
        "Garrett",
        "Washington",
    ],
    '2': [
        "Washington",
    ],
    '3': [
        "Frederick",
        "Washington",
    ],
    '4': [
        "Carroll",
        "Frederick",
    ],
    '5': [
        "Baltimore",
        "Carroll",
    ],
    '6': [
        "Baltimore",
    ],
    '7': [
        "Baltimore",
        "Harford",
    ],
    '8': [
        "Baltimore",
    ],
    '9': [
        "Carroll",
        "Howard",
    ],
    '10': [
        "Baltimore",
    ],
    '11': [
        "Baltimore",
    ],
    '12': [
        "Baltimore",
        "Howard",
    ],
    '13': [
        "Howard",
    ],
    '14': [
        "Montgomery",
    ],
    '15': [
        "Montgomery",
    ],
    '16': [
        "Montgomery",
    ],
    '17': [
        "Montgomery",
    ],
    '18': [
        "Montgomery",
    ],
    '19': [
        "Montgomery",
    ],
    '20': [
        "Montgomery",
    ],
    '21': [
        "Anne Arundel",
        "Prince George's",
    ],
    '22': [
        "Prince George's",
    ],
    '23': [
        "Prince George's",
    ],
    '24': [
        "Prince George's",
    ],
    '25': [
        "Prince George's",
    ],
    '26': [
        "Prince George's",
    ],
    '27': [
        "Calvert",
        "Prince George's",
    ],
    '28': [
        "Charles",
    ],
    '29': [
        "Calvert",
        "Charles",
        "St. Mary's",
    ],
    '30': [
        "Anne Arundel",
    ],
    '31': [
        "Anne Arundel",
    ],
    '32': [
        "Anne Arundel",
    ],
    '33': [
        "Anne Arundel",
    ],
    '34': [
        "Cecil",
        "Harford",
    ],
    '35': [
        "Harford",
    ],
    '36': [
        "Caroline",
        "Cecil",
        "Kent",
        "Queen Anne's",
    ],
    '37': [
        "Caroline",
        "Dorchester",
        "Talbot",
        "Wicomico",
    ],
    '38': [
        "Somerset",
        "Wicomico",
        "Worcester",
    ],
    '39': [
        "Montgomery",
    ],
    '40': [
        "Baltimore City",
    ],
    '41': [
        "Baltimore City",
    ],
    '42': [
        "Baltimore",
    ],
    '43': [
        "Baltimore City",
    ],
    '44': [
        "Baltimore City",
    ],
    '45': [
        "Baltimore City",
    ],
    '46': [
        "Baltimore City",
    ],
    '47': [
        "Prince George's",
    ],
}

state_legislative_district_to_county = {
    '1A': [
        "Allegany",
        "Garrett",
    ],
    '1B': [
        "Allegany",
    ],
    '1C': [
        "Allegany",
        "Washington",
    ],
    '2A': [
        "Washington",
    ],
    '2B': [
        "Washington",
    ],
    '2C': [
        "Washington",
    ],
    '3A': [
        "Frederick",
    ],
    '3B': [
        "Frederick",
        "Washington",
    ],
    '4A': [
        "Frederick",
    ],
    '4B': [
        "Carroll",
        "Frederick",
    ],
    '5A': [
        "Carroll",
    ],
    '5B': [
        "Baltimore",
    ],
    '6': [
        "Baltimore",
    ],
    '7': [
        "Baltimore",
        "Harford",
    ],
    '8': [
        "Baltimore",
    ],
    '9A': [
        "Howard",
    ],
    '9B': [
        "Carroll",
    ],
    '10': [
        "Baltimore",
    ],
    '11': [
        "Baltimore",
    ],
    '12A': [
        "Baltimore",
        "Howard",
    ],
    '12B': [
        "Howard",
    ],
    '13': [
        "Howard",
    ],
    '14': [
        "Montgomery",
    ],
    '15': [
        "Montgomery",
    ],
    '16': [
        "Montgomery",
    ],
    '17': [
        "Montgomery",
    ],
    '18': [
        "Montgomery",
    ],
    '19': [
        "Montgomery",
    ],
    '20': [
        "Montgomery",
    ],
    '21': [
        "Anne Arundel",
        "Prince George's",
    ],
    '22': [
        "Prince George's",
    ],
    '23A': [
        "Prince George's",
    ],
    '23B': [
        "Prince George's",
    ],
    '24': [
        "Prince George's",
    ],
    '25': [
        "Prince George's",
    ],
    '26': [
        "Prince George's",
    ],
    '27A': [
        "Calvert",
        "Prince George's",
    ],
    '27B': [
        "Calvert",
    ],
    '28': [
        "Charles",
    ],
    '29A': [
        "Charles",
        "St. Mary's",
    ],
    '29B': [
        "St. Mary's",
    ],
    '29C': [
        "Calvert",
        "St. Mary's",
    ],
    '30': [
        "Anne Arundel",
    ],
    '31': [
        "Anne Arundel",
    ],
    '32': [
        "Anne Arundel",
    ],
    '33A': [
        "Anne Arundel",
    ],
    '33B': [
        "Anne Arundel",
    ],
    '34A': [
        "Cecil",
        "Harford",
    ],
    '34B': [
        "Cecil",
    ],
    '35A': [
        "Harford",
    ],
    '35B': [
        "Harford",
    ],
    '36': [
        "Caroline",
        "Cecil",
        "Kent",
        "Queen Anne's",
    ],
    '37A': [
        "Dorchester",
        "Wicomico",
    ],
    '37B': [
        "Caroline",
        "Dorchester",
        "Talbot",
        "Wicomico",
    ],
    '38A': [
        "Somerset",
        "Wicomico",
    ],
    '38B': [
        "Wicomico",
        "Worcester",
    ],
    '39': [
        "Montgomery",
    ],
    '40': [
        "Baltimore City",
    ],
    '41': [
        "Baltimore City",
    ],
    '42': [
        "Baltimore",
    ],
    '43': [
        "Baltimore City",
    ],
    '44': [
        "Baltimore City",
    ],
    '45': [
        "Baltimore City",
    ],
    '46': [
        "Baltimore City",
    ],
    '47': [
        "Prince George's",
    ],
}

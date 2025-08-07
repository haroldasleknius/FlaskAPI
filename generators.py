import random
import iso3166
from datetime import date
import coolname
from faker import Faker

faker = Faker("en_GB")
ALLOWED_TYPES = {
    "integer",
    "name",
    "dob",
    "country",
    "ip",
    "game",
    "role",
    "org",
    "trophies",
    "gamertag",
}

PLACEMENTS = ["Winner", "Runner-up", "3rd-4th", "Top 8", "Top 16"]

GAMES = {
    "league_of_legends": {
        "roles": ["Top", "Jungle", "Mid", "ADC", "Support"],
        "orgs": [
            "Gen.G Esports",
            "T1",
            "Anyone's Legend",
            "Hanwha Life Esports",
            "BiliBili Gaming DREAMSMART",
            "FlyQuest",
            "Top Esports",
            "CTBC Flying Oyster",
            "kt Rolster",
            "Cloud9",
            "PSG Talon",
            "DPlus KIA",
            "G2 Esports",
            "Invictus Gaming",
            "Weibo Gaming",
            "Beijing JDG Intel Esports",
            "Karmine Corp",
            "GAM Esports",
            "Team Liquid",
            "Movistar KOI",
            "Fnatic",
            "Ninjas In Pyjamas",
            "FunPlus Phoenix",
            "100 Thieves",
            "Team BDS",
            "Team Vitality",
            "Dignitas",
            "Disguised",
            "GIANTX",
        ],
        "tournaments": [
            "Spring Split",
            "Summer Split",
            "Mid-Season Invitational",
            "World Championship",
            "All-Star Event",
            "Regional Finals",
        ],
    },
    "cs2": {
        "roles": ["AWPer", "IGL", "Entry", "Rifler", "Support", "Lurker", "Anchor"],
        "orgs": [
            "Team Vitality",
            "Spirit",
            "MOUZ",
            "The MongolZ",
            "Natus Vincere",
            "FURIA",
            "Falcons",
            "FaZe",
            "Astralis",
            "TYLOO",
            "Aurora",
            "G2 Esports",
            "3DMAX",
            "GamerLegion",
            "Lynn Vision",
            "PaiN Gaming",
            "Virtus.pro",
            "HEROIC",
            "Ninjas in Pyjamas",
            "Team Liquid",
            "FlyQuest",
            "Fnatic",
        ],
        "tournaments": [
            "Major",
            "ESL Pro League",
            "IEM",
            "BLAST Premier",
            "DreamHack Masters",
            "Regional",
        ],
    },
}


def generate_game(value):
    option = value.get("option")
    if option is not None:
        opt = option.strip().lower()
        if opt == "lol":
            return "league_of_legends"
        if opt == "cs2":
            return "cs2"
        raise ValueError("option must be 'lol' or 'cs2'")

    return random.choice(list(GAMES.keys()))


def generate_role(value, document, name):
    game = document.get(name)
    if not game:
        raise ValueError("role requires 'game' to be generated first")

    role = value.get("custom")
    if role is not None:
        return role

    roles = GAMES[game].get("roles")
    return random.choice(roles)


def generate_org(value, document, name):
    game = document.get(name)
    if not game:
        raise ValueError("role requires 'game' to be generated first")

    org = value.get("custom")
    if org is not None:
        return org

    orgs = GAMES[game].get("orgs")
    return random.choice(orgs)


def generate_trophies(value, document, name):
    game = document.get(name)
    if not game:
        raise ValueError("role requires 'game' to be generated first")

    amount = value.get("amount")

    if amount is not None:
        amount = int(amount)
    else:
        low = int(value.get("min", 1))
        high = int(value.get("max", 10))
        amount = random.randint(low, high)

    trophies = []
    tournaments = GAMES[game].get("tournaments")

    start_year = int(value.get("start_year", 2012))
    end_year = int(value.get("end_year", date.today().year))

    for _ in range(amount):
        d = faker.date_between_dates(
            date_start=date(start_year, 1, 1), date_end=date(end_year, 12, 31)
        )
        d = d.isoformat()
        tournament = random.choice(tournaments)
        trophies.append(
            {"tournament": f"{tournament} {d}", "placement": random.choice(PLACEMENTS)}
        )

    return trophies


def generate_gamer_tag():
    word = coolname.generate_slug(1).capitalize()
    if random.random() < 0.5:
        word += str(random.randint(1, 999))
    return word


def generate_integer(value):
    low = int(value.get("min", 1))
    high = int(value.get("max", 50000))
    return random.randint(low, high)


def generate_name(value):
    name_format = value.get("format", "full")
    match name_format:
        case "first":
            return faker.first_name()
        case "last":
            return faker.last_name()
        case "full":
            return f"{faker.first_name()} {faker.last_name()}"
        case "gamertag":
            return generate_gamer_tag()
        case _:
            raise ValueError("invalid name format entered")


def generate_dob(value):
    min_age = int(value.get("min", 1))
    max_age = int(value.get("max", 100))
    dob = faker.date_of_birth(minimum_age=min_age, maximum_age=max_age)
    return dob.isoformat()


def generate_ip(value):
    version = value.get("version", 4)
    visibility = str(value.get("visibility", None)).lower()

    match (version, visibility):
        case (4, "public"):
            return faker.ipv4(private=False)
        case (4, "private"):
            return faker.ipv4(private=True)
        case (6, _):
            return faker.ipv6()
        case (4, _):
            return faker.ipv4()
        case _:
            raise ValueError("ip version must be 4 or 6")


def generate_country(value):
    ### alpha2 = US, alpha3 = USA, name = United States
    country_format = value.get("format", "alpha2")
    countries = value.get("countries", None)

    if countries is not None:
        option = random.choice(countries)
        option = option.upper()
    else:
        option = faker.country_code()

    country = iso3166.countries.get(option)

    match country_format:
        case "alpha2":
            return option
        case "alpha3":
            return country.alpha3
        case "name":
            return country.name
        case _:
            raise ValueError("unsupported country format")


def make_document(key_pairs):
    """
    {
      "schema_name": "Haroldas's Generator",
      "fields": {
        "nickname": {"type":"name", "format":"gamertag"},
        "name": {"type": "name", "format": "full"},
        "id": {"type": "integer","min": 1,"max": 9999},
        "dob": {"type": "dob","min": 12,"max": 100},
        "ip": {"type": "ip","version": 4,"visibility": "public"},
        "country_code": {"type":"country", "format":"alpha2", "countries":["US","GB","FR"]},
        "game": {"type":"game", "option":"lol"},
        "role": {"type":"role", "custom":"Sniper"},
        "org": {"type":"org", "custom":"Fnatic"},
        "trophies": {"type":"trophies", "min": 1, "max": 5, "start_year":2020}
      }
    }
    """

    document = {}
    game = None
    for field_name, value in key_pairs.items():
        data_type = value["type"]

        match data_type:
            case "integer":
                document[field_name] = generate_integer(value)
            case "name":
                document[field_name] = generate_name(value)
            case "dob":
                document[field_name] = generate_dob(value)
            case "ip":
                document[field_name] = generate_ip(value)
            case "country":
                document[field_name] = generate_country(value)

            case "game":
                document[field_name] = generate_game(value)
                game = field_name
            case "role":
                document[field_name] = generate_role(value, document, game)
            case "org":
                document[field_name] = generate_org(value, document, game)
            case "trophies":
                document[field_name] = generate_trophies(value, document, game)
            case _:
                raise ValueError(f"Unsupported type: {data_type}")

    return document


def process_fields(fields):
    field_map = {}
    bad_types = []
    for field_name, value in fields.items():
        if isinstance(value, str):
            value = {"type": value}

        data_type = value["type"]
        if data_type not in ALLOWED_TYPES:
            bad_types.append(data_type)
            continue

        field_map[field_name] = value

    return field_map, bad_types

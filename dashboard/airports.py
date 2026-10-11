"""Global Airports database and geodetic route resolver for SkyPulse."""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class Airport:
    iata: str
    icao: str
    name: str
    city: str
    country: str
    latitude: float
    longitude: float


# Comprehensive global airport registry (Morocco, Europe, Mediterranean, Americas, Middle East, Asia)
AIRPORTS_DB: list[Airport] = [
    # --- Morocco ---
    Airport("CMN", "GMMN", "Mohammed V International", "Casablanca", "Morocco", 33.3675, -7.58997),
    Airport("RAK", "GMMX", "Menara Airport", "Marrakech", "Morocco", 31.6069, -8.0363),
    Airport("AGA", "GMAD", "Al Massira Airport", "Agadir", "Morocco", 30.3250, -9.4131),
    Airport("TNG", "GMTT", "Ibn Battouta Airport", "Tangier", "Morocco", 35.7269, -5.9169),
    Airport("FEZ", "GMFF", "Saïss Airport", "Fès", "Morocco", 33.9273, -4.9779),
    Airport("RBA", "GMME", "Rabat-Salé Airport", "Rabat", "Morocco", 34.0515, -6.7515),
    Airport("OUD", "GMFO", "Angads Airport", "Oujda", "Morocco", 34.7872, -1.9239),
    Airport("NDR", "GMMW", "El Aroui Airport", "Nador", "Morocco", 34.9889, -3.0286),
    Airport("OZZ", "GMMZ", "Ouarzazate Airport", "Ouarzazate", "Morocco", 30.9392, -6.9094),
    Airport("EUN", "GMML", "Hassan I Airport", "Laâyoune", "Morocco", 27.1517, -13.2192),
    Airport("VIL", "GMMH", "Dakhla Airport", "Dakhla", "Morocco", 23.7183, -15.9322),
    Airport("ERH", "GMFK", "Moulay Ali Cherif Airport", "Errachidia", "Morocco", 31.9472, -4.3983),

    # --- Spain ---
    Airport("MAD", "LEMD", "Adolfo Suárez Madrid-Barajas", "Madrid", "Spain", 40.4839, -3.5680),
    Airport("BCN", "LEBL", "Josep Tarradellas Barcelona-El Prat", "Barcelona", "Spain", 41.2974, 2.0833),
    Airport("AGP", "LEMG", "Málaga-Costa del Sol", "Málaga", "Spain", 36.6749, -4.4991),
    Airport("SVQ", "LEZL", "Seville Airport", "Seville", "Spain", 37.4180, -5.8931),
    Airport("VLC", "LEVC", "Valencia Airport", "Valencia", "Spain", 39.4893, -0.4816),
    Airport("ALC", "LEAL", "Alicante-Elche Miguel Hernández", "Alicante", "Spain", 38.2822, -0.5582),
    Airport("PMI", "LEPA", "Palma de Mallorca", "Palma de Mallorca", "Spain", 39.5517, 2.7388),
    Airport("IBZ", "LEIB", "Ibiza Airport", "Ibiza", "Spain", 38.8729, 1.3731),
    Airport("MAH", "LEMH", "Menorca Airport", "Menorca", "Spain", 39.8626, 4.2186),
    Airport("BIO", "LEBB", "Bilbao Airport", "Bilbao", "Spain", 43.3011, -2.9106),
    Airport("SCQ", "LEST", "Santiago-Rosalía de Castro", "Santiago de Compostela", "Spain", 42.8963, -8.4151),
    Airport("LPA", "GCLP", "Gran Canaria Airport", "Las Palmas", "Spain", 27.9319, -15.3866),
    Airport("TFS", "GCTS", "Tenerife South Airport", "Tenerife", "Spain", 28.0445, -16.5725),
    Airport("TFN", "GCXO", "Tenerife North-Ciudad de La Laguna", "Tenerife", "Spain", 28.4827, -16.3415),
    Airport("ACE", "GCRR", "César Manrique-Lanzarote", "Lanzarote", "Spain", 28.9455, -13.6052),
    Airport("GRX", "LEGR", "Federico García Lorca Granada-Jaén", "Granada", "Spain", 37.1887, -3.7772),

    # --- Portugal ---
    Airport("LIS", "LPPT", "Humberto Delgado Airport", "Lisbon", "Portugal", 38.7756, -9.1354),
    Airport("OPO", "LPPR", "Francisco Sá Carneiro Airport", "Porto", "Portugal", 41.2481, -8.6814),
    Airport("FAO", "LPFR", "Faro Algarve Airport", "Faro", "Portugal", 37.0144, -7.9659),
    Airport("FNC", "LPMA", "Cristiano Ronaldo Madeira Airport", "Funchal", "Portugal", 32.6979, -16.7745),
    Airport("PDL", "LPPD", "João Paulo II Airport", "Ponta Delgada", "Portugal", 37.7412, -25.6979),

    # --- France ---
    Airport("CDG", "LFPG", "Charles de Gaulle Airport", "Paris", "France", 49.0097, 2.5479),
    Airport("ORY", "LFPO", "Paris Orly Airport", "Paris", "France", 48.7262, 2.3652),
    Airport("NCE", "LFMN", "Nice Côte d'Azur Airport", "Nice", "France", 43.6653, 7.2150),
    Airport("MRS", "LFML", "Marseille Provence Airport", "Marseille", "France", 43.4367, 5.2150),
    Airport("TLS", "LFBO", "Toulouse-Blagnac Airport", "Toulouse", "France", 43.6350, 1.3678),
    Airport("LYS", "LFLL", "Lyon-Saint Exupéry Airport", "Lyon", "France", 45.7256, 5.0811),
    Airport("BOD", "LFBD", "Bordeaux-Mérignac Airport", "Bordeaux", "France", 44.8283, -0.7156),
    Airport("NTE", "LFRS", "Nantes Atlantique Airport", "Nantes", "France", 47.1532, -1.6107),
    Airport("MPL", "LFMT", "Montpellier-Méditerranée Airport", "Montpellier", "France", 43.5762, 3.9630),
    Airport("LIL", "LFQQ", "Lille Airport", "Lille", "France", 50.5619, 3.0894),

    # --- Italy ---
    Airport("FCO", "LIRF", "Leonardo da Vinci-Fiumicino", "Rome", "Italy", 41.8003, 12.2389),
    Airport("CIA", "LIRA", "Rome Ciampino Airport", "Rome", "Italy", 41.7994, 12.5949),
    Airport("MXP", "LIMC", "Milan Malpensa Airport", "Milan", "Italy", 45.6301, 8.7255),
    Airport("LIN", "LIML", "Milan Linate Airport", "Milan", "Italy", 45.4451, 9.2767),
    Airport("BGY", "LIME", "Milan Bergamo Orio al Serio", "Bergamo", "Italy", 45.6739, 9.7042),
    Airport("VCE", "LIPZ", "Venice Marco Polo Airport", "Venice", "Italy", 45.5053, 12.3519),
    Airport("NAP", "LIRN", "Naples International Airport", "Naples", "Italy", 40.8860, 14.2908),
    Airport("BLQ", "LIPE", "Bologna Guglielmo Marconi", "Bologna", "Italy", 44.5354, 11.2887),
    Airport("CTA", "LICC", "Catania-Fontanarossa Airport", "Catania", "Italy", 37.4668, 15.0664),
    Airport("PMO", "LICJ", "Falcone Borsellino Airport", "Palermo", "Italy", 38.1760, 13.0910),
    Airport("FLR", "LIRQ", "Florence Airport", "Florence", "Italy", 43.8100, 11.2051),

    # --- Algeria & Tunisia ---
    Airport("ALG", "DAAG", "Houari Boumediene Airport", "Algiers", "Algeria", 36.6910, 3.2154),
    Airport("ORN", "DAOO", "Ahmed Ben Bella Airport", "Oran", "Algeria", 35.6239, -0.6211),
    Airport("CZL", "DABC", "Mohamed Boudiaf Airport", "Constantine", "Algeria", 36.2761, 6.6204),
    Airport("TUN", "DTTA", "Tunis-Carthage Airport", "Tunis", "Tunisia", 36.8510, 10.2272),
    Airport("DJE", "DTTJ", "Djerba-Zarzis International", "Djerba", "Tunisia", 33.8750, 10.7755),
    Airport("NBE", "DTNH", "Enfidha-Hammamet International", "Enfidha", "Tunisia", 36.0758, 10.4386),

    # --- United Kingdom & Ireland ---
    Airport("LHR", "EGLL", "Heathrow Airport", "London", "United Kingdom", 51.4700, -0.4543),
    Airport("LGW", "EGKK", "Gatwick Airport", "London", "United Kingdom", 51.1537, -0.1821),
    Airport("STN", "EGSS", "Stansted Airport", "London", "United Kingdom", 51.8860, 0.2389),
    Airport("MAN", "EGCC", "Manchester Airport", "Manchester", "United Kingdom", 53.3537, -2.2750),
    Airport("BHX", "EGBB", "Birmingham Airport", "Birmingham", "United Kingdom", 52.4539, -1.7480),
    Airport("EDI", "EGPH", "Edinburgh Airport", "Edinburgh", "United Kingdom", 55.9500, -3.3725),
    Airport("DUB", "EIDW", "Dublin Airport", "Dublin", "Ireland", 53.4264, -6.2499),

    # --- Central & Northern Europe ---
    Airport("AMS", "EHAM", "Amsterdam Airport Schiphol", "Amsterdam", "Netherlands", 52.3105, 4.7683),
    Airport("FRA", "EDDF", "Frankfurt Airport", "Frankfurt", "Germany", 50.0379, 8.5622),
    Airport("MUC", "EDDM", "Munich Airport", "Munich", "Germany", 48.3537, 11.7750),
    Airport("BER", "EDDB", "Berlin Brandenburg Airport", "Berlin", "Germany", 52.3667, 13.5033),
    Airport("DUS", "EDDL", "Düsseldorf Airport", "Düsseldorf", "Germany", 51.2895, 6.7668),
    Airport("BRU", "EBBR", "Brussels Airport", "Brussels", "Belgium", 50.9010, 4.4844),
    Airport("GVA", "LSGG", "Geneva Airport", "Geneva", "Switzerland", 46.2381, 6.1089),
    Airport("ZRH", "LSZH", "Zurich Airport", "Zurich", "Switzerland", 47.4582, 8.5555),
    Airport("VIE", "LOWW", "Vienna International Airport", "Vienna", "Austria", 48.1103, 16.5697),
    Airport("CPH", "EKCH", "Copenhagen Airport", "Copenhagen", "Denmark", 55.6180, 12.6560),
    Airport("ARN", "ESSA", "Stockholm Arlanda Airport", "Stockholm", "Sweden", 59.6498, 17.9238),
    Airport("OSL", "ENGM", "Oslo Airport Gardermoen", "Oslo", "Norway", 60.1975, 11.1004),
    Airport("WAW", "EPWA", "Warsaw Chopin Airport", "Warsaw", "Poland", 52.1672, 20.9679),
    Airport("PRG", "LKPR", "Václav Havel Airport Prague", "Prague", "Czechia", 50.1008, 14.2600),
    Airport("ATH", "LGAV", "Athens International Airport", "Athens", "Greece", 37.9364, 23.9445),
    Airport("IST", "LTFM", "Istanbul Airport", "Istanbul", "Turkey", 41.2753, 28.7519),
    Airport("SAW", "LTFJ", "Istanbul Sabiha Gökçen", "Istanbul", "Turkey", 40.8986, 29.3092),

    # --- Middle East & Gulf ---
    Airport("DXB", "OMDB", "Dubai International Airport", "Dubai", "United Arab Emirates", 25.2532, 55.3657),
    Airport("AUH", "OMAA", "Zayed International Airport", "Abu Dhabi", "United Arab Emirates", 24.4330, 54.6511),
    Airport("DOH", "OTHH", "Hamad International Airport", "Doha", "Qatar", 25.2731, 51.6081),
    Airport("JED", "OEJN", "King Abdulaziz International", "Jeddah", "Saudi Arabia", 21.6796, 39.1565),
    Airport("RUH", "OERK", "King Khalid International", "Riyadh", "Saudi Arabia", 24.9576, 46.6988),
    Airport("CAI", "HECA", "Cairo International Airport", "Cairo", "Egypt", 30.1219, 31.4056),

    # --- North America ---
    Airport("JFK", "KJFK", "John F. Kennedy International", "New York", "United States", 40.6413, -73.7781),
    Airport("EWR", "KEWR", "Newark Liberty International", "New York", "United States", 40.6895, -74.1745),
    Airport("BOS", "KBOS", "Boston Logan International", "Boston", "United States", 42.3656, -71.0096),
    Airport("ORD", "KORD", "O'Hare International Airport", "Chicago", "United States", 41.9742, -87.9073),
    Airport("ATL", "KATL", "Hartsfield-Jackson Atlanta", "Atlanta", "United States", 33.6407, -84.4277),
    Airport("MIA", "KMIA", "Miami International Airport", "Miami", "United States", 25.7959, -80.2870),
    Airport("LAX", "KLAX", "Los Angeles International", "Los Angeles", "United States", 33.9416, -118.4085),
    Airport("SFO", "KSFO", "San Francisco International", "San Francisco", "United States", 37.6213, -122.3790),
    Airport("DFW", "KDFW", "Dallas/Fort Worth International", "Dallas", "United States", 32.8998, -97.0403),
    Airport("YYZ", "CYYZ", "Toronto Pearson International", "Toronto", "Canada", 43.6777, -79.6248),
    Airport("YUL", "CYUL", "Montréal-Trudeau International", "Montréal", "Canada", 45.4706, -73.7408),

    # --- Asia & Latin America ---
    Airport("DEL", "VIDP", "Indira Gandhi International", "Delhi", "India", 28.5562, 77.1000),
    Airport("BOM", "VABB", "Chhatrapati Shivaji Maharaj", "Mumbai", "India", 19.0896, 72.8656),
    Airport("SIN", "WSSS", "Singapore Changi Airport", "Singapore", "Singapore", 1.3644, 103.9915),
    Airport("BKK", "VTBS", "Suvarnabhumi Airport", "Bangkok", "Thailand", 13.6900, 100.7501),
    Airport("HND", "RJTT", "Tokyo Haneda Airport", "Tokyo", "Japan", 35.5494, 139.7798),
    Airport("NRT", "RJAA", "Narita International Airport", "Tokyo", "Japan", 35.7647, 140.3863),
    Airport("ICN", "RKSI", "Incheon International Airport", "Seoul", "South Korea", 37.4602, 126.4407),
    Airport("GRU", "SBGR", "São Paulo/Guarulhos International", "São Paulo", "Brazil", -23.4356, -46.4731),
    Airport("BOG", "SKBO", "El Dorado International Airport", "Bogotá", "Colombia", 4.7016, -74.1469),
    Airport("LIM", "SPJC", "Jorge Chávez International", "Lima", "Peru", -12.0219, -77.1143),
    Airport("SCL", "SCEL", "Arturo Merino Benítez", "Santiago", "Chile", -33.3930, -70.7858),
    Airport("EZE", "SAEZ", "Ministro Pistarini International", "Buenos Aires", "Argentina", -34.8222, -58.5358),
]

AIRPORTS_BY_IATA: dict[str, Airport] = {a.iata: a for a in AIRPORTS_DB}

# Airline ICAO code to Primary Airline Hub mappings
AIRLINE_HUBS: dict[str, tuple[str, str]] = {
    "RAM": ("CMN", "Royal Air Maroc"),
    "MAC": ("RAK", "Air Arabia Maroc"),
    "IBE": ("MAD", "Iberia"),
    "IBS": ("MAD", "Iberia Express"),
    "ANE": ("MAD", "Air Nostrum"),
    "VLG": ("BCN", "Vueling"),
    "AEA": ("MAD", "Air Europa"),
    "TAP": ("LIS", "TAP Air Portugal"),
    "RZO": ("PDL", "SATA Azores Airlines"),
    "AFR": ("CDG", "Air France"),
    "TOY": ("ORY", "Transavia France"),
    "EZY": ("LGW", "easyJet"),
    "EZS": ("GVA", "easyJet Switzerland"),
    "EJU": ("VIE", "easyJet Europe"),
    "RYR": ("STN", "Ryanair"),
    "RYS": ("WAW", "Buzz / Ryanair"),
    "MAY": ("MLA", "Malta Air / Ryanair"),
    "BAW": ("LHR", "British Airways"),
    "DLH": ("FRA", "Lufthansa"),
    "LHA": ("MUC", "Lufthansa"),
    "EWG": ("DUS", "Eurowings"),
    "SWR": ("ZRH", "Swiss International"),
    "AUA": ("VIE", "Austrian Airlines"),
    "KLM": ("AMS", "KLM Royal Dutch Airlines"),
    "TRA": ("AMS", "Transavia"),
    "BEL": ("BRU", "Brussels Airlines"),
    "ITY": ("FCO", "ITA Airways"),
    "ISS": ("MXP", "Air Italy"),
    "AHY": ("ALG", "Air Algérie"),
    "TAR": ("TUN", "Tunisair"),
    "MSR": ("CAI", "EgyptAir"),
    "THY": ("IST", "Turkish Airlines"),
    "PGT": ("SAW", "Pegasus Airlines"),
    "UAE": ("DXB", "Emirates"),
    "ETD": ("AUH", "Etihad Airways"),
    "QTR": ("DOH", "Qatar Airways"),
    "SVA": ("JED", "Saudia"),
    "AIC": ("DEL", "Air India"),
    "IGO": ("DEL", "IndiGo"),
    "UAL": ("EWR", "United Airlines"),
    "AAL": ("DFW", "American Airlines"),
    "DAL": ("ATL", "Delta Air Lines"),
    "JBU": ("JFK", "JetBlue"),
    "LPE": ("LIM", "LATAM Peru"),
    "LAN": ("SCL", "LATAM Chile"),
    "TAM": ("GRU", "LATAM Brasil"),
}


def haversine_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate Great-Circle distance in km between two lat/lon points."""
    r_earth = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    return 2.0 * r_earth * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))


def initial_bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate initial compass bearing in degrees (0..360) from point 1 to point 2."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dlambda = math.radians(lon2 - lon1)
    y = math.sin(dlambda) * math.cos(phi2)
    x = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(dlambda)
    b = math.degrees(math.atan2(y, x))
    return (b + 360.0) % 360.0


def angle_diff_deg(a: float, b: float) -> float:
    """Angular difference between two compass bearings in degrees (0..180)."""
    diff = abs((a - b + 180.0) % 360.0 - 180.0)
    return diff


def resolve_flight_route(
    lat: float,
    lon: float,
    track: float,
    callsign: str | None = None,
    origin_country: str | None = None,
    altitude_m: float | None = None,
    vertical_rate: float | None = None,
) -> tuple[Airport, Airport, str]:
    """
    Resolve real Origin and Destination Airports using geodetic heading vector raycasting
    and airline callsign network hubs.

    Returns:
        (origin_airport, destination_airport, airline_name)
    """
    callsign_clean = (callsign or "").strip().upper()
    prefix3 = callsign_clean[:3] if len(callsign_clean) >= 3 else ""

    hub_info = AIRLINE_HUBS.get(prefix3)
    airline_name = hub_info[1] if hub_info else ("Commercial Airline" if callsign_clean else "General Aviation")
    hub_iata = hub_info[0] if hub_info else None
    hub_airport = AIRPORTS_BY_IATA.get(hub_iata) if hub_iata else None

    # Step 1: Detect immediate proximity for takeoff / landing (< 65 km, < 3500m)
    nearest_airport = None
    min_dist = float("inf")
    for apt in AIRPORTS_DB:
        d = haversine_distance_km(lat, lon, apt.latitude, apt.longitude)
        if d < min_dist:
            min_dist = d
            nearest_airport = apt

    is_low_altitude = (altitude_m is not None and altitude_m < 3500.0) or (min_dist < 40.0)
    is_descending = vertical_rate is not None and vertical_rate < -1.5
    is_climbing = vertical_rate is not None and vertical_rate > 1.5

    # Step 2: Ray-cast along track (Heading vector) to find candidate Destination Airports
    # Forward heading cone: bearing difference <= 55 degrees, distance between 60km and 3500km
    forward_candidates = []
    # Backward heading cone: bearing difference from reverse heading <= 55 degrees
    reverse_track = (track + 180.0) % 360.0
    backward_candidates = []

    for apt in AIRPORTS_DB:
        d = haversine_distance_km(lat, lon, apt.latitude, apt.longitude)
        if d < 15.0:
            continue  # Too close to be forward cruising target

        b_to_apt = initial_bearing_deg(lat, lon, apt.latitude, apt.longitude)
        fwd_diff = angle_diff_deg(track, b_to_apt)
        bwd_diff = angle_diff_deg(reverse_track, b_to_apt)

        # Score forward: lower diff & balanced distance
        if fwd_diff <= 55.0 and 60.0 <= d <= 4500.0:
            # Score formula: aligns well with heading vector, prefer realistic cruise distance
            score = fwd_diff * 1.5 + (abs(d - 600.0) / 100.0)
            forward_candidates.append((score, d, apt))

        if bwd_diff <= 55.0 and 60.0 <= d <= 4500.0:
            score = bwd_diff * 1.5 + (abs(d - 600.0) / 100.0)
            backward_candidates.append((score, d, apt))

    forward_candidates.sort(key=lambda x: x[0])
    backward_candidates.sort(key=lambda x: x[0])

    dest_airport = forward_candidates[0][2] if forward_candidates else None
    origin_airport = backward_candidates[0][2] if backward_candidates else None

    # Step 3: Handle terminal climb / descent
    if is_low_altitude and nearest_airport:
        if is_descending:
            dest_airport = nearest_airport
        elif is_climbing:
            origin_airport = nearest_airport

    # Step 4: Airline hub refinement
    if hub_airport:
        # If plane is flying toward its hub, destination is likely hub
        b_to_hub = initial_bearing_deg(lat, lon, hub_airport.latitude, hub_airport.longitude)
        if angle_diff_deg(track, b_to_hub) <= 45.0:
            dest_airport = hub_airport
        elif angle_diff_deg(reverse_track, b_to_hub) <= 45.0:
            origin_airport = hub_airport

    # Fallback defaults if cones are empty
    if not dest_airport:
        # Fallback to prominent international destination in forward direction
        dest_airport = AIRPORTS_BY_IATA.get("MAD") or AIRPORTS_DB[0]
    if not origin_airport:
        origin_airport = AIRPORTS_BY_IATA.get("CMN") or AIRPORTS_DB[1]

    # Ensure origin != destination
    if origin_airport.iata == dest_airport.iata:
        # Pick second best forward candidate or sensible pair
        if len(forward_candidates) > 1 and forward_candidates[1][2].iata != origin_airport.iata:
            dest_airport = forward_candidates[1][2]
        else:
            # Fallback to major counterpart
            dest_airport = AIRPORTS_BY_IATA.get("MAD") if origin_airport.iata != "MAD" else AIRPORTS_BY_IATA.get("CMN")

    return origin_airport, dest_airport, airline_name

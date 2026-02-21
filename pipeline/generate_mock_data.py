#!/usr/bin/env python3
"""
generate_mock_data.py

Generates mock broadband data for all 254 Texas counties:
  - data/processed/counties.geojson  (GeoJSON FeatureCollection)
  - data/processed/providers.json    (GEOID -> list of provider objects)

Centroids are approximate real-world lat/lon values for each county.
"""

import json
import math
import random
import os

random.seed(42)

# ---------------------------------------------------------------------------
# 1. County data: (FIPS code, Name, centroid_lat, centroid_lon)
#    Centroids sourced from USGS/Census approximate county seat / centroid.
# ---------------------------------------------------------------------------
COUNTIES = [
    (48001, "Anderson",        31.832, -95.654),
    (48003, "Andrews",         32.305, -102.638),
    (48005, "Angelina",        31.254, -94.609),
    (48007, "Aransas",         28.100, -97.000),
    (48009, "Archer",          33.614, -98.689),
    (48011, "Armstrong",       34.965, -101.357),
    (48013, "Atascosa",        28.894, -98.527),
    (48015, "Austin",          29.888, -96.277),
    (48017, "Bailey",          34.068, -102.828),
    (48019, "Bandera",         29.748, -99.246),
    (48021, "Bastrop",         30.103, -97.311),
    (48023, "Baylor",          33.617, -99.214),
    (48025, "Bee",             28.418, -97.741),
    (48027, "Bell",            31.046, -97.478),
    (48029, "Bexar",           29.449, -98.520),
    (48031, "Blanco",          30.259, -98.394),
    (48033, "Borden",          32.744, -101.431),
    (48035, "Bosque",          31.901, -97.636),
    (48037, "Bowie",           33.447, -94.171),
    (48039, "Brazoria",        29.170, -95.438),
    (48041, "Brazos",          30.661, -96.302),
    (48043, "Brewster",        29.974, -103.253),
    (48045, "Briscoe",         34.529, -100.975),
    (48047, "Brooks",          27.032, -98.222),
    (48049, "Brown",           31.776, -99.012),
    (48051, "Burleson",        30.500, -96.651),
    (48053, "Burnet",          30.788, -98.238),
    (48055, "Caldwell",        29.837, -97.620),
    (48057, "Calhoun",         28.432, -96.622),
    (48059, "Callahan",        32.297, -99.372),
    (48061, "Cameron",         26.131, -97.482),
    (48063, "Camp",            32.975, -94.978),
    (48065, "Carson",          35.404, -101.357),
    (48067, "Cass",            33.076, -94.342),
    (48069, "Castro",          34.079, -102.263),
    (48071, "Chambers",        29.702, -94.676),
    (48073, "Cherokee",        31.844, -95.163),
    (48075, "Childress",       34.527, -100.214),
    (48077, "Clay",            33.786, -98.210),
    (48079, "Cochran",         33.604, -102.825),
    (48081, "Coke",            31.888, -100.524),
    (48083, "Coleman",         31.777, -99.464),
    (48085, "Collin",          33.190, -96.572),
    (48087, "Collingsworth",   34.958, -100.270),
    (48089, "Colorado",        29.621, -96.535),
    (48091, "Comal",           29.807, -98.248),
    (48093, "Comanche",        31.948, -98.557),
    (48095, "Concho",          31.318, -99.881),
    (48097, "Cooke",           33.647, -97.213),
    (48099, "Coryell",         31.393, -97.797),
    (48101, "Cottle",          33.967, -100.281),
    (48103, "Crane",           31.431, -102.348),
    (48105, "Crockett",        30.713, -101.399),
    (48107, "Crosby",          33.611, -101.299),
    (48109, "Culberson",       30.754, -104.524),
    (48111, "Dallam",          36.278, -102.601),
    (48113, "Dallas",          32.767, -96.778),
    (48115, "Dawson",          32.743, -101.946),
    (48117, "Deaf Smith",      34.965, -102.600),
    (48119, "Delta",           33.388, -95.673),
    (48121, "Denton",          33.205, -97.133),
    (48123, "DeWitt",          29.083, -97.358),
    (48125, "Dickens",         33.617, -100.781),
    (48127, "Dimmit",          28.430, -99.757),
    (48129, "Donley",          34.965, -100.814),
    (48131, "Duval",           27.681, -98.512),
    (48133, "Eastland",        32.326, -98.821),
    (48135, "Ector",           31.869, -102.544),
    (48137, "Edwards",         29.982, -100.305),
    (48139, "Ellis",           32.348, -96.808),
    (48141, "El Paso",         31.769, -106.237),
    (48143, "Erath",           32.238, -98.207),
    (48145, "Falls",           31.252, -96.938),
    (48147, "Fannin",          33.596, -96.110),
    (48149, "Fayette",         29.878, -96.924),
    (48151, "Fisher",          32.741, -100.401),
    (48153, "Floyd",           33.975, -101.306),
    (48155, "Foard",           33.974, -99.778),
    (48157, "Fort Bend",       29.528, -95.772),
    (48159, "Franklin",        33.175, -95.221),
    (48161, "Freestone",       31.705, -96.148),
    (48163, "Frio",            28.867, -99.109),
    (48165, "Gaines",          32.743, -102.638),
    (48167, "Galveston",       29.351, -94.834),
    (48169, "Garza",           33.178, -101.299),
    (48171, "Gillespie",       30.319, -98.944),
    (48173, "Glasscock",       31.869, -101.522),
    (48175, "Goliad",          28.658, -97.389),
    (48177, "Gonzales",        29.456, -97.493),
    (48179, "Gray",            35.401, -100.812),
    (48181, "Grayson",         33.644, -96.677),
    (48183, "Gregg",           32.481, -94.820),
    (48185, "Grimes",          30.541, -95.987),
    (48187, "Guadalupe",       29.573, -97.940),
    (48189, "Hale",            34.068, -101.824),
    (48191, "Hall",            34.530, -100.681),
    (48193, "Hamilton",        31.702, -98.115),
    (48195, "Hansford",        36.278, -101.357),
    (48197, "Hardeman",        34.291, -99.748),
    (48199, "Hardin",          30.307, -94.377),
    (48201, "Harris",          29.847, -95.398),
    (48203, "Harrison",        32.548, -94.380),
    (48205, "Hartley",         35.840, -102.601),
    (48207, "Haskell",         33.177, -99.728),
    (48209, "Hays",            30.000, -98.030),
    (48211, "Hemphill",        35.834, -100.271),
    (48213, "Henderson",       32.215, -95.854),
    (48215, "Hidalgo",         26.395, -98.186),
    (48217, "Hill",            31.991, -97.134),
    (48219, "Hockley",         33.605, -102.344),
    (48221, "Hood",            32.450, -97.827),
    (48223, "Hopkins",         33.150, -95.560),
    (48225, "Houston",         31.316, -95.432),
    (48227, "Howard",          32.303, -101.431),
    (48229, "Hudspeth",        31.458, -105.399),
    (48231, "Hunt",            33.130, -96.087),
    (48233, "Hutchinson",      35.840, -101.357),
    (48235, "Irion",           31.297, -100.987),
    (48237, "Jack",            33.237, -98.169),
    (48239, "Jackson",         28.957, -96.577),
    (48241, "Jasper",          30.743, -94.015),
    (48243, "Jeff Davis",      30.712, -104.134),
    (48245, "Jefferson",       29.847, -94.154),
    (48247, "Jim Hogg",        27.036, -98.700),
    (48249, "Jim Wells",       27.732, -98.096),
    (48251, "Johnson",         32.376, -97.366),
    (48253, "Jones",           32.736, -99.875),
    (48255, "Karnes",          28.903, -97.859),
    (48257, "Kaufman",         32.598, -96.289),
    (48259, "Kendall",         29.943, -98.709),
    (48261, "Kenedy",          26.922, -97.645),
    (48263, "Kent",            33.177, -100.779),
    (48265, "Kerr",            30.066, -99.353),
    (48267, "Kimble",          30.484, -99.734),
    (48269, "King",            33.617, -100.257),
    (48271, "Kinney",          29.351, -100.423),
    (48273, "Kleberg",         27.432, -97.692),
    (48275, "Knox",            33.607, -99.741),
    (48277, "Lamar",           33.668, -95.570),
    (48279, "Lamb",            34.068, -102.351),
    (48281, "Lampasas",        31.197, -98.243),
    (48283, "La Salle",        28.341, -99.101),
    (48285, "Lavaca",          29.384, -96.915),
    (48287, "Lee",             30.316, -96.971),
    (48289, "Leon",            31.296, -95.987),
    (48291, "Liberty",         30.153, -94.821),
    (48293, "Limestone",       31.547, -96.580),
    (48295, "Lipscomb",        36.278, -100.271),
    (48297, "Live Oak",        28.344, -98.118),
    (48299, "Llano",           30.700, -98.675),
    (48301, "Loving",          31.852, -103.597),
    (48303, "Lubbock",         33.566, -101.821),
    (48305, "Lynn",            33.177, -101.817),
    (48307, "McCulloch",       31.202, -99.341),
    (48309, "McLennan",        31.552, -97.199),
    (48311, "McMullen",        28.352, -98.574),
    (48313, "Madison",         30.961, -95.921),
    (48315, "Marion",          32.798, -94.358),
    (48317, "Martin",          32.305, -101.946),
    (48319, "Mason",           30.716, -99.229),
    (48321, "Matagorda",       28.785, -96.003),
    (48323, "Maverick",        28.739, -100.315),
    (48325, "Medina",          29.353, -99.111),
    (48327, "Menard",          30.884, -99.822),
    (48329, "Midland",         31.869, -102.033),
    (48331, "Milam",           30.788, -96.979),
    (48333, "Mills",           31.489, -98.598),
    (48335, "Mitchell",        32.305, -100.921),
    (48337, "Montague",        33.674, -97.722),
    (48339, "Montgomery",      30.299, -95.503),
    (48341, "Moore",           35.840, -101.892),
    (48343, "Morris",          33.108, -94.733),
    (48345, "Motley",          34.075, -100.782),
    (48347, "Nacogdoches",     31.607, -94.618),
    (48349, "Navarro",         32.046, -96.472),
    (48351, "Newton",          30.783, -93.731),
    (48353, "Nolan",           32.297, -100.408),
    (48355, "Nueces",          27.726, -97.584),
    (48357, "Ochiltree",       36.278, -100.814),
    (48359, "Oldham",          35.404, -102.601),
    (48361, "Orange",          30.122, -93.898),
    (48363, "Palo Pinto",      32.745, -98.307),
    (48365, "Panola",          32.159, -94.308),
    (48367, "Parker",          32.778, -97.806),
    (48369, "Parmer",          34.530, -102.784),
    (48371, "Pecos",           30.790, -102.723),
    (48373, "Polk",            30.791, -94.832),
    (48375, "Potter",          35.400, -101.893),
    (48377, "Presidio",        29.661, -104.207),
    (48379, "Rains",           32.870, -95.792),
    (48381, "Randall",         34.965, -101.893),
    (48383, "Reagan",          31.369, -101.522),
    (48385, "Real",            29.832, -99.820),
    (48387, "Red River",       33.620, -95.052),
    (48389, "Reeves",          31.320, -103.697),
    (48391, "Refugio",         28.323, -97.155),
    (48393, "Roberts",         35.840, -100.812),
    (48395, "Robertson",       31.028, -96.514),
    (48397, "Rockwall",        32.898, -96.420),
    (48399, "Runnels",         31.832, -99.974),
    (48401, "Rusk",            32.111, -94.770),
    (48403, "Sabine",          31.344, -93.850),
    (48405, "San Augustine",   31.392, -94.167),
    (48407, "San Jacinto",     30.579, -95.164),
    (48409, "San Patricio",    28.019, -97.520),
    (48411, "San Saba",        31.168, -98.720),
    (48413, "Schleicher",      30.900, -100.538),
    (48415, "Scurry",          32.747, -100.917),
    (48417, "Shackelford",     32.736, -99.353),
    (48419, "Shelby",          31.793, -94.142),
    (48421, "Sherman",         36.278, -101.892),
    (48423, "Smith",           32.375, -95.269),
    (48425, "Somervell",       32.222, -97.772),
    (48427, "Starr",           26.556, -98.752),
    (48429, "Stephens",        32.736, -98.820),
    (48431, "Sterling",        31.831, -101.055),
    (48433, "Stonewall",       33.177, -100.254),
    (48435, "Sutton",          30.498, -100.538),
    (48437, "Swisher",         34.530, -101.730),
    (48439, "Tarrant",         32.771, -97.291),
    (48441, "Taylor",          32.299, -99.883),
    (48443, "Terrell",         30.219, -102.073),
    (48445, "Terry",           33.174, -102.339),
    (48447, "Throckmorton",    33.178, -99.211),
    (48449, "Titus",           33.216, -94.963),
    (48451, "Tom Green",       31.397, -100.461),
    (48453, "Travis",          30.336, -97.771),
    (48455, "Trinity",         31.094, -95.134),
    (48457, "Tyler",           30.775, -94.379),
    (48459, "Upshur",          32.736, -94.943),
    (48461, "Upton",           31.369, -102.038),
    (48463, "Uvalde",          29.354, -99.786),
    (48465, "Val Verde",       29.887, -100.991),
    (48467, "Van Zandt",       32.558, -95.838),
    (48469, "Victoria",        28.794, -96.979),
    (48471, "Walker",          30.726, -95.570),
    (48473, "Waller",          30.000, -95.988),
    (48475, "Ward",            31.507, -103.099),
    (48477, "Washington",      30.208, -96.396),
    (48479, "Webb",            27.761, -99.328),
    (48481, "Wharton",         29.278, -96.215),
    (48483, "Wheeler",         35.404, -100.271),
    (48485, "Wichita",         33.986, -98.702),
    (48487, "Wilbarger",       34.094, -99.245),
    (48489, "Willacy",         26.481, -97.752),
    (48491, "Williamson",      30.648, -97.601),
    (48493, "Wilson",          29.168, -98.086),
    (48495, "Winkler",         31.849, -103.053),
    (48497, "Wise",            33.220, -97.653),
    (48499, "Wood",            32.781, -95.377),
    (48501, "Yoakum",          33.174, -102.826),
    (48503, "Young",           33.177, -98.688),
    (48505, "Zapata",          27.007, -99.179),
    (48507, "Zavala",          28.867, -99.760),
]

# Verify we have exactly 254 counties
assert len(COUNTIES) == 254, f"Expected 254 counties, got {len(COUNTIES)}"

# ---------------------------------------------------------------------------
# 2. Define urban counties (higher actual speeds, lower overstatement)
# ---------------------------------------------------------------------------
URBAN_FIPS = {
    48113,  # Dallas
    48201,  # Harris
    48439,  # Tarrant
    48029,  # Bexar
    48453,  # Travis
    48085,  # Collin
    48121,  # Denton
    48157,  # Fort Bend
    48141,  # El Paso
    48215,  # Hidalgo
    48039,  # Brazoria
    48167,  # Galveston
    48041,  # Brazos
    48309,  # McLennan
    48251,  # Johnson
    48139,  # Ellis
    48491,  # Williamson
    48187,  # Guadalupe
    48091,  # Comal
    48303,  # Lubbock
    48375,  # Potter (Amarillo)
    48355,  # Nueces (Corpus Christi)
    48245,  # Jefferson (Beaumont)
    48209,  # Hays
    48027,  # Bell
}

# Semi-rural counties (moderate speeds)
SEMIRURAL_FIPS = {
    48257,  # Kaufman
    48139,  # Ellis
    48367,  # Parker
    48221,  # Hood
    48339,  # Montgomery
    48291,  # Liberty
    48071,  # Chambers
    48395,  # Robertson
    48185,  # Grimes
    48473,  # Waller
    48021,  # Bastrop
    48055,  # Caldwell
    48013,  # Atascosa
    48177,  # Gonzales
    48325,  # Medina
    48463,  # Uvalde
    48265,  # Kerr
    48171,  # Gillespie
    48053,  # Burnet
    48281,  # Lampasas
    48499,  # Wood
    48183,  # Gregg
    48401,  # Rusk
    48423,  # Smith
    48203,  # Harrison
    48037,  # Bowie
    48277,  # Lamar
    48181,  # Grayson
    48097,  # Cooke
    48337,  # Montague
    48363,  # Palo Pinto
    48133,  # Eastland
}

PROVIDER_POOL = [
    "Rise Broadband",
    "Viasat",
    "SpaceX Starlink",
    "AT&T Fixed Wireless",
    "Nextlink Internet",
    "Wisper ISP",
    "Geoverse",
    "Hector Communications",
    "Totelcom",
    "PC Electronics",
    "LightStream",
]

# Tech codes: 70=Fixed Wireless, 60=Satellite, 50=Cable, 11=DSL
PROVIDER_TECH = {
    "Rise Broadband":       70,
    "Viasat":               60,
    "SpaceX Starlink":      60,
    "AT&T Fixed Wireless":  70,
    "Nextlink Internet":    70,
    "Wisper ISP":           70,
    "Geoverse":             70,
    "Hector Communications":70,
    "Totelcom":             70,
    "PC Electronics":       70,
    "LightStream":          70,
}

def make_bbox_polygon(lat, lon, half_deg=0.3):
    """Return a GeoJSON Polygon as a 5-point closed ring bounding box."""
    min_lon = lon - half_deg
    max_lon = lon + half_deg
    min_lat = lat - half_deg
    max_lat = lat + half_deg
    coords = [
        [min_lon, min_lat],
        [max_lon, min_lat],
        [max_lon, max_lat],
        [min_lon, max_lat],
        [min_lon, min_lat],   # closed
    ]
    return {"type": "Polygon", "coordinates": [coords]}


def generate_county_data(fips, name, lat, lon):
    """Generate realistic broadband stats for a county."""
    geoid = f"{fips:05d}"

    if fips in URBAN_FIPS:
        # Urban: high actual speeds, modest overstatement
        fcc_down  = round(random.uniform(50.0, 100.0), 1)
        fcc_up    = round(random.uniform(10.0,  20.0), 1)
        act_down  = round(random.uniform(30.0,  60.0), 1)
        act_up    = round(random.uniform(8.0,   18.0), 1)
    elif fips in SEMIRURAL_FIPS:
        # Semi-rural: moderate
        fcc_down  = round(random.uniform(35.0,  75.0), 1)
        fcc_up    = round(random.uniform(5.0,   15.0), 1)
        act_down  = round(random.uniform(15.0,  35.0), 1)
        act_up    = round(random.uniform(3.0,   12.0), 1)
    else:
        # Rural / West Texas: low actual, high overstatement
        fcc_down  = round(random.uniform(25.0,  75.0), 1)
        fcc_up    = round(random.uniform(3.0,   10.0), 1)
        act_down  = round(random.uniform(5.0,   15.0), 1)
        act_up    = round(random.uniform(1.0,    5.0), 1)

    # Ensure act_down is always strictly less than fcc_down
    act_down = min(act_down, round(fcc_down * 0.85, 1))

    overstatement = round(fcc_down / act_down, 2)

    # Pick 1–3 providers
    n_providers = random.randint(1, 3)
    providers = random.sample(PROVIDER_POOL, n_providers)

    # Small county gets a tighter bounding box
    half = 0.2 if name in {"Rockwall", "Somervell", "Kenedy", "Aransas",
                            "Camp", "Delta", "Rains", "Roberts"} else 0.3

    return {
        "geoid": geoid,
        "name": name,
        "lat": lat,
        "lon": lon,
        "half": half,
        "fcc_claimed_down_mbps": fcc_down,
        "fcc_claimed_up_mbps":   fcc_up,
        "ookla_actual_down_mbps": act_down,
        "ookla_actual_up_mbps":   act_up,
        "overstatement_ratio":   overstatement,
        "top_providers":         providers,
    }


def build_geojson(county_records):
    features = []
    for rec in county_records:
        geometry = make_bbox_polygon(rec["lat"], rec["lon"], rec["half"])
        feature = {
            "type": "Feature",
            "id": rec["geoid"],
            "geometry": geometry,
            "properties": {
                "GEOID":                   rec["geoid"],
                "NAME":                    rec["name"],
                "fcc_claimed_down_mbps":   rec["fcc_claimed_down_mbps"],
                "fcc_claimed_up_mbps":     rec["fcc_claimed_up_mbps"],
                "ookla_actual_down_mbps":  rec["ookla_actual_down_mbps"],
                "ookla_actual_up_mbps":    rec["ookla_actual_up_mbps"],
                "overstatement_ratio":     rec["overstatement_ratio"],
                "top_providers":           rec["top_providers"],
            },
        }
        features.append(feature)

    return {"type": "FeatureCollection", "features": features}


def build_providers_json(county_records):
    result = {}
    for rec in county_records:
        geoid = rec["geoid"]
        provider_list = []
        for pname in rec["top_providers"]:
            provider_list.append({
                "name":            pname,
                "fcc_claimed_down": rec["fcc_claimed_down_mbps"],
                "fcc_claimed_up":   rec["fcc_claimed_up_mbps"],
                "tech_code":        PROVIDER_TECH.get(pname, 70),
            })
        result[geoid] = provider_list
    return result


def main():
    out_dir = "/home/user/texas-broadband-truth-map/data/processed"
    os.makedirs(out_dir, exist_ok=True)

    print(f"Generating data for {len(COUNTIES)} Texas counties...")

    county_records = []
    for fips, name, lat, lon in COUNTIES:
        rec = generate_county_data(fips, name, lat, lon)
        county_records.append(rec)

    # --- GeoJSON ---
    geojson_data = build_geojson(county_records)
    geojson_path = os.path.join(out_dir, "counties.geojson")
    with open(geojson_path, "w") as f:
        json.dump(geojson_data, f, indent=2)

    # --- providers.json ---
    providers_data = build_providers_json(county_records)
    providers_path = os.path.join(out_dir, "providers.json")
    with open(providers_path, "w") as f:
        json.dump(providers_data, f, indent=2)

    # --- Report ---
    geojson_size   = os.path.getsize(geojson_path)
    providers_size = os.path.getsize(providers_path)
    n_features     = len(geojson_data["features"])

    print(f"\nOutput files:")
    print(f"  {geojson_path}")
    print(f"    Features  : {n_features}")
    print(f"    Size      : {geojson_size:,} bytes ({geojson_size/1024:.1f} KB)")
    print(f"  {providers_path}")
    print(f"    Counties  : {len(providers_data)}")
    print(f"    Size      : {providers_size:,} bytes ({providers_size/1024:.1f} KB)")

    # Quick sanity checks
    assert n_features == 254, f"Feature count mismatch: {n_features}"
    geoids = {f["id"] for f in geojson_data["features"]}
    assert len(geoids) == 254, "Duplicate or missing GEOIDs"

    # Sample output
    sample = geojson_data["features"][0]["properties"]
    print(f"\nSample county ({geojson_data['features'][0]['id']}):")
    for k, v in sample.items():
        print(f"    {k}: {v}")

    print("\nDone. All checks passed.")


if __name__ == "__main__":
    main()

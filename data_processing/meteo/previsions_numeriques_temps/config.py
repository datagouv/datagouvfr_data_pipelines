# Nb of batch we want to retrieve (every 6 hours)
MAX_LAST_BATCHES = 60
BATCH_URL_SIZE = 50
METEO_API_URL = "https://public-api.meteofrance.fr/previnum/"

BATCH_URL_SIZE_PACKAGE = {
    "arome": 40,
    "arpege": 40,
    "arome-om": 340,
    "vague-surcote": 90,
}


def standardize_hour(hour: int, nb_char: int):
    # standardize_hour(1, 3) = 001H
    str_hour = str(hour)
    return "0" * (nb_char - len(str_hour)) + str_hour + "H"


def create_echeances(min_hour: int, max_hour: int, nb_char: int):
    return [standardize_hour(h, nb_char) for h in range(min_hour, max_hour + 1)]


class Package:
    def __init__(self, name: str, **kwargs):
        self.name = name
        if "time" in kwargs:
            self.time = kwargs["time"]
        else:
            self.time = create_echeances(
                min_hour=kwargs["min_hour"],
                max_hour=kwargs["max_hour"],
                nb_char=kwargs["nb_char"],
            )
        if kwargs.get("additional_time"):
            self.time += kwargs["additional_time"]


SP1_MFWAM = Package(
    name="SP1",
    **{
        "min_hour": 1,
        "max_hour": 48,
        "nb_char": 3,
        "additional_time": [
            '051H', '054H', '057H',
            '060H', '063H', '066H', '069H',
            '072H', '075H', '078H',
            '081H', '084H', '087H',
            '090H', '093H', '096H', '099H',
            '102H',
        ],
    }
)
SP1_WW3 = Package(name="SP1", **{"time": ['000H999H']})
SP1_HYCOM2D_ARP = Package(name="SP1", **{"min_hour": 0, "max_hour": 102, "nb_char": 3})
SP1_HYCOM2D_ARO = Package(name="SP1", **{"min_hour": 0, "max_hour": 51, "nb_char": 3})
IP1 = Package(name="IP1", **{"min_hour": 0, "max_hour": 48, "nb_char": 3})
IP2 = Package(name="IP2", **{"min_hour": 0, "max_hour": 48, "nb_char": 3})
IP3 = Package(name="IP3", **{"min_hour": 0, "max_hour": 48, "nb_char": 3})
IP4 = Package(name="IP4", **{"min_hour": 1, "max_hour": 48, "nb_char": 3})
IP5 = Package(name="IP5", **{"min_hour": 0, "max_hour": 48, "nb_char": 3})
SP1 = Package(name="SP1", **{"min_hour": 0, "max_hour": 48, "nb_char": 3})
SP2 = Package(name="SP2", **{"min_hour": 0, "max_hour": 48, "nb_char": 3})
SP3 = Package(name="SP3", **{"min_hour": 0, "max_hour": 48, "nb_char": 3})
HP1 = Package(name="HP1", **{"min_hour": 0, "max_hour": 48, "nb_char": 3})
HP2 = Package(name="HP2", **{"min_hour": 0, "max_hour": 48, "nb_char": 3})
HP3 = Package(name="HP3", **{"min_hour": 1, "max_hour": 48, "nb_char": 3})

AROME_TIME = ["00H06H", "07H12H", "13H18H", "19H24H", "25H30H", "31H36H", "37H42H", "43H48H", "49H51H"]
ARPEGE01_TIME = ["000H012H", "013H024H", "025H036H", "037H048H", "049H060H", "061H072H", "073H084H", "085H096H", "097H102H"]
ARPEGE025_TIME = ["000H024H", "025H048H", "049H072H", "073H102H"]

# model => pack => grid => packages
PACKAGES = {
    "vague-surcote": {
        "MFWAM": {
            "0.025": {
                "dataset_id": {
                    "dev": "65b68c83b833790f93a4ab27",
                    "prod": "65bd1a505a5b412989a84ca7",
                },
                "check_availability_url": f"{METEO_API_URL}DPPaquetWAVESMODELS/models/MFWAM/grids/0.025/packages/SP1",
                "packages": [Package(name="SP1", **{"min_hour": 1, "max_hour": 48, "nb_char": 3})],
            },
            "0.1": {
                "dataset_id": {
                    "dev": "65b68c841a2bd22881b8e487",
                    "prod": "65bd1a2957e1cc7c9625e7b5",
                },
                "check_availability_url": f"{METEO_API_URL}DPPaquetWAVESMODELS/models/MFWAM/grids/0.1/packages/SP1",
                "packages": [SP1_MFWAM],
            },
            "0.5": {
                "dataset_id": {
                    "dev": "65b68c841a2bd22881b8e488",
                    "prod": "65bd19fe0d61026813636c33",
                },
                "check_availability_url": f"{METEO_API_URL}DPPaquetWAVESMODELS/models/MFWAM/grids/0.5/packages/SP1",
                "packages": [SP1_MFWAM],
            },
            "base_url": f"{METEO_API_URL}DPPaquetWAVESMODELS/models/MFWAM/grids",
            "product": "productMFWAM",
            "extension": "grib2",
        },
        "WW3-MARP": {
            "0.01": {
                "dataset_id": {
                    "dev": "65b68c840053e6459a859ccf",
                    "prod": "65bd19a20a9351d1cbe9a090",
                },
                "check_availability_url": f"{METEO_API_URL}DPPaquetWAVESMODELS/models/WW3-MARP/grids/0.01/packages/SP1",
                "packages": [SP1_WW3],
            },
            "base_url": f"{METEO_API_URL}DPPaquetWAVESMODELS/models/WW3-MARP/grids",
            "product": "productWMARP",
            "extension": "nc",
        },
        "WW3-MARO": {
            "0.01": {
                "dataset_id": {
                    "dev": "65b68c852bb8441329433a30",
                    "prod": "65bd19226c4e3fcbf4948f99",
                },
                "check_availability_url": f"{METEO_API_URL}DPPaquetWAVESMODELS/models/WW3-MARO/grids/0.01/packages/SP1",
                "packages": [SP1_WW3],
            },
            "base_url": f"{METEO_API_URL}DPPaquetWAVESMODELS/models/WW3-MARO/grids",
            "product": "productWMARO",
            "extension": "nc",
        },
        "HYCOM2D-MARP": {
            "0.04": {
                "dataset_id": {
                    "dev": "65b68c8580a75b6c6bae3d67",
                    "prod": "65bd183c9ec6ae3f87a5334a",
                },
                "check_availability_url": f"{METEO_API_URL}DPPaquetWAVESMODELS/models/HYCOM2D-MARP/grids/0.04/packages/SP1",
                "packages": [SP1_HYCOM2D_ARP],
            },
            "base_url": f"{METEO_API_URL}DPPaquetWAVESMODELS/models/HYCOM2D-MARP/grids",
            "product": "productHMARP",
            "extension": "grib2",
        },
        "HYCOM2D-WARP": {
            "0.04": {
                "dataset_id": {
                    "dev": "65b68c850e237844c20fd501",
                    "prod": "65bd17fe9ec6ae3f87a53349",
                },
                "check_availability_url": f"{METEO_API_URL}DPPaquetWAVESMODELS/models/HYCOM2D-WARP/grids/0.04/packages/SP1",
                "packages": [SP1_HYCOM2D_ARP],
            },
            "base_url": f"{METEO_API_URL}DPPaquetWAVESMODELS/models/HYCOM2D-WARP/grids",
            "product": "productHWARP",
            "extension": "grib2",
        },
        "HYCOM2D-MARO": {
            "0.04": {
                "dataset_id": {
                    "dev": "65b68c860e237844c20fd502",
                    "prod": "65bd17779ec6ae3f87a53348",
                },
                "check_availability_url": f"{METEO_API_URL}DPPaquetWAVESMODELS/models/HYCOM2D-MARO/grids/0.04/packages/SP1",
                "packages": [SP1_HYCOM2D_ARO],
            },
            "base_url": f"{METEO_API_URL}DPPaquetWAVESMODELS/models/HYCOM2D-MARO/grids",
            "product": "productHMARO",
            "extension": "grib2",
        },
        "HYCOM2D-WARO": {
            "0.04": {
                "dataset_id": {
                    "dev": "65b68c8545f1789c428c8907",
                    "prod": "65bd17b9775b5222832d67a4",
                },
                "check_availability_url": f"{METEO_API_URL}DPPaquetWAVESMODELS/models/HYCOM2D-WARO/grids/0.04/packages/SP1",
                "packages": [SP1_HYCOM2D_ARO],
            },
            "base_url": f"{METEO_API_URL}DPPaquetWAVESMODELS/models/HYCOM2D-WARO/grids",
            "product": "productHWARO",
            "extension": "grib2",
        },
    },
    "arome-om": {
        "ANTIL": {
            "0.025": {
                "dataset_id": {
                    "dev": "65b68c862bb8441329433a31",
                    "prod": "65bd162b9dc0d31edfabc2b9",
                },
                "check_availability_url": f"{METEO_API_URL}DPPaquetAROME-OM/models/AROME-OM-ANTIL/grids/0.025/packages/IP1",
                "packages": [IP1, IP2, IP3, IP4, IP5, SP1, SP2, SP3, HP1, HP2, HP3],
            },
            "base_url": f"{METEO_API_URL}DPPaquetAROME-OM/v1/models/AROME-OM-ANTIL/grids",
            "product": "productOMAN",
            "extension": "grib2",
        },
        "GUYANE": {
            "0.025": {
                "dataset_id": {
                    "dev": "65b68c860053e6459a859cd0",
                    "prod": "65e0bd4b88e4fd88b989ba46",
                },
                "check_availability_url": f"{METEO_API_URL}DPPaquetAROME-OM/models/AROME-OM-GUYANE/grids/0.025/packages/IP1",
                "packages": [IP1, IP2, IP3, IP4, IP5, SP1, SP2, SP3, HP1, HP2, HP3],
            },
            "base_url": f"{METEO_API_URL}DPPaquetAROME-OM/v1/models/AROME-OM-GUYANE/grids",
            "product": "productOMGU",
            "extension": "grib2",
        },
        "INDIEN": {
            "0.025": {
                "dataset_id": {
                    "dev": "65b68c860053e6459a859cd1",
                    "prod": "65bd1560c73941a5e0ec1891",
                },
                "check_availability_url": f"{METEO_API_URL}DPPaquetAROME-OM/models/AROME-OM-INDIEN/grids/0.025/packages/IP1",
                "packages": [IP1, IP2, IP3, IP4, IP5, SP1, SP2, SP3, HP1, HP2, HP3],
            },
            "base_url": f"{METEO_API_URL}DPPaquetAROME-OM/v1/models/AROME-OM-INDIEN/grids",
            "product": "productOMOI",
            "extension": "grib2",
        },
        "POLYN": {
            "0.025": {
                "dataset_id": {
                    "dev": "65b68c870e237844c20fd503",
                    "prod": "65bd1509cc112e6a1458ab95",
                },
                "check_availability_url": f"{METEO_API_URL}DPPaquetAROME-OM/models/AROME-OM-POLYN/grids/0.025/packages/IP1",
                "packages": [IP1, IP2, IP3, IP4, IP5, SP1, SP2, SP3, HP1, HP2, HP3],
            },
            "base_url": f"{METEO_API_URL}DPPaquetAROME-OM/v1/models/AROME-OM-POLYN/grids",
            "product": "productOMPF",
            "extension": "grib2",
        },
        "NCALED": {
            "0.025": {
                "dataset_id": {
                    "dev": "65b68c870e237844c20fd504",
                    "prod": "65bd14cca6919e97e9699b09",
                },
                "check_availability_url": f"{METEO_API_URL}DPPaquetAROME-OM/models/AROME-OM-NCALED/grids/0.025/packages/IP1",
                "packages": [IP1, IP2, IP3, IP4, IP5, SP1, SP2, SP3, HP1, HP2, HP3],
            },
            "base_url": f"{METEO_API_URL}DPPaquetAROME-OM/v1/models/AROME-OM-NCALED/grids",
            "product": "productOMNC",
            "extension": "grib2",
        },
    },
    "arome": {
        "$1": {
            "0.01": {
                "dataset_id": {
                    "dev": "65aade5a97e39e6c4cae5252",
                    "prod": "65bd1247a6238f16e864fa80",
                },
                "check_availability_url": f"{METEO_API_URL}DPPaquetAROME/models/AROME/grids/0.01/packages/SP1",
                "packages": [
                    Package(name="SP1", **{"min_hour": 0, "max_hour": 51, "nb_char": 2}),
                    Package(name="SP2", **{"min_hour": 0, "max_hour": 51, "nb_char": 2}),
                    Package(name="SP3", **{"min_hour": 0, "max_hour": 51, "nb_char": 2}),
                    Package(name="HP1", **{"min_hour": 0, "max_hour": 51, "nb_char": 2}),
                ],
            },
            "base_url": f"{METEO_API_URL}DPPaquetAROME/models/AROME/grids",
            "product": "productARO",
            "extension": "grib2",
        },
        "$2": {
            "0.025": {
                "dataset_id": {
                    "dev": "65aadec3dc9e969fd00e71e8",
                    "prod": "65bd12d7bfd26e26804204cb",
                },
                "check_availability_url": f"{METEO_API_URL}DPPaquetAROME/models/AROME/grids/0.025/packages/SP1",
                "packages": [
                    Package(name="IP1", **{"time": AROME_TIME}),
                    Package(name="IP2", **{"time": AROME_TIME}),
                    Package(name="IP3", **{"time": AROME_TIME}),
                    Package(name="IP4", **{"time": AROME_TIME}),
                    Package(name="IP5", **{"time": AROME_TIME}),
                    Package(name="SP1", **{"time": AROME_TIME}),
                    Package(name="SP2", **{"time": AROME_TIME}),
                    Package(name="SP3", **{"time": AROME_TIME}),
                    Package(name="HP1", **{"time": AROME_TIME}),
                    Package(name="HP2", **{"time": AROME_TIME}),
                    Package(name="HP3", **{"time": AROME_TIME}),
                ],
            },
            "base_url": f"{METEO_API_URL}DPPaquetAROME/models/AROME/grids",
            "product": "productARO",
            "extension": "grib2",
        },
    },
    "arpege": {
        "$1": {
            "0.1": {
                "dataset_id": {
                    "dev": "65aaded8dc9e969fd00e71e9",
                    "prod": "65bd13b2eb9e79ab309f6e63",
                },
                "check_availability_url": f"{METEO_API_URL}DPPaquetARPEGE/models/ARPEGE/grids/0.1/packages/IP1",
                "packages": [
                    Package(name="IP1", **{"time": ARPEGE01_TIME}),
                    Package(name="IP2", **{"time": ARPEGE01_TIME}),
                    Package(name="IP3", **{"time": ARPEGE01_TIME}),
                    Package(name="IP4", **{"time": ARPEGE01_TIME}),
                    Package(name="SP1", **{"time": ARPEGE01_TIME}),
                    Package(name="SP2", **{"time": ARPEGE01_TIME}),
                    Package(name="HP1", **{"time": ARPEGE01_TIME}),
                    Package(name="HP2", **{"time": ARPEGE01_TIME}),
                ],
            },
            "base_url": f"{METEO_API_URL}DPPaquetARPEGE/models/ARPEGE/grids",
            "product": "productARP",
            "extension": "grib2",
        },
        "$2": {
            "0.25": {
                "dataset_id": {
                    "dev": "65aadeebdc9e969fd00e71ea",
                    "prod": "65bd13e557b26b467363b521",
                },
                "check_availability_url": f"{METEO_API_URL}DPPaquetARPEGE/models/ARPEGE/grids/0.25/packages/IP1",
                "packages": [
                    Package(name="IP1", **{"time": ARPEGE025_TIME}),
                    Package(name="IP2", **{"time": ARPEGE025_TIME}),
                    Package(name="IP3", **{"time": ARPEGE025_TIME}),
                    Package(name="IP4", **{"time": ARPEGE025_TIME}),
                    Package(name="SP1", **{"time": ARPEGE025_TIME}),
                    Package(name="SP2", **{"time": ARPEGE025_TIME}),
                    Package(name="HP1", **{"time": ARPEGE025_TIME}),
                    Package(name="HP2", **{"time": ARPEGE025_TIME}),
                ],
            },
            "base_url": f"{METEO_API_URL}DPPaquetARPEGE/models/ARPEGE/grids",
            "product": "productARP",
            "extension": "grib2",
        },
    },
}

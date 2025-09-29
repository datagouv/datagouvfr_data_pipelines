def test_extract_log_info():
    """Test if the function return output is as expected."""
    from datagouvfr_data_pipelines.dgv.metrics.config import MetricsConfig
    from datagouvfr_data_pipelines.dgv.metrics.task_functions import extract_log_info

    config = MetricsConfig()

    test_logs = [
        {
            "log": "2025-08-01T03:22:51.022900+02:00 slb-04 haproxy[345597]: X.X.X.X:0000 [01/Aug/2025:03:22:50.974]"
            ' DATAGOUVFR_RGS~ DATAGOUVFR_NEWINFRA/dataweb-06 0/0/4/44/+48 302 +792 - - --NN 442/362/8/1/0 0/0 "GET'
            ' https://www.data.gouv.fr/api/1/datasets/r/5ffa8553-0e8f-4622-add9-5c0b593ca1f8 HTTP/2.0"',
            "expected_output": (
                "5ffa8553-0e8f-4622-add9-5c0b593ca1f8",
                "resources",
                "api_permalink",
            ),
        },
    ]

    for log in test_logs:
        assert (
            extract_log_info(log["log"], config.logs_config) == log["expected_output"]
        )


def test_parse_logs():
    """Test if the function output file respects the expected parsing logic."""
    import os
    from datagouvfr_data_pipelines.dgv.metrics.config import MetricsConfig
    from datagouvfr_data_pipelines.dgv.metrics.task_functions import parse_logs

    config = MetricsConfig()
    test_path = os.path.join(os.path.dirname(__file__), "test_")

    for log_config in config.logs_config:
        if os.path.exists(test_path + f"1900-01-01_{log_config.type}_found.csv"):
            os.remove(test_path + f"1900-01-01_{log_config.type}_found.csv")

    with open(test_path + "raw.log", "rb") as log_data:
        lines = log_data.readlines()
        parse_logs(
            logs=lines,
            date="1900-01-01",
            logs_config=config.logs_config,
            output_path=test_path,
        )

    for log_config in config.logs_config:
        with (
            open(test_path + f"1900-01-01_{log_config.type}_found.csv", "rb") as out_f,
            open(test_path + f"{log_config.type}_found_expected.csv", "rb") as exp_f,
        ):
            output_content = out_f.read()
            expected_content = exp_f.read()

            assert output_content == expected_content

        os.remove(test_path + f"1900-01-01_{log_config.type}_found.csv")


def test_aggregate_metrics():
    """Test if the aggregation function works correctly."""
    import os
    import pandas as pd
    from datagouvfr_data_pipelines.dgv.metrics.config import MetricsConfig
    from datagouvfr_data_pipelines.dgv.metrics.task_functions import aggregate_metrics

    config = MetricsConfig()
    test_path = os.path.join(os.path.dirname(__file__), "test_")

    for log_config in config.logs_config:
        if os.path.exists(test_path + f"{log_config.type}.csv"):
            os.remove(test_path + f"{log_config.type}.csv")

        df_catalog = pd.read_csv(
            f"{test_path}{log_config.type}_catalog.csv",
            dtype="string",
            sep=";",
            usecols=list(log_config.catalog_columns.keys()),
        )
        df = pd.read_csv(
            f"{test_path}{log_config.type}_found_expected.csv",
            dtype="string",
            sep=";",
        )

        aggregate_metrics(
            df,
            df_catalog,
            log_config,
            config,
            f"{test_path}{log_config.type}.csv",
        )

        with (
            open(test_path + f"{log_config.type}.csv", "rb") as out_f,
            open(test_path + f"{log_config.type}_expected.csv", "rb") as exp_f,
        ):
            output_content = out_f.read()
            expected_content = exp_f.read()

            assert output_content == expected_content
            os.remove(test_path + f"{log_config.type}.csv")


def test_get_catalog_id_mapping():
    import pandas as pd
    from datagouvfr_data_pipelines.dgv.metrics.task_functions import (
        get_catalog_id_mapping,
    )

    df = pd.DataFrame(
        {
            "id": [
                "679af967b5ff53d3c639a389",
                "679af9477001c9a6fe6fd893",
                "679af0d533b1468d0b6fd895",
                "679af0d433b1468d0b6fd894",
                "679aefcc8ffc9c88376fd893",
                "679aee11159adf9ce839a388",
                "679aebe509e40722956fd894",
                "679ade2f29d4d58a8739a388",
                "679ad76edd7b3ca9796fd894",
                "679ac40b65ded23d8d6fd8a9",
                "679ac143d16efacf806fd893",
            ],
            "slug": [
                "equipements-sportifs-de-proximite-angers-stadium",
                "lidar-hd-ign-2022",
                "loi-eau-67-2024",
                "prairies-permanentes-2024",
                "structure-a-competence-gestion-des-milieux-aquatiques-hautes-pyrenees-2",
                "limites-des-nouvelles-communes-concernees-par-un-ou-plusieurs-risque-s-majeur-s-dans-le-departement-de-lorne-1",
                "inondations-2025-routes-barrees",
                "espaces-proches-rivage-epr-limites-au-titre-du-l-146-6",
                "commune-030",
                "pprn-de-culoz-beon",
                "elections-partielles",
            ],
            "organization_id": [
                "538346d6a3a72906c7ec5c36",
                "544e1b0888ee38289ae1ed05",
                "5617abdc88ee386b8f628ef9",
                "5617abdc88ee386b8f628ef9",
                "58aeee1688ee382134fb5212",
                "558e9b15c751df2a26a453b9",
                "580735be88ee3814295ff490",
                "558ea2d6c751df3646a453cf",
                "58aedb7c88ee3801ec1afb7e",
                "5883648088ee38358d9b81a4",
                "5733038988ee38472ed1b934",
            ],
        }
    )

    expected_output = {
        "equipements-sportifs-de-proximite-angers-stadium": "679af967b5ff53d3c639a389",
        "679af967b5ff53d3c639a389": "679af967b5ff53d3c639a389",
        "lidar-hd-ign-2022": "679af9477001c9a6fe6fd893",
        "679af9477001c9a6fe6fd893": "679af9477001c9a6fe6fd893",
        "loi-eau-67-2024": "679af0d533b1468d0b6fd895",
        "679af0d533b1468d0b6fd895": "679af0d533b1468d0b6fd895",
        "prairies-permanentes-2024": "679af0d433b1468d0b6fd894",
        "679af0d433b1468d0b6fd894": "679af0d433b1468d0b6fd894",
        "structure-a-competence-gestion-des-milieux-aquatiques-hautes-pyrenees-2": "679aefcc8ffc9c88376fd893",
        "679aefcc8ffc9c88376fd893": "679aefcc8ffc9c88376fd893",
        "limites-des-nouvelles-communes-concernees-par-un-ou-plusieurs-risque-s-majeur-s-dans-le-departement-de-lorne-1": "679aee11159adf9ce839a388",
        "679aee11159adf9ce839a388": "679aee11159adf9ce839a388",
        "inondations-2025-routes-barrees": "679aebe509e40722956fd894",
        "679aebe509e40722956fd894": "679aebe509e40722956fd894",
        "espaces-proches-rivage-epr-limites-au-titre-du-l-146-6": "679ade2f29d4d58a8739a388",
        "679ade2f29d4d58a8739a388": "679ade2f29d4d58a8739a388",
        "commune-030": "679ad76edd7b3ca9796fd894",
        "679ad76edd7b3ca9796fd894": "679ad76edd7b3ca9796fd894",
        "pprn-de-culoz-beon": "679ac40b65ded23d8d6fd8a9",
        "679ac40b65ded23d8d6fd8a9": "679ac40b65ded23d8d6fd8a9",
        "elections-partielles": "679ac143d16efacf806fd893",
        "679ac143d16efacf806fd893": "679ac143d16efacf806fd893",
    }

    assert get_catalog_id_mapping(df, "slug") == expected_output


def test_get_matomo_outlinks():
    from datagouvfr_data_pipelines.dgv.metrics.task_functions import get_matomo_outlinks

    # This is the most consulted reuse of our catalog
    result = get_matomo_outlinks(
        "reuses", "deces-en-france", "https://www.deces-en-france.fr", "previous month"
    )

    assert isinstance(result, int)
    assert result > 0

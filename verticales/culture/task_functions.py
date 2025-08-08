from datagouvfr_data_pipelines.utils.grist import GristTable


def get_perimeter_orgas(ti):
    table = GristTable("hrDZg8StuE1d", "Perimetre_culture")
    ti.xcom_push(key="orgas", value=table["datagouv_id"].to_list())

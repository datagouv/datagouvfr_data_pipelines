from datagouvfr_data_pipelines.dgv.simplifions.simplifions_manager import SimplifionsManager

def get_and_format_grist_data(ti, client=None, api_key=None):
    return SimplifionsManager(client, api_key).get_and_format_grist_data(ti)

def update_topics(ti, client=None, api_key=None):
    return SimplifionsManager(client, api_key).update_topics(ti)

def update_topics_references(ti, client=None, api_key=None):
    return SimplifionsManager(client, api_key).update_topics_references(ti)

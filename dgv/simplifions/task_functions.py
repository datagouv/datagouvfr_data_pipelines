from datagouvfr_data_pipelines.dgv.simplifions.simplifions_manager import SimplifionsManager

def get_and_format_grist_data(ti, client=None):
    return SimplifionsManager(client).get_and_format_grist_data(ti)

def update_topics(ti, client=None):
    return SimplifionsManager(client).update_topics(ti)

def update_topics_references(ti, client=None):
    return SimplifionsManager(client).update_topics_references(ti)

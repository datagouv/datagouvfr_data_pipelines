from datagouvfr_data_pipelines.verticales.simplifions.grist_v2_manager import (
    GristV2Manager,
)
from datagouvfr_data_pipelines.verticales.simplifions.topics_v2_manager import (
    TopicsV2Manager,
)


def get_and_format_grist_v2_data(ti, client=None):
    return GristV2Manager().get_and_format_grist_v2_data(ti)


def update_topics_v2(ti, client=None):
    return TopicsV2Manager(client).update_topics(ti)


def watch_grist_data(ti):
    return GristV2Manager().watch_grist_data(ti)

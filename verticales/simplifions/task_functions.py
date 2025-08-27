from datagouvfr_data_pipelines.verticales.simplifions.grist_manager import (
    GristManager,
)
from datagouvfr_data_pipelines.verticales.simplifions.topics_manager import (
    TopicsManager,
)


def get_and_format_grist_data(ti, client=None):
    return GristManager().get_and_format_grist_data(ti)


def update_topics(ti, client=None):
    return TopicsManager(client).update_topics(ti)


def update_topics_references(ti, client=None):
    return TopicsManager(client).update_topics_references(ti)

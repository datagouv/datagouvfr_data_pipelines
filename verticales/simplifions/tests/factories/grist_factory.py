from mocks.grist_mock import GristMock
from factories.external_resources_factory import ExternalResourcesFactory

grist_mock = GristMock()


class GristFactory(ExternalResourcesFactory):
    def fixture_folder(self):
        return "grist"

    def resource_mock(self):
        return grist_mock

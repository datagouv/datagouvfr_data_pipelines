from factories.external_resources_factory import ExternalResourcesFactory
from mocks.grist_mock import GristMock

grist_mock = GristMock()


class GristFactory(ExternalResourcesFactory):
    def __init__(self):
        super().__init__()
        grist_mock.mock_workspace()

    def fixture_folder(self):
        return "grist"

    def resource_mock(self):
        return grist_mock

class TaskInstanceFactory:
    def __init__(self):
        pass

    def build_ti(self, data: dict):
        """Create a mock TaskInstance that returns the provided grist data via xcom_pull"""

        class MockTI:
            def xcom_pull(self, key, task_ids):
                return data[key]

        return MockTI()

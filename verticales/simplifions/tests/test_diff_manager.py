# The factory must be imported before the manager because it initializes the mocks
from diff_manager import DiffManager


class TestDiffManager:
    def setup_method(self):
        self.fields = {
            "key": "value",
            "key2": "value2",
            "Modifie_le": 1729000000,
            "Modifie_par": "testman the tester",
            "anchor_link": "https://example.com",
            "technical_title": "Test Case Modified",
        }

    def test_get_diff_with_similar_objects(self):
        backup_fields = self.fields.copy()
        diff = DiffManager.get_diff(self.fields, backup_fields)
        assert diff is None

    def test_get_diff_with_different_objects(self):
        backup_fields = self.fields.copy()
        backup_fields["key"] = "different value"
        diff = DiffManager.get_diff(self.fields, backup_fields)
        assert diff == {"key": {"new": "value", "old": "different value"}}

    def test_get_diff_with_ignored_keys(self):
        backup_fields = self.fields.copy()
        backup_fields["key"] = "different value"
        backup_fields["Modifie_le"] = 1729000001
        backup_fields["Modifie_par"] = "Monique"
        backup_fields["anchor_link"] = "https://example.com/different"
        backup_fields["technical_title"] = "Different Test Case"
        diff = DiffManager.get_diff(self.fields, backup_fields)
        assert "Modifie_le" not in diff
        assert "Modifie_par" not in diff
        assert "anchor_link" not in diff
        assert "technical_title" not in diff

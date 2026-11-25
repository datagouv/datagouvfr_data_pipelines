"""Unit tests for the ambiguous-match resolution in utils/cadastre_index."""

import numpy as np
import shapely
from datagouvfr_data_pipelines.data_processing.dvf.geoloc.utils.cadastre_index import (
    ParcellesIndex,
)


def _resolve(parcelle_ids, dvf_parcelle_ids):
    index = ParcellesIndex(
        departement="75",
        srid=2154,
        tree=shapely.STRtree(shapely.points([[0.0, 0.0], [1.0, 1.0]])),
        parcelle_ids=np.array(parcelle_ids, dtype=object),
        communes=np.array(["75056", "75056"], dtype=object),
    )
    # match() assigns in reverse, so the first candidate returned by the tree wins
    hit = np.array([0], dtype=np.int64)
    index._resolve_ambiguous_matches(
        point_pos=np.array([0, 0], dtype=np.int64),
        geom_pos=np.array([0, 1], dtype=np.int64),
        hit=hit,
        usable_rows=np.array([0], dtype=np.int64),
        longitude=np.array([2.0]),
        latitude=np.array([48.0]),
        dvf_parcelle_ids=dvf_parcelle_ids,
    )
    return hit


class TestResolveAmbiguousMatches:
    def test_without_dvf_ids_the_first_candidate_is_kept(self):
        assert _resolve(["A", "B"], None).tolist() == [0]

    def test_dvf_id_among_the_candidates_is_kept(self):
        assert _resolve(["A", "B"], np.array(["B"], dtype=object)).tolist() == [1]

    def test_dvf_id_outside_the_candidates_falls_back_to_the_first(self):
        assert _resolve(["A", "B"], np.array(["Z"], dtype=object)).tolist() == [0]

    def test_missing_dvf_id_does_not_match_a_null_parcelle(self):
        # a null cadastre id is not a candidate worth keeping when DVF has no id either
        assert _resolve(["A", None], None).tolist() == [0]

    def test_null_dvf_id_does_not_match_a_null_parcelle(self):
        assert _resolve(["A", None], np.array([None], dtype=object)).tolist() == [0]

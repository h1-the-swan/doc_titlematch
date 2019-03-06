#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `doc_titlematch` package."""


import unittest
import pandas as pd

import os
TEST_SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

from doc_titlematch import doc_titlematch


class TestDoc_titlematch(unittest.TestCase):
    """Tests for `doc_titlematch` package."""

    def setUp(self):
        self.origin_doc = doc_titlematch.Doc(id=12, title='The Eigenfactor Metrics', dataset='corpus', is_origin=True)

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_000_docobject(self):
        doc = doc_titlematch.Doc(id=12, title='some title', dataset='corpus')
        assert doc.id == 12
        assert doc.title == 'some title'
        assert doc.dataset == 'corpus'

    def test_001_docmatchobject(self):
        docmatch = doc_titlematch.DocMatch(self.origin_doc)
        assert docmatch.origin.id == 12
        assert docmatch.origin.is_origin is True

    def test_002_makequery(self):
        docmatch = doc_titlematch.DocMatch(self.origin_doc, target_index='mag_titles')
        docmatch.make_es_query()
        response = docmatch.es_response
        assert response.success()

    def test_003_numconfident(self):
        docmatch = doc_titlematch.DocMatch(self.origin_doc, target_index='mag_titles')
        docmatch.make_es_query()
        docmatch.get_number_confident_matches()
        assert docmatch.num_confident_matches == 1

        doc_negative = doc_titlematch.Doc(id=13, title="This article does not exist and shouldn't match any")
        docmatch = doc_titlematch.DocMatch(doc_negative, target_index='mag_titles')
        docmatch.make_es_query()
        docmatch.get_number_confident_matches()
        assert docmatch.num_confident_matches == 0

class TestMatchCollection(unittest.TestCase):
    """Tests for `doc_titlematch` matching a collection of papers."""

    def setUp(self):
        TEST_DATA_FNAME = os.path.join(TEST_SCRIPT_DIR, 'data/nas3_mag_doi_left_join.tsv')
        self.data = self.load_test_data(TEST_DATA_FNAME)
        # only take the first 50
        self.data = self.data.drop_duplicates(subset='wos_UID')
        self.data = self.data.head(50)
        self.collmatch = doc_titlematch.CollectionMatch(self.data.set_index('wos_UID')['wos_title'], 'wos', 'mag_titles')

    def load_test_data(self, fname):
        return pd.read_csv(fname, sep='\t')

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_000_check_testdata_is_loaded(self):
        assert len(self.data) > 0

    def test_001_check_collectionmatch_is_initialized(self):
        assert len(self.collmatch.docmatch_objects) == 50

    def test_002_perform_matching(self):
        for dm in self.collmatch.docmatch_objects:
            dm.get_number_confident_matches()
        for dm in self.collmatch.docmatch_objects:
            assert dm.num_confident_matches >= 0

    def test_003_get_all_confident_matches(self):
        confident_match_dict = self.collmatch.get_all_confident_matches()
        for origin_id, target_ids in confident_match_dict.items():
            docmatch = self.collmatch.docmatch_objects_by_id[origin_id]
            num_confident_matches = docmatch.num_confident_matches
            assert len(target_ids) == num_confident_matches

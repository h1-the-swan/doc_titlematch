#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `doc_titlematch` package."""


import unittest

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

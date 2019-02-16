# -*- coding: utf-8 -*-

"""Main module."""

from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'timeout': 60}])
from elasticsearch_dsl import Search


class Doc(object):

    """Document object"""

    def __init__(self, id=None, title=None, dataset=None, is_origin=False, docmatch=None, hit=None):
        """

        :id: unique identifier in this dataset
        :title: title of document
        :dataset: name of dataset which this document comes from (e.g., 'wos')
        :is_origin: bool, True if this Doc is an origin (document to be matched)
        :docmatch: list of DocMatch objects matched to this Doc (only if this Doc is origin)
        :hit: Hit object from elasticsearch-dsl-py (only if this Doc is not origin)

        """
        self.id = id
        self.title = title
        self.dataset = dataset
        self.is_origin = is_origin
        self.docmatch = docmatch
        self.hit = hit
        

class DocMatch(object):

    """Object for matching a title in one dataset (associated with an ID) with a title or titles in another dataset (associated with a different ID)"""

    def __init__(self, origin=None, elasticsearch_client=es, target_index=None):
        """
        :origin: Doc object for the document to be matched
        :target_index: the index in Elasticsearch to search for matches
        """

        self.origin = origin
        self.elasticsearch_client = elasticsearch_client
        self.target_index = target_index

        self.matches = []
        
    def make_es_query(self, index_to_query=None, field_to_query='title', id_field='Paper_ID'):
        """Make a query to elasticsearch

        :index_to_query: elasticsearch index to query
        :field_to_query: field to query
        :id_field: field for id in the corpus being queried
        :returns: self, with matched docs stored in DocMatch.matches

        """
        if index_to_query is None:
            index_to_query = self.target_index
        s = Search(using=self.elasticsearch_client, index=index_to_query)
        s = s.query('match', **{field_to_query: self.origin.title})
        r = s.execute()
        self.es_query = s
        self.es_response = r

        for hit in r:
            id = hit[id_field]
            doc = Doc(id=id, title=hit[field_to_query], dataset=self.target_index, hit=hit)
            self.matches.append(doc)
        self.origin.docmatch = self.matches
        return self

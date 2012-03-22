#! /usr/local/bin/python2.7
#coding=utf-8

#  docdb: Simple Document DB
#
#  Copyright (C) 2012 Miguel Rodriguez (marbdq@gmail.com)
#

__version__ = 0.1
__doc__ = """ docdb module - A simple document DB on top of sqlite, mysql or posgre..

DocDB(dbpath) - returns a new DB object implementing the given functions;
                dbpath sets the DB file to use. If none given, a db0.db
                file will be created.

DocDB objects have these methods:

 - get(key):        Gets the object stored in key. If there is no object,
                    None is returned.
 - set(key, doc):   Stores document doc into the DB using key as the index.
                    When successfull True is returned. If a document is
                    updated, the previous version is stored (and accessible
                    through versions().
 - mget(*args):     Multiple get. Receives multiple arguments, and returns
                    the related documents.
 - mset(*args):     Multiple set. Receives multiple tuples (key, doc) as
                    arguments, and stores the documents for the given keys.
 - versions(key):   Returns all the history of documents stored under a
                    given key.
 - info():          Return a dict with DB info (filesize, number of keys).
 - keys():          Returns a list of all the existing keys in DB.
 - flushall():      Removes all the documents from the DB. Complete delete.
 - compact():       Deletes all the old version documents from the DB,
                    freeing the space.

Basic usage:

    >>> import docdb
    >>> db = docdb.DocDB()
    >>> db['test'] = 'testing'
    >>> db['test']
    >>> u'testing'
"""

import sqlite3
import json
import os
import unittest
import time


class DocDB(object):
    """Creates instances of DocDB. Each instance will have a db connection.

    Objects have the following private attributes:
        dbpath:      db file path.
        db:          db connection object
    """

    def __init__(self, dbpath=os.path.join(os.path.dirname(__file__), 'db0.db')):
        """
        Generates a connection with the given DB file.
        @dbpath: path to the sqlite file to use. If None given, a default db0.db file will be created.
        """
        self._dbpath = dbpath
        self._db = sqlite3.Connection(self._dbpath)
        self._db.execute('CREATE TABLE IF NOT EXISTS document (id integer primary key autoincrement, \
                          key text, data text, valid integer)')
        self._db.execute('CREATE INDEX IF NOT EXISTS keyx ON document (key)')

    def get(self, key):
        """
        Searches and returns the doc for a given key.
        """
        for doc in self._db.execute('select data from document where key == ? and valid == 1', (key,)):
            return json.loads(doc[0])
        return None

    def set(self, key, doc):
        """
        Inserts a document (doc) into the DB.
        @doc: can be of any python data structure (string, number, dict, list, ...
        """
        data = json.dumps(doc)
        self._db.execute('UPDATE document SET valid = 0 WHERE key == ?', (key,))
        self._db.execute('insert into document (key, data, valid) values (?, ?, 1)', (key, data,))
        self._db.commit()
        return True

    def mget(self, *args):
        """
        Searches and returns the docs for a set of keys.
        Example: db.mget('key1', 'key2', 'key3')
        Or:
            >>> l = ['key1', 'key2', 'key3']
            >>> db.mget(*l)
        """
        results = []
        for key in args:
            for doc in self._db.execute('select key, data from document where key == ? and valid == 1', (key,)):
                results.append((doc[0], json.loads(doc[1])))
        return results

    def mset(self, *args):
        """
        Inserts a set of documents into the DB.
        Example:
            >>> l = [('key1', {'key': 'value'}), ('key2', {'key': 'value'}), ('key3', {'key': 'value'})]
            >>> db.mset(*l)
        """
        counter = 0
        for doc in args:
            data = json.dumps(doc[1])
            self._db.execute('UPDATE document SET valid = 0 WHERE key == ?', (doc[0],))
            self._db.execute('insert into document (key, data, valid) values (?, ?, 1)', (doc[0], data))
            counter += 1
            if counter > 1000:
                self._db.commit()
                counter = 0
        self._db.commit()
        return True


    def versions(self, key):
        """
        Lists all the documents related to a key.
        """
        results = []
        for doc in self._db.execute('select id, data from document where key == ?', (key,)):
            id, doc = doc
            result = json.loads(doc)
            results.append((id, result))
        return results

    def info(self):
        """
        Returns a dict with info about the current DB (including db filesize, number of keys, etc.).
        """
        import os
        dbsize = '{0} MB'.format(os.path.getsize(self._dbpath)/1024.0/1024.0) if self._dbpath != ':memory:' else -1
        keys = [doc for doc in self._db.execute('select count(*) from document where valid == 1')]
        keys = keys[0][0] if keys else keys
        return {'dbsize': dbsize, 'keys': keys}

    def keys(self):
        """
        Returns a list of ALL the keys in the current DB.
        """
        return [doc[0] for doc in self._db.execute('select key from document where valid == 1')]

    def flushall(self):
        """
        Deletes ALL the content from the DB.
        """
        self._db.execute('DROP TABLE document')
        self._db.commit()
        self.__init__()
        return True

    def compact(self, key=None):
        """
        Deletes ALL the versions for a given document or the entire DB.
        """
        if key:
            self._db.execute('DELETE FROM document WHERE key == ? and valid == 0', (key,))
        else:
            self._db.execute('DELETE FROM document WHERE valid == 0')
        self._db.commit()
        return True

    def __contains__(self, item):
        if self.get(item): return True
        return False

    def __getitem__(self, item):
        return self.get(item)

    def __setitem__(self, key, doc):
        return self.set(key, doc)

    def __delitem__(self, key):
        self._db.execute('UPDATE document SET valid == 0 WHERE key == ?', (key,))
        self._db.commit()
        return True

#-----------------------------------------------------------------------------------------------------------------------
class TestDocDB(unittest.TestCase):
    """
    Tests for the DocDb module. All the features should be tested.
    """
    def setUp(self):
        """
        Creates a DB connection for each test.
        """
        self.db = DocDB()

    def test_1_set(self):
        """
        Tests the set method for different data types.
        """
        text = self.db['text'] = 'text'
        self.assertTrue(text)
        number = self.db['number'] = 1
        self.assertTrue(number)
        document = self.db['document'] = {'key': 'value'}
        self.assertTrue(document)

    def test_2_get(self):
        """
        Tests the get method for different document types.
        """
        doc = self.db['text']
        self.assertEqual(doc, 'text')
        doc = self.db['number']
        self.assertEqual(doc, 1)
        doc = self.db['document']
        res = {'key': 'value'}
        self.assertEqual(doc, res)

    def test_3_mset(self):
        """
        Tests inserting several documents per batch.
        """
        l = [('one', {'key': 1}), ('two', {'key': 2}), ('three', {'key': 3})]
        result = self.db.mset(*l)
        self.assertTrue(result)

    def test_4_mget(self):
        """
        Tests getting several documents in the same request.
        """
        result = self.db.mget('one', 'two', 'three')
        expected = [(u'one', {u'key': 1}), (u'two', {u'key': 2}), (u'three', {u'key': 3})]
        self.assertEqual(result, expected)

    def test_5_versions(self):
        """
        Tests getting all the existing versions of a given key.
        """
        self.db['key_versions'] = 'versions'
        self.db['key_versions'] = 'versions_modified'
        versions = self.db.versions('key_versions')
        expected = [(7, u'versions'), (8, u'versions_modified')]
        self.assertEqual(versions, expected)

    def test_6_info(self):
        """
        Tests getting info about the DB.
        """
        info = self.db.info()
        self.assertIn('keys', info)
        self.assertEqual(info['keys'], 7)
        self.assertIn('dbsize', info)

    def test_7_keys(self):
        """
        Tests getting a list of existing keys.
        """
        keys = self.db.keys()
        expected = [u'text', u'number', u'document', u'one', u'two', u'three', u'key_versions']
        self.assertEqual(expected, keys)

    def test_8_compact(self):
        """
        Tests the removal of version documents.
        """
        versions = self.db.versions('key_versions')
        self.db.compact()
        versions_after = self.db.versions('key_versions')
        self.assertNotEqual(versions, versions_after)

    def test_9_flushall(self):
        """
        Tests the deletion of the current DB.
        """
        self.db.flushall()
        info = self.db.info()
        self.assertEqual(info['keys'], 0)

#-----------------------------------------------------------------------------------------------------------------------
class BenchmarkDocDB(unittest.TestCase):
    """
    Tests for the DocDb module. All the features should be tested.
    """
    def setUp(self):
        self.db = DocDB(':memory:')
        self.reqs = 1000
        self.t0 = time.clock()

    def test_1_set(self):
        for i in range(self.reqs):
            self.db[str(i)] = i
        print 'test_1_set:', self.reqs/(time.clock())-self.t0, 'req/s'

    def test_2_get(self):
        for i in range(self.reqs):
            doc = self.db[str(i)]
        print 'test_2_get:', self.reqs/(time.clock())-self.t0, 'req/s'

    def test_3_mset(self):
        l = []
        for i in range(self.reqs):
            l.append((str(i), i))
        self.db.mset(*l)
        print 'test_3_mset:', self.reqs/(time.clock())-self.t0, 'req/s'

    def test_4_mget(self):
        l = []
        for i in range(self.reqs):
            l.append(str(i))
        result = self.db.mget(*l)
        print 'test_4_mget:', self.reqs/(time.clock())-self.t0, 'req/s'

    def test_cleanup(self):
        self.db.flushall()

#-----------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    unittest.main(verbosity=2)

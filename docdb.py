#! /usr/local/bin/python2.7
#coding=utf-8

#  docdb: Simple Document DB...
#
#  Copyright (C) 2012 Miguel Rodriguez (marbdq@gmail.com)
#  docdb uses dal.py, a web2py component (www.web2py.com).
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

import json
import unittest
import time
import sys
import os
PATH = os.path.dirname(__file__)
sys.path.append(PATH)

try:
    from gluon.dal import DAL, Field
except:
    from dal import DAL, Field


class DocDB(object):
    """Creates instances of DocDB. Each instance will have a db connection.

    Extracted from web2py docs:
    ---------------------------
    Here are examples of connection strings for specific types of supported back-end databases
    (in all cases, we assume the database is running from localhost on its default port and is named "test"):

    SQLite	sqlite://storage.db
    MySQL	mysql://username:password@localhost/test
    PostgreSQL	postgres://username:password@localhost/test
    MSSQL	mssql://username:password@localhost/test
    FireBird	firebird://username:password@localhost/test
    Oracle	oracle://username/password@test
    DB2	db2://username:password@test
    Ingres	ingres://username:password@localhost/test
    Informix	informix://username:password@test
    Google App Engine/SQL	google:sql
    Google App Engine/NoSQL	google:datastore

    Notice that in SQLite the database consists of a single file. If it does not exist, it is created.
    This file is locked every time it is accessed. In the case of MySQL, PostgreSQL, MSSQL, FireBird,
    Oracle, DB2, Ingres and Informix the database "test" MUST exist.

    """

    def __init__(self, conn='sqlite://temp.db', pool_size=0, migrate=True):
        """
        Generates a connection with the given DB.
        @connection: system path to the sqlite file to use or DB connection string. If None given,
        a default temp.db file will be created.
        """
        if '://' not in conn:
            print """Connection string needed!\n \
                  Some examples:\n \
                  SQLite  sqlite://storage.db
                  MySQL   mysql://username:password@localhost/test \
                  PostgreSQL  postgres://username:password@localhost/test
            """
            sys.exit(2)
        self._conn = conn
        self._db = DAL(conn, folder=PATH, pool_size=pool_size)
        self._db.define_table('documents',
                   Field('key'),
                   Field('data', 'text'),
                   Field('valid', 'boolean'),
                   migrate = migrate)

        if not self._db(self._db.documents).count():
            try:
                self._db.executesql('CREATE INDEX keyx ON documents (key)') #CREATE INDEX IF NOT EXISTS
            except Exception:
                self._db.rollback()

    def get(self, key):
        """
        Searches and returns the doc for a given key.
        """
        db = self._db
        doc = db((db.documents.key==key) & (db.documents.valid==True)).select(db.documents.data).first()
        if doc:
            return json.loads(doc['data'])
        return None

    def set(self, key, doc):
        """
        Inserts a document (doc) into the DB.
        @doc: can be of any python data structure (string, number, dict, list, ...
        """
        db = self._db
        data = json.dumps(doc)
        db((db.documents.key==key) & (db.documents.valid==True)).update(valid=False)
        db.documents.insert(key=key, data=data, valid=True)
        db.commit()
        return True

    def mset(self, docs):
        """
        Inserts a set of documents into the DB.
        Example:
            >>> l = [('key1', {'key': 'value'}), ('key2', {'key': 'value'}), ('key3', {'key': 'value'})]
            >>> db.mset(*l)
        """
        db = self._db
        counter = 0
        for doc in docs:
            key, data = doc
            data = json.dumps(data)
            db((db.documents.key==key) & (db.documents.valid==True)).update(valid=False)
            db.documents.insert(key=key, data=data, valid=True)
            counter += 1
            if counter > 1000:
                db.commit()
                counter = 0
        db.commit()
        return True

    def versions(self, key):
        """
        Lists all the documents related to a key.
        """
        db = self._db
        results = []
        for doc in db(db.documents.key == key).select():
            id, data, valid = doc['id'], json.loads(doc['data']), doc['valid']
            results.append((id, data, valid))
        return results

    def revert(self, key, version):
        """
        Reverts to a previous version of the document.
        """
        db = self._db
        vers = self.versions(key)
        for doc in vers:
            id, data, valid = doc
            if id == version:
                db((db.documents.key==key) & (db.documents.valid==True)).update(valid=False)
                db(db.documents.id==id).update(valid=True)
                db.commit()
                return True
        return False

    def info(self):
        """
        Returns a dict with info about the current DB (including db filesize, number of keys, etc.).
        """
        dbsize = -1
        db = self._db
        if "postgres" in self._conn:
            dbsize = db.executesql("SELECT pg_size_pretty(pg_database_size('%s'));" % self.dbname)[0][0]
        num_keys = db(db.documents.valid==True).count()
        return dict(keys=num_keys, dbsize=dbsize)

    def keys(self):
        """
        Returns a list of ALL the keys in the current DB.
        """
        db = self._db
        return [doc['key'] for doc in db(db.documents.valid==True).select(db.documents.key)]

    def flushall(self):
        """
        Deletes ALL the content from the DB.
        TODO: Use truncate.
        """
        self._db.documents.truncate()
        self._db.commit()
        #self.__init__()
        return True

    def compact(self, key=None):
        """
        Deletes ALL the versions for a given document or the entire DB.
        """
        db = self._db
        if key:
            db((db.documents.key==key) & (db.documents.valid==False)).delete()
        else:
            db(db.documents.valid==False).delete()
        db.commit()
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
        self.db = DocDB(migrate=True)

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
        result = self.db.mset(l)
        self.assertTrue(result)

    def test_5_versions(self):
        """
        Tests getting all the existing versions of a given key.
        """
        self.db['key_versions'] = 'versions'
        self.db['key_versions'] = 'versions_modified'
        versions = self.db.versions('key_versions')
        expected = [(7, u'versions', False), (8, u'versions_modified', True)]
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
        self.db = DocDB(migrate=True) #'sqlite://:memory:'
        self.reqs = 100
        self.t0 = time.clock()

    def test_1_set(self):
        for i in range(self.reqs):
            self.db[str(i)] = i
        print 'test_1_set:', self.reqs/(time.clock())-self.t0, 'req/s'

    def test_2_get(self):
        for i in range(self.reqs):
            _ = self.db[str(i)]
        print 'test_2_get:', self.reqs/(time.clock())-self.t0, 'req/s'

    def test_3_mset(self):
        l = []
        for i in range(self.reqs):
            l.append((str(i), i))
        self.db.mset(l)
        print 'test_3_mset:', self.reqs/(time.clock())-self.t0, 'req/s'

    def test_cleanup(self):
        self.db.flushall()

#-----------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    unittest.main(verbosity=2)

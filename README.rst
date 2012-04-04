docDB
=====

docDB is a simple to use, lightweight and fast document database on top of SQLite, MySQL, Posgre and many other DB engines (thanks to the DAL component from web2py).
It has a versioning system, so that all changes for a key are stored and can be reverted.

docDB has no external dependencies, and comes under a MIT license.

Basic usage
------------

    >>> import docdb
    >>> db = docdb.DocDB()

    >>> db.set('key', 'value')
    True
    >>> db.get('key')
    u'value'
    >>> db['key'] = 'value2'
    True
    >>> db['key']
    u'value'
    
    >>> db.versions('key')
    [(1, u'value', False), (2, u'value1', True)] #Shows all historical values, version numbers and marks the current one to True

    >>> db.revert('key', 2)
    True
    >>> db.versions('key')
    [(1, u'value', False), (2, u'value1', True)]

    >>> db['new_key'] = 'new value'
    >>> db.keys()
    ['key', 'new_key'] #Lists all existing keys in the DB.

    >>> db.mset([('key3', 'value3'), ('key4', {'key': 'value'})])  #You can set multiple keys at the same go
    True
    >>> db.keys()
    ['key', 'new_key', 'key3', 'key4']
    
    >>> db.compact() #You can delete old versions to save disk space
    True
    >>> db.versions('key')
    [(2, u'value1', True)]  #Old versions have been removed!

    >>> db.flushall() #Deletes all DB records.
    True
    >>> db.info()
    {'dbsize': -1, 'keys': 0}
    


Installation
------------

    $ pip install docdb

    or

    $ easy_install docdb

    or

    Download, and python setup.py install


Links
------

* `website <http://xxx/>`_
* `documentation <http://xxx>`_
* `pypi <http://pypi.python.org/pypi/docdb>`_
docDB
=====

docDB is a simple to use, lightweight and fast document database on top of SQLite, MySQL and Posgre. It has a versioning system, so that all changes for a key are stored and can be reverted.

docDB has no external dependencies, and comes under a MIT license.

Basic usage
------------

    >>> import docdb
    >>> db = docdb.DocDB()

    >>> db.set('key', 'value')
    True
    >>> db.get('key')
    u'value'

    or

    >>> db['key'] = 'value'
    True
    >>> db['key']
    u'value'


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
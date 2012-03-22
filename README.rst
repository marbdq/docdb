docDB
``````
docDB is a simploe to use, lightweight and fast document database on top of SQLite, MySQL and Posgre.

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
-------------

    $ pip install docdb


Links
------

* `website <http://xxx/>`_
* `documentation <http://xxx>`_
* `pypi
  <http://pypi.python.org/pypi/pickleDB>`_
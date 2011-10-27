"""
.. module:: DataStore
    :synopsis: Module responsible for all the data-persistence action.

.. moduleauthor:: David Garcia Quintas <dgarciaquintas@lbl.gov>

"""

import pycassa
import collections

class DataStore(object):

    def __init__(self, keyspace, cf, servers=['localhost:9160']):
        """
            :param str keyspace: The keyspace to consider.
            :param str cf: The column family (collection/table) storing the data.
            :param server: List of servers to connect to. 

            Refer to :mod:`pycassa.pool.ConnectionPool` for more info.
        """
        self._pool = pycassa.pool.ConnectionPool(keyspace, servers)

        #TODO: make it a ColumnMapFamily in order to deal with the conversions 
        #to/form strings to actual data types
        self._cf = pycassa.ColumnFamily(self._pool, cf)


    def write(self, key, data_dict):
        """ Performs a persistent write.

            :param str key: The key under which `data_dict` will be stored. 
            :param dict data_dict: Columns to be inserted in \
            ``{col_name: col_value, ...}`` format. 
        """
        self._cf.insert(key, data_dict)

    def read(self, keys, columns=None):
        """ Performs a batch read for the given keys.

            Missing keys will be returned with an empty dictionary as their value.

            :param list keys: List of key names to retrieve.
            :param list columns: Columns to return for each of the keys.
            :rtype: a dictionary of the form ``{key1: {col: val, ...}, key2: {col, val, ...}}``
        """
        res = collections.OrderedDict( (key, {}) for key in keys )
        res.update(self._cf.multiget(keys, columns))
        
        return res


    






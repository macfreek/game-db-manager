#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Interface to Filemaker database which stores registration codes of games and applications.

Requirements:
port install py-pyodbc  (also installs unixODBC)
install Filemaker ODBC, can be downloaded from http://www.filemaker.com/support/downloads/
"""
from os.path import exists
import logging

# Type hints
try:
    from typing import Dict, Any, Callable, Union, Tuple, Optional, Iterator
    STR_OR_DICT = Union[str, Dict[str, Union[str, None]]]
except ImportError:
    from collections import defaultdict
    Dict = Union = Optional = Tuple = Iterator = defaultdict(str)  # type: ignore
    Any = Callable = STR_OR_DICT = ''  # type: ignore

# third party packages
try:
    import pyodbc
except ImportError:
    raise ImportError("Package pyodbc is not available. Install using e.g. "
            "`port install py-pyodbc` or `port install unixODBC; pip install pyodbc --no-binary :all:`.") from None

# for some quircks on FileMaker SQL, see e.g.:
# https://coddswallop.wordpress.com/2015/07/08/accessing-filemaker-pro-server-11-via-odbc-sql/

# TODO: add type hints


class FileMaker:
    def __init__(self, database: str, username='Admin', password='') -> None:
        self.database = database  # type: str
        self.username = username  # type: str
        self.password = password  # type: str
        self.fm_odbc_path = '/Library/ODBC/FileMaker ODBC.bundle/Contents/MacOS/fmodbc.so'
        self.server_ip = '127.0.0.1'
        self.server_port = 2399
        self._connection = None  # type: Optional[pyodbc.Connection]
        self._precommit_hook1 = None  # type: Optional[Callable]
        self._precommit_hook1_called = False

    def connect(self) -> None:
        """Connect to the filemaker database.
        Provide useful error messages in case connection fails."""
        dsn = "DRIVER=%s;Server=%s;Port=%s;Database=%s;UID=%s;PWD=%s" % \
                (self.fm_odbc_path, self.server_ip, self.server_port, 
                self.database, self.username, self.password)
        logging.debug("Connect to FileMaker ODBC with DSN %s" % (dsn))
        try:
            # Port seems to be ignored.
            self._connection = pyodbc.connect(dsn)
        except pyodbc.Error as e:
            if not exists(self.fm_odbc_path):
                logging.error("File %s not found. Install the FileMaker ODBC Driver. " \
                        "Download from " \
                        "https://support.filemaker.com/s/answerview?language=en_US&anum=12921." % \
                        (self.fm_odbc_path))
            elif "Failed to connect to listener" in e.args[1]:
                logging.error("Can't connect to FileMaker. "\
                        "Is it running, and is ODBC Sharing enabled?")
            elif "Unable to open file" in e.args[1]:
                logging.error("Can connect to FileMaker, but database '%s' not found." % \
                        (self.database))
            raise
        # Encoding must be set on Python 3.5 or 3.6, otherwise it will raise an exception:
        #     FQL0001/(1:1): There is an error in the syntax of the query. (8310) (SQLExecDirectW)'
        # see https://github.com/mkleehammer/pyodbc/issues/89
        # or https://github.com/mkleehammer/pyodbc/wiki/Unicode
        self._connection.setencoding(encoding='utf-8')
        self._connection.setdecoding(pyodbc.SQL_CHAR, encoding='macroman')
        # self._connection.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
        # self._connection.setdecoding(pyodbc.SQL_WMETADATA, encoding='utf-32le')
    
    def close(self) -> None:
        if self._connection:
            logging.debug("Close database connection")
            self._connection.close()
            self._connection = None
    
    def __del__(self):
        self.close()
    
    def set_precommit_hook1(self, func: Callable) -> None:
        self._precommit_hook1 = func
        self._precommit_hook1_called = False
    
    def call_precommit_hooks(self) -> None:
        if self._precommit_hook1 and not self._precommit_hook1_called:
            self._precommit_hook1()
            self._precommit_hook1_called = True
    
    def commit(self) -> None:
        if self._connection:
            self._connection.commit()
        else:
            logging.error("FileMaker.commit() called before connection was established.")
    
    def select(self, fields: Tuple[str, ...], tablename: str, where: STR_OR_DICT={}, 
                order: Optional[str]=None) -> Iterator[Dict[str, str]]:
        """Yield all selected records in the given table.
        
        :param tuple fields: A tuple with the fieldnames to return.
        :param str tablename: name of the table.
        :param str or dict where: (optional) selection of the fields.
        :param str order: (optional) field to return.
        :return: Yield a dict with fieldname: value for each record.
        """
        if not self._connection:
            self.connect()
        wherevalues = ()  # type: Tuple[str, ...]
        if isinstance(where, dict):
            wherevalues = tuple(v for v in where.values() if v)
            where = ' AND '.join(k + ('=?' if v else 'IS NULL') for k, v in where.items())
        cursor = self._connection.cursor()
        queryparts = {
            'fields': ','.join(fields),
            'tablename': tablename,
            'where': 'WHERE ' + where if where else '',
            'order': 'ORDER BY ' + order if order else '',
        }
        query = "SELECT {fields} FROM {tablename} {where} {order}".format(**queryparts)
        logging.debug('%s%s' % (query, ' with ' + str(wherevalues) if wherevalues else ''))
        try:
            cursor.execute(query, *wherevalues)
        except pyodbc.Error as e:
            logging.error(query + (' with ' + str(wherevalues) if wherevalues else ''))
            logging.error(str(e))
            raise
        # support "abc AS def" or "FUNCTION(abc) AS def" syntax
        fields = [field.split(' AS ')[-1] for field in fields]
        while True:
            record = cursor.fetchone()
            if record is None:
                break
            record = dict(zip(fields, record))
            yield record
        cursor.close()

    def update(self, tablename: str, where: STR_OR_DICT, update: STR_OR_DICT) -> int:
        """Update the selected records in the given table.
        You must call commit() before the database is actually changed.
        
        :param str tablename: name of the table.
        :param str or dict where: selection of the records to update.
        :param str or dict update: fields and values to change.
        :return: The number of modified records (if available)
        """
        if not self._connection:
            self.connect()
        self.call_precommit_hooks()
        wherevalues = ()
        if isinstance(where, dict):
            wherevalues = tuple(v for v in where.values() if v)  # type: ignore
            where = ' AND '.join(k + ('=?' if v else ' IS NULL') for k, v in where.items())
        if isinstance(update, dict):
            wherevalues = tuple(update.values()) + wherevalues  # type: ignore
            update = ', '.join((k + '=?') for k in update.keys())
        
        cursor = self._connection.cursor()
        queryparts = {
            'tablename': tablename,
            'where': where,
            'update': update,
        }
        query = "UPDATE {tablename} SET {update} WHERE {where}".format(**queryparts)
        logging.debug('%s%s' % (query, ' with ' + str(wherevalues) if wherevalues else ''))
        if not where:
            logging.error("Update without a where is not supported. It would modify all records.")
            return 0
        try:
            cursor.execute(query, *wherevalues)
        except pyodbc.Error as e:
            if wherevalues:
                query += ' with %d arguments: (%s)' % \
                        (len(wherevalues), ', '.join(type(v).__name__ for v in wherevalues))
            logging.error(query)
            logging.error('%s %s' % (type(e).__name__, str(e.args[1])))
            raise
        rowcount = cursor.rowcount
        cursor.close()
        return rowcount

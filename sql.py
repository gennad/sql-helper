#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Module for working with the database."""

import logging
from numbers import Number
from sqlite3 import connect

__author__ = "Gennadiy Zlobin"
__email__ = "gennad.zlobin@gmail.com"
__status__ = "Production"
__version__ = "1.0.1"

logging.basicConfig(level=logging.DEBUG,
        format=('%(filename)s: '
            '%(levelname)s: '
            '%(funcName)s(): '
            '%(lineno)d:\t'
            '%(message)s'))

class InvalidTypeError(Exception):
    """Raises in case of wrong type of the argument in methods of
    Column subclasses."""
    pass

class InvalidOrderError(Exception):
    """Raises if SqlBuilder method was called in improper order."""
    pass

class Result(object):
    """Represents results of fetched data."""
    pass

class Singleton(type):
    """Singletone meta class."""
    def __init__(cls, name, bases, dict):
        super(Singleton, cls).__init__(name, bases, dict)
        cls.instance = None

    def __call__(cls,*args,**kw):
        if cls.instance is None:
            cls.instance = super(Singleton, cls).__call__(*args, **kw)
        return cls.instance

class SingletoneData(object):
    """Singletone class that contains information that should be shared between
    Column and SqlBuilder classes."""
    def __init__(self):
        self.last_method = ''
        self.added = set()
        self.data = []

    __metaclass__ = Singleton

class Column(object):
    """Base class for columns."""
    def __init__(self):
        self.sdata = SingletoneData()

    @property
    def table_name(self):
        """Returns table name."""
        return self.table

    @table_name.setter
    def table_name(self, name):
        """
        Setter for table name.

        Arguments:
            name -- new table name
        """
        self.table = name

    def create(self):
        """Returns column name and type for CREATE TABLE expression."""
        raise NotImplementedError()

    def __lt__(self, right):
        """
        Overrides '<' sign.

        Arguments:
            right -- object to the right from the sign.
        Returns:
            text representation of the condition.
        """
        if isinstance(right, Column):
            right = ''.join((right.table_name, '.', right.column_name))

        left = ''.join((self.table_name, '.', self.column_name))
        sql = ''.join((left, ' < ', '?'))
        self.sdata.data.append(right)
        return sql

    def __le__(self, right):
        """
        Overrides '<=' sign.

        Arguments:
            right -- object to the right from the sign.
        Returns:
            text representation of the condition.
        """
        if isinstance(right, Column):
            right = ''.join((right.table_name, '.', right.column_name))

        left = ''.join((self.table_name, '.', self.column_name))
        sql = ''.join((left, ' <= ', '?'))
        self.sdata.data.append(right)
        return sql

    def __gt__(self, right):
        """
        Overrides '>' sign.

        Arguments:
            right -- object to the right from the sign.
        Returns:
            text representation of the condition.
        """
        if isinstance(right, Column):
            right = ''.join((right.table_name, '.', right.column_name))

        left = ''.join((self.table_name, '.', self.column_name))
        sql = ''.join((left, ' > ', '?'))
        self.sdata.data.append(right)
        return sql

    def __ge__(self, right):
        """
        Overrides '>=' sign.

        Arguments:
            right -- object to the right from the sign.
        Returns:
            text representation of the condition.
        """
        if isinstance(right, Column):
            right = ''.join((right.table_name, '.', right.column_name))

        left = ''.join((self.table_name, '.', self.column_name))
        sql = ''.join((left, ' >= ', '?'))
        self.sdata.data.append(right)
        return sql

    def __eq__(self, right):
        """
        Overrides '==' sign.

        Arguments:
            right -- object to the right from the sign.
        Returns:
            text representation of the condition.
        """
        if isinstance(right, Column):
            if self.sdata.last_method != 'Update':
                right = ''.join((right.table_name, '.', right.column_name))
            else:
                right = right.column_name

        if self.sdata.last_method != 'Update':
            left = ''.join((self.table_name, '.', self.column_name))
        else:
            left = self.column_name

        sql = ''.join((left, ' = '))
        if self.sdata.last_method == 'Join':
            sql += right
        else:
            sql += '?'
            self.sdata.data.append(right)
        return sql

    def __ne__(self, right):
        """
        Overrides '!=' sign.

        Arguments:
            right -- object to the right from the sign.
        Returns:
            text representation of the condition.
        """
        if isinstance(right, Column):
            if self.sdata.last_method != 'Update':
                right = ''.join((right.table_name, '.', right.column_name))
            else:
                right = right.column_name

        if self.sdata.last_method != 'Update':
            left = ''.join((self.table_name, '.', self.column_name))
        else:
            left = self.column_name

        sql = ''.join((left, ' != '))
        if self.sdata.last_method == 'Join':
            sql += right
        else:
            sql += '?'
            self.sdata.data.append(right)
        return sql

    def In(self, arg):
        """
        Method for IN (set) expression.

        Arguments:
            arg -- sequence for checking in
        Returns:
            text representation of the condition.
        """
        left = ''.join((self.table_name, '.', self.column_name))
        right = str(tuple(arg))
        sql = ''.join((left, ' IN ', right))
        return sql

    def NotIn(self, arg):
        """
        Method for NOT IN (set) expression.

        Arguments:
            arg -- sequence for checking in
        Returns:
            text representation of the condition.
        """
        left = ''.join((self.table_name, '.', self.column_name))
        right = str(tuple(arg))
        sql = ''.join((left, ' NOT IN ', right))
        return sql

class IntegerColumn(Column):
    """Represents integer column in the database."""
    def create(self):
        """Return column name and type for CREATE TABLE expression."""
        return self.column_name + ' INT'

    def validate_type(fn):
        """Decorator for validating type. In expressions with integer column
        the second parameter must be integer or Column subclass.

        Arguments:
            fn -- decorated function
        """
        def nested(*args, **kwargs):
            """
            Checks type. Raises InvalidTypeError if argument is not
            Number or Column subclass.
            Raises:
                InvalidTypeError
            """
            right = args[1]
            if not isinstance(right, Number) and not isinstance(right, Column):
                raise InvalidTypeError('Invalid type of {0}'.format(right))
            return fn(*args, **kwargs)
        return nested

    @validate_type
    def __lt__(self, right):
        """
        Overrides '<' sign.

        Arguments:
            right -- object to the right from the sign.
        Returns:
            text representation of the condition.
        """
        return super(IntegerColumn, self).__lt__(right)

    @validate_type
    def __le__(self, right):
        """
        Overrides '<=' sign.

        Arguments:
            right -- object to the right from the sign.
        Returns:
            text representation of the condition.
        """
        return super(IntegerColumn, self).__le__(right)

    @validate_type
    def __gt__(self, right):
        """
        Overrides '>' sign.

        Arguments:
            right -- object to the right from the sign.
        Returns:
            text representation of the condition.
        """
        return super(IntegerColumn, self).__gt__(right)

    @validate_type
    def __ge__(self, right):
        """
        Overrides '>=' sign.

        Arguments:
            right -- object to the right from the sign.
        Returns:
            text representation of the condition.
        """
        return super(IntegerColumn, self).__ge__(right)

    @validate_type
    def __eq__(self, right):
        """
        Overrides '==' sign.

        Arguments:
            right -- object to the right from the sign.
        Returns:
            text representation of the condition.
        """
        return super(IntegerColumn, self).__eq__(right)

    @validate_type
    def __ne__(self, right):
        """
        Overrides '!=' sign.

        Arguments:
            right -- object to the right from the sign.
        Returns:
            text representation of the condition.
        """
        return super(IntegerColumn, self).__ne__(right)


class StringColumn(Column):
    """Represents text column in the database."""
    def create(self):
        """Return column name and type for CREATE TABLE expression."""
        return self.column_name + ' TEXT'

    def validate_type(fn):
        """
        Decorator for validating type. In expressions with text column
        the second parameter must be string or Column subclass.

        Arguments:
            fn -- decorated function
        """
        def nested(*args, **kwargs):
            """Checks type. Raises InvalidTypeError if argument is
            not basestring or Column subclass."""
            right = args[1]
            if (not isinstance(right, basestring)
                    and not isinstance(right, Column)):
                raise InvalidTypeError('Invalid type of {0}'.format(right))
            return fn(*args, **kwargs)
        return nested

    def __lt__(self, right):
        """
        Overrides '<' sign.

        Arguments:
            right -- object to the right from the sign.
        Returns:
            text representation of the condition.
        """
        raise InvalidTypeError('Invalid sign')

    @validate_type
    def __le__(self, right):
        """
        Overrides '<=' sign.

        Arguments:
            right -- object to the right from the sign.
        Returns:
            text representation of the condition.
        """
        raise InvalidTypeError('Invalid sign')

    @validate_type
    def __gt__(self, right):
        """
        Overrides '>' sign.

        Arguments:
            right -- object to the right from the sign.
        Returns:
            text representation of the condition.
        """
        raise InvalidTypeError('Invalid sign')

    @validate_type
    def __ge__(self, right):
        """
        Overrides '>=' sign.

        Arguments:
            right -- object to the right from the sign.
        Returns:
            text representation of the condition.
        """
        raise InvalidTypeError('Invalid sign')

    @validate_type
    def __eq__(self, right):
        """
        Overrides '==' sign.

        Arguments:
            right -- object to the right from the sign.
        Returns:
            text representation of the condition.
        """
        return super(StringColumn, self).__eq__(right)

    @validate_type
    def __ne__(self, right):
        """
        Overrides '!=' sign.

        Arguments:
            right -- object to the right from the sign.
        Returns:
            text representation of the condition.
        """
        return super(StringColumn, self).__ne__(right)

class DateTimeColumn(Column):
    """Represents datetime column in the database."""
    def create(self):
        """Return column name and type for CREATE TABLE expression."""
        return ''.join((' ', self.column_name, ' DATETIME'))

    def validate_type(fn):
        """Decorator for validating type. In expressions with datetime column
        the second parameter must be basestring subclass in proper format
        or Column subcass.

        Arguments:
            fn -- decorated function
        """
        def nested(*args, **kwargs):
            """
            Checks type. Raises InvalidTypeError if argument is not
            basestring subclass in propert format or Column subclass.

            Raises:
                InvalidTypeError
            """
            right = args[1]
            if (not isinstance(right, Column) and
                    not isinstance(right, basestring)
                    or len(right.split('-')) != 3):
                raise InvalidTypeError('Invalid type of {0}'.format(right))
            return fn(*args, **kwargs)
        return nested

    @validate_type
    def __lt__(self, right):
        """
        Overrides '<' sign.

        Arguments:
            right -- object to the right from the sign.
        Returns:
            text representation of the condition.
        """
        return super(DateTimeColumn, self).__lt__(right)

    @validate_type
    def __le__(self, right):
        """
        Overrides '<=' sign.

        Arguments:
            right -- object to the right from the sign.
        Returns:
            text representation of the condition.
        """
        return super(DateTimeColumn, self).__le__(right)

    @validate_type
    def __gt__(self, right):
        """
        Overrides '>' sign.

        Arguments:
            right -- object to the right from the sign.
        Returns:
            text representation of the condition.
        """
        return super(DateTimeColumn, self).__gt__(right)

    @validate_type
    def __ge__(self, right):
        """
        Overrides '>=' sign.

        Arguments:
            right -- object to the right from the sign.
        Returns:
            text representation of the condition.
        """
        return super(DateTimeColumn, self).__ge__(right)

    @validate_type
    def __eq__(self, right):
        """
        Overrides '==' sign.

        Arguments:
            right -- object to the right from the sign.
        Returns:
            text representation of the condition.
        """
        return super(DateTimeColumn, self).__eq__(right)

    @validate_type
    def __ne__(self, right):
        """
        Overrides '!=' sign.

        Arguments:
            right -- object to the right from the sign.
        Returns:
            text representation of the condition.
        """
        return super(DateTimeColumn, self).__ne__(right)

class MetaTable(type):
    """Meta class for Table."""
    def __new__(cls, name, bases, attrs):
        """
        Inspects table columns and assigns them table name and column name.
        Adds a new attribute 'all' that keeps all available
        columns of the table.
        """
        attrs['all'] = []
        for key in attrs:
            if issubclass(attrs[key].__class__, Column):
                attrs[key].table_name = name
                attrs[key].column_name = key
                attrs['all'].append(attrs[key])
        return super(MetaTable, cls).__new__(cls, name, bases, attrs)

class Table(object):
    """Base class for all tables."""
    @classmethod
    def get_name(cls):
        """Returns table_name."""
        return cls.__name__.lower()

    __metaclass__ = MetaTable

class Db(object):
    """
    Base class for database abstraction. Should be subclassed with the
    concrete implementations.

    Keeps all tables in the database represented as nested classes.
    """
    def __init__(self, db_type='sqlite', params=None):
        """Constructor creates delegated Db subclass depending on
        requested db type."""
        if not params:
            params = {}
        if db_type == 'sqlite':
            self.db = SQLiteDb(params)
        elif db_type == 'mysql':
            self.db = MySQLDb(params)
        else:
            raise NotImplementedError()

    def _Query(self):
        """Queries database."""
        raise NotImplementedError()

    def _OpenConnection(self):
        """Opens connection with the database."""
        raise NotImplementedError()

    def _CloseConnection(self):
        """Closes connection with the database."""
        raise NotImplementedError()

    def _GetResults(self, select_columns):
        """
        Returns results from cursor object.

        Arguments:
            select_columns -- columns to fetch from
        Returns:
            list of Result objects
        """
        raise NotImplementedError()

    def _Commit(self):
        """Commits a transaction."""
        raise NotImplementedError()

    def Fetch(self, sqlbuilder, result=False, commit=False):
        """
        Facade for Db class for executing queries
        and fetching the results.

        Arguments:
            sqlbuilder -- SqlBuilder instance
            result -- is it need to return result
            commit -- is it need to commit
        Returns:
            if result is True, returns list of Result objects
        """
        raise NotImplementedError()

    class Users(Table):
        """ Represents Users table in the database."""
        id = IntegerColumn()
        login = StringColumn()
        last_login_time = DateTimeColumn()
        flag = StringColumn()
        position = IntegerColumn()
        class_field = StringColumn()

    class Managers(Table):
        """ Represents Managers table in the database."""
        id = IntegerColumn()
        photo = StringColumn()

class SQLiteDb(Db):
    """SQLite3 implementation."""
    def __init__(self, params):
        self.cursor = None
        self.connection = None
        self.name = params.get('name', 'sample.db')

    def _Query(self):
        """Queries database."""
        logging.info('Sql = {0}, data={1}'.format(self.sql, self.data))
        self.cursor.execute(self.sql, self.data)

    def _Commit(self):
        """Commits a transaction."""
        self.connection.commit()

    def _OpenConnection(self):
        """Opens connection with the database."""
        self.connection = connect(self.name)
        self.cursor = self.connection.cursor()

    def _CloseConnection(self):
        """Closes connection with the database."""
        self.cursor.close()
        self.connection.close()

    def _GetResults(self, select_columns):
        """
        Returns results from cursor object.

        Arguments:
            select_columns -- columns to fetch from
        Returns:
            list of Result objects
        """
        results = []
        for row in self.cursor:
            myobj = Result()
            for i, name in enumerate(select_columns):
                # Split name and take last part if there is a dot in it
                name = name.split('.')[-1] if name.find('.') != -1 else name
                setattr(myobj, name, row[i])
            results.append(myobj)
        return results

    def Fetch(self, sqlbuilder, result=False, commit=False):
        """
        Facade for Db class for executing queries
        and fetching the results.

        Arguments:
            sqlbuilder -- SqlBuilder instance
            result -- is it need to return result
            commit -- is it need to commit
        Returns:
            if result is True, returns list of Result objects
        """
        self.sql = ''.join(sqlbuilder.sql)
        self.data = sqlbuilder.sdata.data
        try:
            self._OpenConnection()
            self._Query()
            if commit:
                self._Commit()
            if result:
                return self._GetResults(sqlbuilder.select_columns)
        finally:
            self._CloseConnection()

class MySQLDb(Db):
    """MySQL implementation."""
    def __init__(self, host, username, password,
            db_name='sample'):
        raise NotImplementedError()

class SqlBuilder(object):
    """Class for building sql queries."""
    def __init__(self):
        self.sql = []
        self.sdata = SingletoneData()
        self.constructed_sql = ''

    def check_order(fn):
        """
        Decorator for checking order of called methods.

        Arguments:
            fn -- decorated function
        """
        def nested(*args, **kwargs):
            """
            Checks type. Raises InvalidOrderError if the function
            was called not in proper order.

            Arguments:
                args - tuple of arguments of the function
                kwargs - dict of keyword arguments of the function

            Raises:
                InvalidOrderError
            """

            sqlbuilder = args[0]
            def clear_data():
                sqlbuilder.sql[:] = []
                sqlbuilder.sdata.data[:] = []

            if fn.__name__ == 'Select':
                clear_data()
                sqlbuilder.sdata.last_method = 'Select'
            elif fn.__name__ == 'Delete':
                clear_data()
                sqlbuilder.sdata.last_method = 'Delete'
            elif fn.__name__ == 'Update':
                clear_data()
                sqlbuilder.sdata.last_method = 'Update'
            elif fn.__name__ == 'Insert':
                clear_data()
                sqlbuilder.sdata.last_method = 'Insert'
            elif fn.__name__ == 'CreateTable':
                clear_data()
                sqlbuilder.sdata.last_method = 'CreateTable'
            elif fn.__name__ == 'DropTable':
                clear_data()
                sqlbuilder.sdata.last_method = 'DropTable'
            elif fn.__name__ == 'From':
                if sqlbuilder.sdata.last_method not in ['Select', 'Delete']:
                    raise InvalidOrderError('Wrong order')
                sqlbuilder.sdata.last_method = 'From'
            elif fn.__name__ == 'Where':
                if sqlbuilder.sdata.last_method not in ['From', 'Set',
                        'Update']:
                    raise InvalidOrderError('Wrong order')
                sqlbuilder.sdata.last_method = 'Where'
            elif fn.__name__ == 'And':
                if sqlbuilder.sdata.last_method not in ['Where', 'Or',
                        'And']:
                    raise InvalidOrderError('Wrong order')
                sqlbuilder.sdata.last_method = 'And'
            elif fn.__name__ == 'Or':
                if sqlbuilder.sdata.last_method not in ['Where', 'And',
                        'Or']:
                    raise InvalidOrderError('Wrong order')
                sqlbuilder.sdata.last_method = 'Or'
            elif fn.__name__  in ['InnerJoin', 'LeftJoin', 'RightJoin',
                    'OuterJoin']:
                if sqlbuilder.sdata.last_method != 'From':
                    raise InvalidOrderError('Wrong order')
                sqlbuilder.sdata.last_method = 'Join'
            elif fn.__name__ == 'On':
                if sqlbuilder.sdata.last_method != 'Join':
                    raise InvalidOrderError('Wrong order')
                sqlbuilder.sdata.last_method = 'On'
            return fn(*args, **kwargs)
        return nested

    @check_order
    def Select(self, *args):
        """
        Generates SELECT expression.

        Arguments:
            args -- columns to select
        Returns:
            self
        """
        # If select all columns
        for arg in args[:]:
            if type(arg) == list:
                args = arg
                break

        sql = 'SELECT '
        self.select_columns = [''.join((arg.table_name, '.', arg.column_name))
                for arg in args]
        sql += ', '.join(self.select_columns)
        self.sql.append(sql)
        return self

    @check_order
    def Update(self, table):
        """
        Generates UPDATE expression.

        Arguments:
            table -- table to update
        Returns:
            self
        """
        sql = 'UPDATE {0} SET '.format(table.get_name())
        self.sql.append(sql)
        return self

    @check_order
    def Insert(self, table):
        """
        Generates INSERT expression.

        Arguments:
            table -- table for inserting
        Returns:
            self
        """
        sql = 'INSERT INTO {0} '.format(table.get_name())
        self.sql.append(sql)
        return self

    @check_order
    def Delete(self):
        """
        Generates DELETE expression.

        Returns:
            self
        """
        self.sql.append('DELETE ')
        return self

    @check_order
    def From(self, *args):
        """
        Generates FROM expression.

        Arguments:
            args -- tables to fetch from
        Returns:
            self
        """
        sql = ' FROM '
        tables = [arg.get_name() for arg in args]
        self.sql.append(sql + ', '.join(tables))
        return self

    @check_order
    def Where(self, *args):
        """
        Generates WHERE expression.

        Arguments:
            condition -- condition
        Returns:
            self
        """
        sql = ''.join(args)
        self.sql.append(' WHERE ' + sql)
        return self

    @check_order
    def InnerJoin(self, table):
        """
        Generates INNER JOIN expression.

        Arguments:
            table -- table to join
        Returns:
            self
        """
        self.sql.append(' INNER JOIN ' + table.get_name())
        return self

    @check_order
    def OuterJoin(self, table):
        """
        Generates OUTER JOIN expression.

        Arguments:
            table -- table to join
        Returns:
            self
        """
        self.sql.append(' OUTER JOIN ' + table.get_name())
        return self

    @check_order
    def LeftJoin(self, table):
        """
        Generates LEFT JOIN expression.

        Arguments:
            table -- table to join
        Returns:
            self
        """
        self.sql.append(' LEFT JOIN ' + table.get_name())
        return self

    @check_order
    def RightJoin(self, table):
        """
        Generates RIGHT JOIN expression.

        Arguments:
            table -- table to join
        Returns:
            self
        """
        self.sql.append(' RIGHT JOIN ' + table.get_name())
        return self

    @check_order
    def On(self, *args):
        """
        Generates ON expression.

        Arguments:
            condition -- condition
        Returns:
            self
        """
        sql = ''.join(args)
        self.sql.append(' ON ' + sql)
        return self

    @check_order
    def And(self, *args):
        """
        Generates AND expression.

        Arguments:
            condition -- condition
        Returns:
            self
        """
        sql = ''.join(args)
        self.sql.append(' AND ' + sql)
        return self

    @check_order
    def Or(self, *args):
        """
        Generates OR expression.

        Arguments:
            condition -- condition
        Returns:
            self
        """
        sql = ''.join(args)
        self.sql.append(' OR ' + sql)
        return self

    @check_order
    def Columns(self, *args):
        """
        Generates list of columns to insert into.
        I.e. -(..., ...)

        Arguments:
            args -- columns
        Returns:
            self
        """
        sql = ', '.join([i.column_name for i in args])
        sql = ''.join((' (', sql, ') '))
        self.sql.append(sql)
        return self

    @check_order
    def Values(self, *args):
        """
        Generates VALUES(..., ...) expression.

        Arguments:
            args -- values
        Returns:
            self
        """
        args = list(args)
        for i in range(len(args)):
            if isinstance(args[i], basestring):
                args[i] = "'{0}'".format(args[i])

        sql = ', '.join([str(i) for i in args])
        sql = 'VALUES (' + sql + ')'
        self.sql.append(sql)
        return self

    @check_order
    def Set(self, *args):
        """
        Generates setting new values in UPDATE expression.

        Arguments:
            args -- new values
        Returns:
            self
        """
        sql = ', '.join(args)
        self.sql.append(sql)
        return self

    def LeftBracket(self, *args):
        """
        Generates left bracket with condition in it.

        Returns:
            self
        """
        sql = ''.join(args)
        self.sql.append(' (')
        self.sql.append(sql)
        return self

    def RightBracket(self):
        """
        Generates right bracket.

        Returns:
            self
        """
        self.sql.append(') ')
        return self

    @check_order
    def CreateTable(self, table):
        """
        Generates CREATE TABLE name (...) expression.

        Arguments:
            table -- subclass of Table
        Returns:
            self
        """
        sql = ''.join(('CREATE TABLE ', table.get_name(), ' ('))
        columns = [column.create() for column in table.all]
        sql += ', '.join(columns)
        sql += ')'
        self.sql.append(sql)
        return self

    @check_order
    def DropTable(self, table):
        """
        Generates DROP TABLE name expression.

        Arguments:
            table -- subclass of Table
        Returns:
            self
        """
        self.sql.append('DROP TABLE IF EXISTS ' + table.get_name())
        return self

    def Execute(self, db):
        """
        Executes expression without fetching the results.

        Arguments:
            db -- db
        """
        db.db.Fetch(self, commit=True)

    def FetchFrom(self, db):
        """
        Executes expression with fetching the results.

        Arguments:
            db -- db to fetch from
        Returns:
            fetched data
        """
        return db.db.Fetch(self, result=True)

    def FetchConstructed(self, db, data):
        """
        Fetches new data with constrcuted sql but new params.

        Arguments:
            db -- db to fetch from
            data -- new data
        Returns:
            fetched data
        """
        self.sdata.data = data
        return db.db.Fetch(self, result=True)
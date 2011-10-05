#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Unit tests."""

import sql
from sql import InvalidOrderError, InvalidTypeError
import unittest

__author__ = "Gennadiy Zlobin"
__email__ = "gennad.zlobin@gmail.com"
__status__ = "Production"
__version__ = "1.0.0"


class TestSql(unittest.TestCase):
    """Test case for testing sql module."""

    def setUp(self):
        self.query = sql.SqlBuilder()
        self.db_type = 'sqlite'
        self.db = sql.Db(self.db_type)
        self.drop_table()
        self.create_table()
        self.add_temp_data()

    def drop_table(self):
        """Drops all tables."""
        self.query.DropTable(self.db.Users)
        self.query.Execute(self.db)

        self.query.DropTable(self.db.Managers)
        self.query.Execute(self.db)

    def create_table(self):
        """Creates all tables."""
        self.query.CreateTable(self.db.Users)
        self.query.Execute(self.db)

        self.query.CreateTable(self.db.Managers)
        self.query.Execute(self.db)

    def add_temp_data(self):
        """Adds temp data."""
        self.query.Insert(self.db.Users).Columns(
                self.db.Users.id, self.db.Users.login,
                self.db.Users.last_login_time).Values(
                1, 'Greg', '2010-01-01')
        self.query.Execute(self.db)

        self.query.Insert(self.db.Users).Columns(self.db.Users.id,
                self.db.Users.login, self.db.Users.last_login_time,
                self.db.Users.flag, self.db.Users.position).Values(
                2, 'Mike', '2014-01-01', 'A', 5)
        self.query.Execute(self.db)

        self.query.Insert(self.db.Users).Columns(self.db.Users.id,
                self.db.Users.login, self.db.Users.last_login_time,
                self.db.Users.flag, self.db.Users.class_field,
                self.db.Users.position).Values(3, 'Alex', '1999-01-01', 'B',
                'm', 5)
        self.query.Execute(self.db)

        self.query.Insert(self.db.Users).Columns(self.db.Users.id,
                self.db.Users.login, self.db.Users.last_login_time,
                self.db.Users.flag, self.db.Users.class_field,
                self.db.Users.position).Values(4, 'admin', '2010-01-01', 'B',
                'm', 5)
        self.query.Execute(self.db)

        self.query.Insert(self.db.Managers).Columns(self.db.Managers.id,
                self.db.Managers.photo).Values(1, 'photo.jpg')
        self.query.Execute(self.db)

    def test_update(self):
        """Tests updating."""
        self.query.Update(self.db.Users).Set(self.db.Users.login == 'Mark').Where(
                self.db.Users.id == 2)
        self.query.Execute(self.db)

        self.query.Select(self.db.Users.login).From(self.db.Users).Where(
                self.db.Users.id == 2)
        rows = self.query.FetchFrom(self.db)
        self.assertTrue(len(rows) == 1)
        row = rows[0]
        self.assertTrue(row.login == 'Mark')

    def test_delete(self):
        """Tests deleting."""
        self.query = sql.SqlBuilder()

        self.query.Select(self.db.Users.all).From(self.db.Users)
        rows = self.query.FetchFrom(self.db)
        self.assertTrue(len(rows) == 4)

        self.query.Delete().From(self.db.Users).Where(self.db.Users.id == 2)
        self.query.Execute(self.db)

        self.query.Select(self.db.Users.all).From(self.db.Users)
        rows = self.query.FetchFrom(self.db)
        self.assertTrue(len(rows) == 3)

        # Now delete, num of rows should be decreased
        self.query.Select(self.db.Users.id).From(self.db.Users).Where(
                self.db.Users.id < 2)
        rows = self.query.FetchFrom(self.db)
        self.assertTrue(len(rows) == 1)

    def test_GetUsersMapping(self):
        """Tests main function for retrieving persons."""
        since = '2012-01-01'

        self.query.Select(self.db.Users.id, self.db.Users.login).From(
                self.db.Users).Where(
                self.db.Users.last_login_time < since).And(
                self.db.Users.login != 'admin')
        rows = self.query.FetchFrom(self.db)

        result = dict((row.id, row.login) for row in rows)
        self.assertEquals(result, {1: u'Greg', 3: u'Alex'})

    def test_complex_query(self):
        """Tests complex query."""
        self.query.Select(self.db.Users.all).From(self.db.Users).Where(
                ).LeftBracket( self.db.Users.flag == 'A').Or().LeftBracket(
                self.db.Users.flag == 'B').And(
                self.db.Users.class_field == 'm').RightBracket().RightBracket(
                ).And().LeftBracket(self.db.Users.position < 10).RightBracket()
        rows = self.query.FetchFrom(self.db)
        self.assertTrue(len(rows) == 3)

    def test_all_fields(self):
        """Tests all fields retriving."""
        self.query.Select(self.db.Users.all).From(self.db.Users)
        rows = self.query.FetchFrom(self.db)
        row = rows[0]
        self.assertTrue(hasattr(row, 'class_field'))
        self.assertTrue(hasattr(row, 'flag'))
        self.assertTrue(hasattr(row, 'id'))
        self.assertTrue(hasattr(row, 'last_login_time'))
        self.assertTrue(hasattr(row, 'login'))
        self.assertTrue(hasattr(row, 'position'))

    def test_joins_fields(self):
        """Tests tables joining."""
        self.query.Select(self.db.Users.id, self.db.Managers.photo).From(
                self.db.Users).InnerJoin( self.db.Managers).On(
                self.db.Users.id == self.db.Managers.id)
        rows = self.query.FetchFrom(self.db)
        self.assertTrue(len(rows) == 1)
        row = rows[0]
        self.assertTrue(hasattr(row, 'photo'))

    def test_in(self):
        """Tests working with sets."""
        self.query.Select(self.db.Users.id).From(self.db.Users).Where(
                self.db.Users.id.In([1, 2]))
        rows = self.query.FetchFrom(self.db)
        self.assertTrue(len(rows) == 2)

        # Tests NOT IN
        self.query.Select(self.db.Users.id).From(self.db.Users).Where(
                self.db.Users.id.NotIn([1, 2, 3]))
        rows = self.query.FetchFrom(self.db)
        self.assertTrue(len(rows) == 1)

    def test_new_params(self):
        """Tests working with constructed query."""
        self.query.Select(self.db.Users.id).From(self.db.Users).Where(
                self.db.Users.id < 2)
        rows = self.query.FetchFrom(self.db)
        self.assertTrue(len(rows) == 1)

        params = [4]
        rows = self.query.FetchConstructed(self.db, params)
        self.assertTrue(len(rows) == 3)

    def test_type_checking(self):
        """Tests checking type."""
        self.query.Select(self.db.Users.id).From(self.db.Users).Where(
                self.db.Users.id < 2)
        rows = self.query.FetchFrom(self.db)
        self.assertTrue(len(rows) == 1)

        with self.assertRaises(InvalidTypeError):
            self.query.Select(self.db.Users.id).From(self.db.Users).Where(
                    self.db.Users.login < 2)

    def test_operations_checking(self):
        """Tests checking order of the operations."""
        with self.assertRaises(InvalidOrderError):
            self.query.Select(self.db.Users.all).And(
                    self.db.Users.id == 1).Where(
                        self.db.Users.login == 'admin')

    def test_sql_injection(self):
        """Tests protection from SQL injection."""
        self.query.Select(self.db.Users.all).From(self.db.Users).Where(
                self.db.Users.login == 'Greg')
        rows = self.query.FetchFrom(self.db)
        self.assertTrue(len(rows) == 1)

        params = ['Alex']
        rows = self.query.FetchConstructed(self.db, params)
        self.assertTrue(len(rows) == 1)

        self.query.Select(self.db.Users.all).From(self.db.Users).Where(
            self.db.Users.login == '"Alex" or 1=1')
        rows = self.query.FetchFrom(self.db)
        self.assertTrue(len(rows) == 0)

        params = ['"Alex" or 1=1']
        rows = self.query.FetchConstructed(self.db, params)
        self.assertTrue(len(rows) == 0)

        params = ['\'Alex\' or 1=1']
        rows = self.query.FetchConstructed(self.db, params)
        self.assertTrue(len(rows) == 0)


if __name__ == '__main__':
    unittest.main()
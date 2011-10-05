#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Module for usage demonstration of sql module."""
import sql

__author__ = "Gennadiy Zlobin"
__email__ = "gennad.zlobin@gmail.com"
__status__ = "Production"
__version__ = "1.0.0"

def GetUsersMapping(since):
    """Gets users from database."""
    db_type = 'sqlite'
    db = sql.Db(db_type)

    query = sql.SqlBuilder()
    query.Select(db.Users.id, db.Users.login).From(db.Users).Where(
            db.Users.last_login_time < since).And(db.Users.login != 'admin')

    rows = query.FetchFrom(db)
    result = dict((row.id, row.login) for row in rows)
    print result

if __name__ == '__main__':
    since = '2012-01-01'
    GetUsersMapping(since)
import sqlite3
from nacl import pwhash, secret, utils
import hashlib
import os
import common
import logging
from authenticationdatabase import AuthenticationDatabase

db = AuthenticationDatabase()
db.openDataBase()

print('Usermanagement')
choice = ''
while choice != 'x':
    choice = input ('Add user(a);Delete user(d); clear database(c), exit(x) ')
    if choice.casefold() == 'a':
        name = input("new user: ")
        pwd = input("password: ")
        db.create_user(name,pwd)
        if db.login(name, pwd):
            print('user added')
    if choice.casefold() == 'd':
        name = input("delete user: ")
        db.deleteUser(name)
    if choice.casefold() == 'c':
        db.clearDatabase()
    if choice.casefold() == 'x':
        break


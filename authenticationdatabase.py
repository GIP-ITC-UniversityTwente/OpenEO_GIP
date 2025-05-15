import sqlite3
from nacl import pwhash, secret, utils
import hashlib
import os
import openeologging
import logging
from datetime import datetime
import threading

class AuthenticationDatabase:
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(AuthenticationDatabase, cls).__new__(cls)
        return cls.instance
    
    def __init__(self):
        self.openDataBase()
        self.authLock = threading.RLock()

    def openDataBase(self):
        openeologging.logMessage(logging.INFO,'open database')
        self.dbConnection = None
        try:
            path = './config/database.db'
            alreadyExists =  os.path.isfile(path)
            self.dbConnection = sqlite3.connect(path,check_same_thread=False)
            if not alreadyExists:
                sql_create_users_table = """ CREATE TABLE IF NOT EXISTS users (
                                        username text TEXT PRIMARY KEY,
                                        password_hash text 
                                    ); """
                self.dbConnection(sql_create_users_table)
                sql_create_token_table = """ CREATE TABLE IF NOT EXISTS tokens (
                                        token text TEXT PRIMARY KEY,
                                        username TEXT,
                                        endtime TEXT 
                                    ); """                
                self.dbConnection.execute(sql_create_token_table)
        except Exception as e:
            print(e)

    def close(self):
        openeologging.logMessage(logging.INFO,'close database')
        if  self.dbConnection:
            self.dbConnection.close()

    def decrypt_password(self, encrypted_password, key):
        """Decrypts the encrypted password using PyNaCl."""
        box = secret.SecretBox(key)
        decrypted_password = box.decrypt(encrypted_password)
        return decrypted_password.decode()

    def hash_password(self, password):
        """Hashes the password using a secure hash function."""
        salt = os.urandom(32)
        key = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, 100000)
        return salt + key
    
    def create_user(self, username, password):
        try:
            with self.authLock:
                cursor = self.dbConnection.cursor()
                hashed_password = self.hash_password(password)
                cursor.execute("INSERT INTO users (username, password_hash) VALUES (?, ?)", (username, hashed_password))
                #self.dbConnection.commit()
        except Exception as err:
            print(err)            

    def verify_password(self, stored_password, provided_password):
        openeologging.logMessage(logging.INFO,'verifying password')
        """Verifies if the provided password matches the stored hashed password."""
        salt = stored_password[:32]
        stored_key = stored_password[32:]
        provided_key = hashlib.pbkdf2_hmac('sha256', provided_password.encode('utf-8'), salt, 100000)
        return stored_key == provided_key

    def login(self, username, password):
        openeologging.logMessage(logging.INFO,'login for user ' + username + ' logged in', username)
        with self.authLock:
            cursor = self.dbConnection.cursor()
            cursor.execute("SELECT password_hash FROM users WHERE username = ?", (username,))
            stored_password = cursor.fetchone()
        if stored_password is not None:
            if self.verify_password(stored_password[0], password):
                openeologging.logMessage(logging.INFO,'user ' + username + ' logged in', username)
                return True
            else:
                openeologging.logMessage(logging.INFO,'user ' + username + ' logged in with incorrect password', username)
                return False
        else:
            openeologging.logMessage(logging.INFO,'user ' + username + ' not found', username)
        return False  

    def addToken(self, token, username, endTime):
        openeologging.logMessage(logging.INFO,'token ' + token + ' added for user ' + username, username)
        with self.authLock:
            query = """Select username from tokens where token= ?"""
            cursor = self.dbConnection.execute(query,(token,))
            if cursor.rowcount > 0:
                return
            self.dbConnection.execute("INSERT INTO tokens (token, username, endtime) VALUES (?, ?,?)", (token, username, endTime))
            query = """Select * from tokens"""
            cursor = self.dbConnection.execute(query)
            dd = cursor.fetchall()
        return dd
        

    def tokenExpired(self, token):
        with self.authLock:
            query = """Select endtime from tokens where token= ?"""
            cursor = self.dbConnection.execute(query,(token,))
            data = cursor.fetchone()
            if data != None:
                endtime = data[0]
                endTime = datetime.strptime(endtime, '%Y-%m-%d %H:%M:%S')
                timeNow = datetime.now()
                delta = endTime - timeNow
                return delta.days < 1
        
        return False
   
    def getUserFromToken(self, token):
        openeologging.logMessage(logging.INFO,'get user from token ' + token)
        user = '?'
        with self.authLock:        
            query = """Select username from tokens where token= ?"""
            cursor = self.dbConnection.execute(query,(token,))
            data = cursor.fetchone()
            user = data[0]
            openeologging.logMessage(logging.INFO,'token ' + token + ' used for user ' + user, user)
          
        return user

    def deleteTokens(self):
        with self.authLock:
            query = """delete from tokens"""
            self.dbConnection.execute(query)

    def deleteUser(self, name):
        with self.authLock:
            query = """delete from users where username= ?"""
            self.dbConnection.execute(query,(name))
            query = """delete from tokens where username= ?"""
            self.dbConnection.execute(query,(name))

    def clearOutOfDateTokes(self):
        with self.authLock:
            query = "delete from tokens where JULIANDAY(endtime) - JULIANDAY('now') < 0"
            self.dbConnection.execute(query)

    def clearDatabase(self):
        openeologging.logMessage(logging.INFO,'clear database')
        with self.authLock:        
            query = """delete from users"""
            self.dbConnection.execute(query)
            query = """delete from tokens"""
            self.dbConnection.execute(query)

authenticationDB = AuthenticationDatabase()



        


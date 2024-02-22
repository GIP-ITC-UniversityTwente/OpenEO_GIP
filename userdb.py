import sqlite3
from nacl import pwhash, secret, utils
import hashlib
import os

class UserDatabase:
    def openDataBase(self):
        self.dbConnection = None
        try:
            path = './config/database.db'
            alreadyExists =  os.path.isfile(path)
            self.dbConnection = sqlite3.connect(path)
            if not alreadyExists:
                sql_create_users_table = """ CREATE TABLE IF NOT EXISTS users (
                                        username text TEXT PRIMARY KEY,
                                        password_hash text 
                                    ); """
                c = self.dbConnection.cursor()
                c.execute(sql_create_users_table)

            print(sqlite3.version)
        except Exception as e:
            print(e)
    
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
            cursor = self.dbConnection.cursor()
            hashed_password = self.hash_password(password)
            cursor.execute("INSERT INTO users (username, password_hash) VALUES (?, ?)", (username, hashed_password))
            self.dbConnection.commit()
        except Exception as err:
            print(err)            

    def verify_password(self, stored_password, provided_password):
        """Verifies if the provided password matches the stored hashed password."""
        salt = stored_password[:32]
        stored_key = stored_password[32:]
        provided_key = hashlib.pbkdf2_hmac('sha256', provided_password.encode('utf-8'), salt, 100000)
        return stored_key == provided_key

    def login(self, username, password):
        cursor = self.dbConnection.cursor()
        cursor.execute("SELECT password_hash FROM users WHERE username = ?", (username,))
        stored_password = cursor.fetchone()
        if stored_password is not None:
            if self.verify_password(stored_password[0], password):
                print("Login successful")
                return True
            else:
                print("Incorrect password")
                return False
        else:
            print("User not found")
        return False             

"""
db = UserDatabase()
db.openDataBase()

print('Usermanagement')
choice = input ('Add user(a); ')
if choice.casefold() == 'a':
    name = input("new user: ")
    pwd = input("password: ")
    db.create_user(name,pwd)
    db.login(name, pwd)
"""    
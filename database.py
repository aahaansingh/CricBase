import mysql.connector
import sqlite3
import os
from CricBase import data_read
import pandas as pd
from pandas.io import sql

class CricDB :
    """An object for initializing a cricket database"""

    def __init__ (self, dbpath) :
        """
        Initializes a connection to the database; if it doesn't exist, creates one at the
        specified path

        Arguments:
            dbpath: the (desired) location of the database
        """
        self.connection = sqlite3.connect(dbpath)

    def create_db(self, path: str) :
        """
        Creates the database at the specified location
        """
        cursor = self.connection.cursor()
        db = data_read.DataRead(path)

        # to_sql doesn't allow you to specify keys; this shouldn't matter for normal
        # database operations but may try to implement primary/foreign keys for 
        # completion's sake
        db.match.to_sql("match", self.connection, if_exists='replace')
        db.player.to_sql("player", self.connection, if_exists='replace')
        db.player_match.to_sql("player_match", self.connection, if_exists='replace')
        db.delivery.to_sql("delivery", self.connection, if_exists='replace')
        db.wickets.to_sql("wickets", self.connection, if_exists='replace')
        db.extras.to_sql("extras", self.connection, if_exists='replace')
        db.fielder_wickets.to_sql("fielder_wickets", self.connection, if_exists='replace')
        cursor.close()

    def get_cursor(self) :
        return self.connection.cursor()

    def close(self) :
        self.connection.close()
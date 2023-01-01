"""
Prints the version of the database when connected to the QIR_DTCASE_LINK connection string.
"""

import os

import psycopg2

with psycopg2.connect(os.getenv("QIR_DTCASE_LINK")) as link:
    with link.cursor() as hand:
        hand.execute("SELECT version()")
        db_version = hand.fetchone()
        print(db_version)

"""
Prints the version of the database when connected to the QIR_DTCASE_LINK connection string.
"""

import os

import psycopg2

address = os.getenv("QIR_DTCASE_LINK")
print("Address", address)

with psycopg2.connect(address) as link:
    with link.cursor() as hand:
        hand.execute("SELECT version()")
        db_version = hand.fetchone()
        print(db_version)

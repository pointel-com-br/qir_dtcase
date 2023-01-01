"""
Updates the price on two tables
"""

import os
from dataclasses import dataclass
from decimal import Decimal


import psycopg2


@dataclass
class PriceTableChange:
    destiny: str
    origin: str
    multiplier: Decimal


changes: list[PriceTableChange] = []
changes.append(PriceTableChange("NORRS", "NORINT", Decimal(1.05)))
changes.append(PriceTableChange("SENRS", "SENINT", Decimal(1.05)))


address = os.getenv("QIR_DTCASE_LINK")


def execute():
    with psycopg2.connect(address) as link:
        with link.cursor() as hand:
            for change in changes:
                hand.execute(
                    f"SELECT produto, valor FROM precos WHERE tabela = '{change.origin}'")
                rows = hand.fetchall()
                multiplied = []
                for row in rows:
                    multiplied.append((row[0], row[1] * change.multiplier))
                hand.execute(
                    f"DELETE FROM precos WHERE tabela = '{change.destiny}'")
                for item in multiplied:
                    produto = item[0]
                    valor = "{:.2f}".format(item[1])
                    hand.execute(
                        f"INSERT INTO precos (tabela, produto, valor) VALUES ('{change.destiny}', '{produto}', {valor})")
                    print("Done", change.destiny, produto)


if __name__ == '__main__':
    execute()

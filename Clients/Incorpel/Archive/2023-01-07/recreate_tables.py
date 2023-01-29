"""
Recreates all tables from the origin tables
"""

import os
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from itertools import product

import psycopg2

address = os.getenv("QIR_DTCASE_LINK")


class KindProduct(Enum):
    BOTH = 1
    FABRICATION = 2
    RESALE = 3


@dataclass
class PriceTableChange:
    destiny: str
    origin: str
    multiplier: Decimal
    addition: Decimal
    kindProduct: KindProduct


tableChanges: list[PriceTableChange] = []


tableChanges.append(PriceTableChange("SENINT", "NORINT",
                    Decimal(0.95), None, KindProduct.BOTH))

tableChanges.append(PriceTableChange("NORILH", "NORINT",
                    Decimal(1.01), None, KindProduct.BOTH))
tableChanges.append(PriceTableChange("SENILH", "SENINT",
                    Decimal(1.01), None, KindProduct.BOTH))

tableChanges.append(PriceTableChange("NORFX", "NORINT",
                    Decimal(1.015), None, KindProduct.FABRICATION))
tableChanges.append(PriceTableChange("NORFX", "NORINT",
                    Decimal(0.985), None, KindProduct.RESALE))
tableChanges.append(PriceTableChange("SENFX", "SENINT",
                    Decimal(1.015), None, KindProduct.FABRICATION))
tableChanges.append(PriceTableChange("SENFX", "SENINT",
                    Decimal(0.985), None, KindProduct.RESALE))

tableChanges.append(PriceTableChange("NOREXT", "NORINT",
                    Decimal(1.02), None, KindProduct.BOTH))
tableChanges.append(PriceTableChange("SENEXT", "SENINT",
                    Decimal(1.02), None, KindProduct.BOTH))

tableChanges.append(PriceTableChange("NORLAG", "NORINT",
                    Decimal(1.03), None, KindProduct.BOTH))
tableChanges.append(PriceTableChange("SENLAG", "SENINT",
                    Decimal(1.03), None, KindProduct.BOTH))

tableChanges.append(PriceTableChange("NORRS", "NORINT",
                    Decimal(1.05), None, KindProduct.BOTH))
tableChanges.append(PriceTableChange("SENRS", "SENINT",
                    Decimal(1.05), None, KindProduct.BOTH))

tableChanges.append(PriceTableChange("NORITA", "NORILH",
                    None, Decimal(0.01), KindProduct.BOTH))
tableChanges.append(PriceTableChange("SENITA", "SENILH",
                    None, Decimal(0.01), KindProduct.BOTH))


def getProductsAndValues(hand, origin: str, kindProduct: KindProduct) -> list[tuple[str, Decimal]]:
    select = "SELECT precos.produto, precos.valor FROM precos"
    if (kindProduct != KindProduct.BOTH):
        select += " JOIN produtos ON produtos.codigo = precos.produto"
    select += f" WHERE precos.tabela = '{origin}'"
    if (kindProduct == KindProduct.FABRICATION):
        select += f" AND produtos.origem = 'F'"
    elif (kindProduct == KindProduct.RESALE):
        select += f" AND produtos.origem = 'R'"
    hand.execute(select)
    rows = hand.fetchall()
    results = []
    for row in rows:
        results.append((row[0], row[1]))
    return results


def executeTableChanges():
    with psycopg2.connect(address) as link:
        with link.cursor() as hand:
            for change in tableChanges:
                filtered = getProductsAndValues(
                    hand, change.origin, change.kindProduct)
                for product, value in filtered:
                    modified = value
                    if change.multiplier != None:
                        modified = modified * change.multiplier
                    if change.addition != None:
                        modified = modified + change.addition
                    modified = "{:.2f}".format(modified)
                    hand.execute(
                        f"DELETE FROM precos WHERE produto = '{product}' AND tabela = '{change.destiny}'")
                    affected = hand.rowcount
                    if affected > 0:
                        hand.execute(
                            f"INSERT INTO precos (tabela, produto, valor) VALUES ('{change.destiny}', '{product}', {modified})")
                    print("Done product", product, "on table", change.destiny)


if __name__ == '__main__':
    executeTableChanges()
    print("Finished updating all tables")

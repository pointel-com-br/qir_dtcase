"""
Updates the pries in several group and recreates the others tables from the origin
"""

import os
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from itertools import product

import psycopg2


address = os.getenv("QIR_DTCASE_LINK")


@dataclass
class PriceGroupChange:
    table: str
    group: str
    subgroup: str
    multiplier: Decimal


groupChanges: list[PriceGroupChange] = []


groupChanges.append(PriceGroupChange("NORINT", "01", "050", Decimal(1.04)))
groupChanges.append(PriceGroupChange("NORINT", "01", "063", Decimal(1.04)))
groupChanges.append(PriceGroupChange("NORINT", "02", "003", Decimal(1.04)))
groupChanges.append(PriceGroupChange("NORINT", "03", "010", Decimal(1.04)))
groupChanges.append(PriceGroupChange("NORINT", "03", "004", Decimal(1.04)))

groupChanges.append(PriceGroupChange("NORINT", "01", "005", Decimal(1.05)))
groupChanges.append(PriceGroupChange("NORINT", "01", "012", Decimal(1.05)))
groupChanges.append(PriceGroupChange("NORINT", "01", "025", Decimal(1.05)))
groupChanges.append(PriceGroupChange("NORINT", "01", "052", Decimal(1.05)))
groupChanges.append(PriceGroupChange("NORINT", "01", "061", Decimal(1.05)))
groupChanges.append(PriceGroupChange("NORINT", "01", "065", Decimal(1.05)))
groupChanges.append(PriceGroupChange("NORINT", "01", "086", Decimal(1.05)))
groupChanges.append(PriceGroupChange("NORINT", "02", "001", Decimal(1.05)))
groupChanges.append(PriceGroupChange("NORINT", "02", "081", Decimal(1.05)))
groupChanges.append(PriceGroupChange("NORINT", "03", "007", Decimal(1.05)))

groupChanges.append(PriceGroupChange("NORINT", "01", "029", Decimal(1.06)))
groupChanges.append(PriceGroupChange("NORINT", "01", "044", Decimal(1.06)))
groupChanges.append(PriceGroupChange("NORINT", "01", "045", Decimal(1.06)))
groupChanges.append(PriceGroupChange("NORINT", "01", "082", Decimal(1.06)))
groupChanges.append(PriceGroupChange("NORINT", "01", "083", Decimal(1.06)))
groupChanges.append(PriceGroupChange("NORINT", "02", "002", Decimal(1.06)))
groupChanges.append(PriceGroupChange("NORINT", "03", "002", Decimal(1.06)))


def executeGroupChanges():
    with psycopg2.connect(address) as link:
        with link.cursor() as hand:
            for groupChange in groupChanges:
                select = "SELECT precos.produto, precos.valor FROM precos"
                select += " JOIN produtos ON produtos.codigo = precos.produto"
                select += f" WHERE precos.tabela = '{groupChange.table}'"
                select += f" AND produtos.grupo = '{groupChange.group}'"
                select += f" AND produtos.subgrupo = '{groupChange.subgroup}'"
                hand.execute(select)
                rows = hand.fetchall()
                changing = []
                for row in rows:
                    changing.append((row[0], row[1] * groupChange.multiplier))
                for produto, valor in changing:
                    valor = "{:.2f}".format(valor)
                    hand.execute(
                        f"DELETE FROM precos WHERE produto = '{produto}' AND tabela = '{groupChange.table}'")
                    hand.execute(
                        f"INSERT INTO precos (tabela, produto, valor) VALUES ('{groupChange.table}', '{produto}', {valor})")
                    print("Done", groupChange.table, produto)


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
                    hand.execute(
                        f"INSERT INTO precos (tabela, produto, valor) VALUES ('{change.destiny}', '{product}', {modified})")
                    print("Done product", product, "on table", change.destiny)


if __name__ == '__main__':
    executeGroupChanges()
    executeTableChanges()

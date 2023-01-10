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


def updateGroups():
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
    print("Finished to update all groups")


if __name__ == '__main__':
    updateGroups()

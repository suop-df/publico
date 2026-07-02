# -*- coding: utf-8 -*-
"""Regenera apenas o data/gz/poupanca_corrente.json.gz com o build atual."""
import sys
sys.path.insert(0, "E:/Projetos/publico")
from etl import (init_oracle, fetch, read_sql, build_poupanca_corrente_data,
                 save_poupanca_corrente_gz,
                 DB_USER, DB_PASSWORD, DB_DSN, DB_MIN, DB_MAX, DB_INC)

oracledb = init_oracle()
pool = oracledb.create_pool(user=DB_USER, password=DB_PASSWORD, dsn=DB_DSN,
                            min=DB_MIN, max=DB_MAX, increment=DB_INC)
with pool.acquire() as conn:
    with conn.cursor() as cur:
        rows = fetch(cur, read_sql("poupanca_corrente.sql"))
pool.close()

D = build_poupanca_corrente_data(rows)
save_poupanca_corrente_gz(D)
print("OK: gz regenerado")

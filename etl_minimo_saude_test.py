"""Script temporário: extrai e gera apenas minimo_saude.json.gz (sem Supabase)."""
import sys
sys.path.insert(0, '.')
from etl import (
    init_oracle, read_sql, fetch, build_minimo_saude_data,
    save_minimo_saude_gz, DB_USER, DB_PASSWORD, DB_DSN,
    CLIENT_PATH, DB_MIN, DB_MAX, DB_INC, log
)

def run():
    oracledb = init_oracle()
    if not DB_USER or not DB_PASSWORD:
        raise ValueError("DB_USER e DB_PASSWORD precisam estar definidos no .env")
    log.info(f"Conectando ao Oracle -> {DB_DSN}")
    pool = oracledb.create_pool(
        user=DB_USER, password=DB_PASSWORD, dsn=DB_DSN,
        min=DB_MIN, max=DB_MAX, increment=DB_INC,
    )
    with pool.acquire() as conn:
        with conn.cursor() as cur:
            log.info("Extraindo -> minimo_saude.json")
            sql = read_sql("minimo_saude.sql")
            data = fetch(cur, sql)
            log.info(f"  {len(data)} linhas retornadas do Oracle")
            D_obj = build_minimo_saude_data(data)
            save_minimo_saude_gz(D_obj)
    pool.close()
    log.info("Concluido.")

if __name__ == "__main__":
    run()

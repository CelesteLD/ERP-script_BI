import csv
import os
import re
import sys
import pathlib
import requests
import yaml
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from datetime import datetime
from dotenv import load_dotenv

ROOT = pathlib.Path(__file__).resolve().parent
DATA_DIR = ROOT / "data" / "raw"   

def snake_case(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^\w]+", "_", s, flags=re.UNICODE)
    s = re.sub(r"_{2,}", "_", s).strip("_")
    if re.match(r"^\d", s):
        s = f"c_{s}"  # prefijo si empieza por número
    return s or "columna"

def ensure_db_exists(pg_conn_info: dict):
    admin_dsn = (
        f"host={pg_conn_info['host']} port={pg_conn_info['port']}"
        f" user={pg_conn_info['user']} password={pg_conn_info['password']} dbname=postgres"
    )
    conn = psycopg2.connect(admin_dsn)
    try:
        conn.autocommit = True  # <- clave: fuera de transacción
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (pg_conn_info['dbname'],))
            exists = cur.fetchone() is not None
            if not exists:
                cur.execute(f"CREATE DATABASE {pg_conn_info['dbname']} ENCODING 'UTF8' TEMPLATE template0;")
                print(f"[OK] Creada base de datos {pg_conn_info['dbname']}")
    finally:
        conn.close()


def connect_db(pg_conn_info: dict):
    dsn = f"host={pg_conn_info['host']} port={pg_conn_info['port']} user={pg_conn_info['user']} password={pg_conn_info['password']} dbname={pg_conn_info['dbname']}"
    return psycopg2.connect(dsn)

def prepare_ingest_log(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS erp_ingest_log (
            id SERIAL PRIMARY KEY,
            dataset_id TEXT,
            title TEXT,
            filename TEXT,
            table_name TEXT,
            fetched_at TIMESTAMP WITH TIME ZONE,
            rowcount BIGINT
        );
    """)

def recreate_table_text(cur, table_name: str, headers: list[str]):
    cols = ", ".join([f'"{h}" TEXT' for h in headers])
    cur.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE;')
    cur.execute(f'CREATE TABLE "{table_name}" ({cols});')

def copy_csv(conn, cur, table_name: str, csv_path: pathlib.Path):
    with open(csv_path, "r", encoding="utf-8") as f:
        # COPY con STDIN
        cur.copy_expert(sql=f'COPY "{table_name}" FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER \',\')', file=f)
    # contar filas
    cur.execute(f'SELECT COUNT(*) FROM "{table_name}";')
    return cur.fetchone()[0]

def main():
    load_dotenv()
    pg = {
        "host": os.getenv("PG_HOST", "localhost"),
        "port": int(os.getenv("PG_PORT", "5432")),
        "user": os.getenv("PG_USER", "postgres"),
        "password": os.getenv("PG_PASSWORD", ""),
        "dbname": os.getenv("PG_DATABASE", "bd_erp"),
    }

    # Crear BD si no existe
    ensure_db_exists(pg)

    # Leer config
    cfg_path = ROOT / "datasets.yaml"
    with open(cfg_path, "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)
    datasets = cfg.get("datasets", [])
    if not datasets:
        print("No hay datasets en datasets.yaml", file=sys.stderr)
        sys.exit(1)

    # Carpeta destino fechada
    today = datetime.now().strftime("%Y-%m-%d")
    out_dir = DATA_DIR / today
    out_dir.mkdir(parents=True, exist_ok=True)

    with connect_db(pg) as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            prepare_ingest_log(cur)
            conn.commit()

        for ds in datasets:
            url = ds["url"].strip()
            filename = ds["filename"].strip()
            table = ds["table"].strip()
            title = ds.get("title", "")
            dsid = ds.get("id", "")

            print(f"\n[DESCARGA] {dsid} → {filename}")
            r = requests.get(url, timeout=120)
            r.raise_for_status()
            csv_path = out_dir / filename
            csv_path.write_bytes(r.content)

            # Leer cabeceras del CSV tal como vienen y normalizarlas
            with open(csv_path, "r", encoding="utf-8", newline="") as fh:
                reader = csv.reader(fh)
                headers_raw = next(reader)
            headers = [snake_case(h) for h in headers_raw]

            # Crear tabla TEXT y cargar CSV
            with connect_db(pg) as conn2:
                with conn2.cursor() as cur2:
                    recreate_table_text(cur2, table, headers)
                    rowcount = copy_csv(conn2, cur2, table, csv_path)
                    cur2.execute("""
                        INSERT INTO erp_ingest_log (dataset_id, title, filename, table_name, fetched_at, rowcount)
                        VALUES (%s, %s, %s, %s, NOW(), %s)
                    """, (dsid, title, str(csv_path), table, rowcount))
                conn2.commit()

            print(f"[OK] {table}: {rowcount} filas")

    print("\n[FIN] Descarga y carga en bd_ERP completadas.")

if __name__ == "__main__":
    main()

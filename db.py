import psycopg2
import psycopg2.extras
from config import DB_DSN

def get_connection():
    return psycopg2.connect(DB_DSN)

def execute_query(sql, params=None, fetchall=True):
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    if params:
        cursor.execute(sql, params)
    else:
        cursor.execute(sql)
    result = cursor.fetchall() if fetchall else cursor.fetchone()
    result = [dict(r) for r in result] if fetchall else (dict(result) if result else None)
    cursor.close()
    conn.close()
    return result

def get_table_counts():
    tables = [
        'empresas', 'estabelecimentos', 'socios', 'cnaes',
        'motivos', 'municipios', 'natureza', 'qualificacoes', 'paises', 'simples'
    ]
    counts = {}
    conn = get_connection()
    cursor = conn.cursor()
    for t in tables:
        try:
            cursor.execute(f'SELECT COUNT(*) FROM "{t}"')
            counts[t] = cursor.fetchone()[0]
        except Exception:
            conn.rollback()
            counts[t] = 0
    cursor.close()
    conn.close()
    return counts

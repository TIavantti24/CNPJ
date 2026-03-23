from flask import Blueprint, render_template, jsonify, request, Response
import requests
from requests.auth import HTTPBasicAuth
import xml.etree.ElementTree as ET
import os, zipfile, threading, time, queue, re
import pandas as pd
import psycopg2
from config import (DOWNLOAD_DIR, DTYPES, TABLE_MAP,
                    CSV_SEP, CSV_DEC, CSV_QUOTE, CHUNK_SIZE, DB_CONFIG)

updater_bp = Blueprint('updater', __name__)
_jobs: dict = {}

# ── Nextcloud WebDAV ──────────────────────────────────────────────────────────
NC_BASE    = 'https://arquivos.receitafederal.gov.br'
NC_TOKEN   = 'gn672Ad4CF8N6TK'
NC_WEBDAV  = f'{NC_BASE}/public.php/webdav'
NC_AUTH    = HTTPBasicAuth(NC_TOKEN, '')
NC_HEADERS = {'User-Agent': 'Mozilla/5.0', 'Depth': '1'}
NC_CNPJ    = '/Dados/Cadastros/CNPJ'

DAV_NS = 'DAV:'

def _push(q, msg, kind='log'):
    q.put({'type': kind, 'msg': msg})

def _webdav_list(path):
    """Lista arquivos/pastas via WebDAV PROPFIND."""
    url = f"{NC_WEBDAV}{path}"
    body = '''<?xml version="1.0"?>
    <d:propfind xmlns:d="DAV:">
      <d:prop>
        <d:displayname/>
        <d:getcontentlength/>
        <d:resourcetype/>
      </d:prop>
    </d:propfind>'''
    r = requests.request('PROPFIND', url, auth=NC_AUTH,
                         headers=NC_HEADERS, data=body, timeout=30)
    r.raise_for_status()

    root = ET.fromstring(r.content)
    items = []
    for response in root.findall(f'{{{DAV_NS}}}response'):
        href = response.find(f'{{{DAV_NS}}}href').text
        name = href.rstrip('/').split('/')[-1]
        if not name or name == path.split('/')[-1]:
            continue  # pula o próprio diretório

        resourcetype = response.find(f'.//{{{DAV_NS}}}resourcetype')
        is_dir = resourcetype is not None and resourcetype.find(f'{{{DAV_NS}}}collection') is not None

        size_el = response.find(f'.//{{{DAV_NS}}}getcontentlength')
        size = int(size_el.text) if size_el is not None and size_el.text else 0

        items.append({'name': name, 'is_dir': is_dir, 'size': size, 'href': href})
    return items

def _rf_list_months():
    items = _webdav_list(NC_CNPJ)
    return sorted([i['name'] for i in items
                   if i['is_dir'] and re.match(r'^\d{4}-\d{2}$', i['name'])])

def _rf_list_files(month):
    items = _webdav_list(f'{NC_CNPJ}/{month}')
    return [
        {
            'name': i['name'],
            'size': i['size'],
            'download_url': f"{NC_WEBDAV}{NC_CNPJ}/{month}/{i['name']}"
        }
        for i in items if not i['is_dir'] and i['name'].lower().endswith('.zip')
    ]

@updater_bp.route('/')
def updater_page():
    return render_template('updater.html')

@updater_bp.route('/months')
def list_months():
    try:
        return jsonify({'ok': True, 'months': _rf_list_months()})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})

@updater_bp.route('/files/<month>')
def list_files(month):
    try:
        return jsonify({'ok': True, 'files': _rf_list_files(month)})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})

@updater_bp.route('/start', methods=['POST'])
def start_job():
    data = request.json
    job_id = f"job_{int(time.time())}"
    q = queue.Queue()
    _jobs[job_id] = q
    threading.Thread(target=_run_job,
                     args=(job_id, data.get('month'), data.get('selected_files', []), q),
                     daemon=True).start()
    return jsonify({'ok': True, 'job_id': job_id})

@updater_bp.route('/progress/<job_id>')
def progress(job_id):
    q = _jobs.get(job_id)
    if not q:
        return Response('data: {"type":"error","msg":"Job not found"}\n\n',
                        content_type='text/event-stream')
    def generate():
        import json
        while True:
            try:
                item = q.get(timeout=60)
                yield f"data: {json.dumps(item)}\n\n"
                if item.get('type') == 'done':
                    break
            except queue.Empty:
                yield 'data: {"type":"ping"}\n\n'
    return Response(generate(), content_type='text/event-stream',
                    headers={'X-Accel-Buffering': 'no', 'Cache-Control': 'no-cache'})

def _run_job(job_id, month, selected_files_input, q):
    try:
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        _push(q, f"🚀 Iniciando job para remessa {month}")

        # selected_files_input is a list of {name, download_url}
        selected_files = []
        for f in selected_files_input:
            name_clean = re.sub(r'\d+$', '', f['name'].lower().replace('.zip', ''))
            prefix = None
            for p in TABLE_MAP.keys():
                if name_clean == p.lower() or f['name'].lower().startswith(p.lower()):
                    prefix = p
                    break
            if prefix:
                selected_files.append((f, prefix))
            else:
                _push(q, f"⚠️ Prefixo não reconhecido para {f['name']}", 'error')

        _push(q, f"🎯 {len(selected_files)} arquivo(s) selecionado(s)")

        for idx, (file_info, prefix) in enumerate(selected_files, 1):
            fname = file_info['name']
            fsize_mb = file_info.get('size', 0) / (1024*1024) if file_info.get('size', 0) else 0
            _push(q, f"⬇️  [{idx}/{len(selected_files)}] Baixando {fname} ({fsize_mb:.1f} MB)...", 'progress')
            local_path = os.path.join(DOWNLOAD_DIR, fname)
            try:
                _download_file(file_info['download_url'], local_path, q)
            except Exception as e:
                _push(q, f"❌ Erro ao baixar {fname}: {e}", 'error')
                continue
            _push(q, f"📥 Importando {fname} → tabela `{TABLE_MAP[prefix]}`...")
            # Verificar se é realmente um ZIP
            if not zipfile.is_zipfile(local_path):
                with open(local_path, 'rb') as f:
                    preview = f.read(200)
                _push(q, f"❌ {fname} não é um ZIP válido. Preview: {preview[:100]}", 'error')
                continue
            try:
                _import_zip(local_path, prefix, q)
                _push(q, f"✅ {fname} importado!", 'success')
            except Exception as e:
                _push(q, f"❌ Erro ao importar {fname}: {e}", 'error')

        _push(q, "🎉 Job concluído!", 'done')
    except Exception as e:
        _push(q, f"💥 Erro fatal: {e}", 'error')
        _push(q, 'Abortado.', 'done')

def _download_file(url, dest, q, retries=3):
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, stream=True, timeout=(30, 900), auth=NC_AUTH,
                             headers={'User-Agent': 'Mozilla/5.0'})
            r.raise_for_status()
            content_type = r.headers.get('content-type', '')
            if 'html' in content_type:
                raise Exception(f"Servidor retornou HTML em vez do ZIP.")
            total = int(r.headers.get('content-length', 0))
            downloaded = 0
            with open(dest, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024*1024):
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total:
                        _push(q, str(int(downloaded*100/total)), 'download_pct')
            return
        except Exception as e:
            if attempt < retries:
                _push(q, f"   ⚠️ Tentativa {attempt} falhou: {e}. Aguardando 10s...", 'error')
                time.sleep(10)
            else:
                raise

def _import_zip(zip_path, prefix, q):
    table = TABLE_MAP[prefix]
    dtype_dict = DTYPES.get(prefix, {})
    columns = list(dtype_dict.keys())
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cols_escaped = ', '.join([f'"{c}"' for c in columns])
    placeholders = ', '.join(['%s'] * len(columns))
    sql = f'INSERT INTO "{table}" ({cols_escaped}) VALUES ({placeholders}) ON CONFLICT DO NOTHING'
    total_rows = 0
    with zipfile.ZipFile(zip_path, 'r') as zf:
        for inner_file in zf.namelist():
            _push(q, f"   📄 Processando {inner_file}...")
            with zf.open(inner_file) as f:
                try:
                    for chunk in pd.read_csv(
                        f, sep=CSV_SEP, decimal=CSV_DEC, quotechar=CSV_QUOTE,
                        dtype=str, encoding='latin1', header=None,
                        names=columns, chunksize=CHUNK_SIZE, low_memory=False
                    ):
                        chunk = chunk.where(pd.notnull(chunk), None)
                        chunk = chunk.iloc[:, :len(columns)]
                        chunk.columns = columns
                        rows = [tuple(r) for _, r in chunk.iterrows()]
                        if rows:
                            cursor.executemany(sql, rows)
                            conn.commit()
                            total_rows += len(rows)
                            _push(q, f"   ✔ {total_rows:,} linhas inseridas", 'row_count')
                except Exception as e:
                    _push(q, f"   ⚠️ Erro em {inner_file}: {e}", 'error')
                    conn.rollback()
    cursor.close()
    conn.close()
    _push(q, f"   📊 Total: {total_rows:,} linhas na tabela `{table}`")

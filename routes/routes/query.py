from flask import Blueprint, render_template, request, jsonify
from db import execute_query

query_bp = Blueprint('query', __name__)

# ── Predefined useful queries ─────────────────────────────────────────────────
SAVED_QUERIES = {
    'empresas_ativas': {
        'label': '🏢 Empresas Ativas por Estado',
        'description': 'Lista empresas ativas com razão social, CNPJ e localização.',
        'sql': """
            SELECT
                CONCAT(e.cnpj_basico, est.cnpj_ordem, est.cnpj_dv) AS cnpj_completo,
                e.razao_social,
                COALESCE(est.nome_fantasia, '—') AS nome_fantasia,
                est.uf,
                m.nome_municipio AS municipio,
                est.situacao_cadastral,
                est.data_de_inicio_da_atividade,
                n.descricao_natureza_juridica AS natureza_juridica,
                CASE e.porte_da_empresa
                    WHEN '01' THEN 'Micro Empresa'
                    WHEN '03' THEN 'Pequeno Porte'
                    WHEN '05' THEN 'Demais'
                    ELSE e.porte_da_empresa
                END AS porte
            FROM empresas e
            JOIN estabelecimentos est ON e.cnpj_basico = est.cnpj_basico
            LEFT JOIN municipios m ON est.municipio = m.codigo_municipio
            LEFT JOIN natureza n ON e.natureza_juridica = n.codigo_natureza_juridica
            WHERE est.situacao_cadastral = '02'
            LIMIT 500
        """
    },
    'socios_empresa': {
        'label': '👥 Sócios por Empresa',
        'description': 'Lista sócios com nome, qualificação e data de entrada.',
        'sql': """
            SELECT
                s.cnpj_basico,
                e.razao_social,
                s.nome_do_socio,
                s.cnpj_ou_cpf_do_socio,
                q.descricao_qualificacao AS qualificacao,
                s.data_de_entrada_sociedade,
                CASE s.faixa_etaria
                    WHEN '1' THEN '0-12 anos'
                    WHEN '2' THEN '13-20 anos'
                    WHEN '3' THEN '21-30 anos'
                    WHEN '4' THEN '31-40 anos'
                    WHEN '5' THEN '41-50 anos'
                    WHEN '6' THEN '51-60 anos'
                    WHEN '7' THEN '61-70 anos'
                    WHEN '8' THEN '71-80 anos'
                    WHEN '9' THEN '80+ anos'
                    ELSE '—'
                END AS faixa_etaria
            FROM socios s
            LEFT JOIN empresas e ON s.cnpj_basico = e.cnpj_basico
            LEFT JOIN qualificacoes q ON s.qualificacao_do_socio = q.codigo_qualificacao
            ORDER BY e.razao_social
            LIMIT 500
        """
    },
    'cnae_atividade': {
        'label': '🏭 Empresas por CNAE / Atividade',
        'description': 'Quantidade de empresas por atividade econômica principal.',
        'sql': """
            SELECT
                est.cnae_fiscal_principal AS codigo_cnae,
                c.descricao_cnae,
                COUNT(*) AS total_empresas,
                SUM(CASE WHEN est.situacao_cadastral = '02' THEN 1 ELSE 0 END) AS ativas
            FROM estabelecimentos est
            LEFT JOIN cnaes c ON est.cnae_fiscal_principal = c.codigo_cnae
            GROUP BY est.cnae_fiscal_principal, c.descricao_cnae
            ORDER BY total_empresas DESC
            LIMIT 200
        """
    },
    'simples_nacional': {
        'label': '📋 Optantes do Simples Nacional',
        'description': 'Empresas optantes pelo Simples com dados cadastrais.',
        'sql': """
            SELECT
                s.cnpj_basico,
                e.razao_social,
                s.opcao_pelo_simples,
                s.data_opcao_simples,
                s.data_exclusao_simples,
                CASE e.porte_da_empresa
                    WHEN '01' THEN 'Micro Empresa'
                    WHEN '03' THEN 'Pequeno Porte'
                    ELSE 'Outros'
                END AS porte
            FROM simples s
            LEFT JOIN empresas e ON s.cnpj_basico = e.cnpj_basico
            WHERE s.opcao_pelo_simples = 'S'
            ORDER BY s.data_opcao_simples DESC
            LIMIT 500
        """
    },
    'capital_social': {
        'label': '💰 Empresas por Capital Social',
        'description': 'Ranking de empresas com maior capital social declarado.',
        'sql': """
            SELECT
                e.cnpj_basico,
                e.razao_social,
                CAST(REPLACE(REPLACE(e.capital_social, '.', ''), ',', '.') AS DECIMAL(20,2)) AS capital_social,
                n.descricao_natureza_juridica AS natureza,
                CASE e.porte_da_empresa
                    WHEN '01' THEN 'Micro Empresa'
                    WHEN '03' THEN 'Pequeno Porte'
                    WHEN '05' THEN 'Demais'
                    ELSE e.porte_da_empresa
                END AS porte,
                est.uf
            FROM empresas e
            LEFT JOIN natureza n ON e.natureza_juridica = n.codigo_natureza_juridica
            LEFT JOIN estabelecimentos est ON e.cnpj_basico = est.cnpj_basico
            ORDER BY capital_social DESC
            LIMIT 200
        """
    },
    'contatos': {
        'label': '📞 Empresas com Contato (Email/Telefone)',
        'description': 'Empresas ativas com e-mail e telefone cadastrado.',
        'sql': """
            SELECT
                CONCAT(est.cnpj_basico, est.cnpj_ordem, est.cnpj_dv) AS cnpj_completo,
                e.razao_social,
                est.nome_fantasia,
                CONCAT(est.ddd1, ' ', est.telefone1) AS telefone_1,
                CONCAT(est.ddd2, ' ', est.telefone2) AS telefone_2,
                est.correio_eletronico AS email,
                est.uf,
                m.nome_municipio AS cidade
            FROM estabelecimentos est
            JOIN empresas e ON est.cnpj_basico = e.cnpj_basico
            LEFT JOIN municipios m ON est.municipio = m.codigo_municipio
            WHERE est.situacao_cadastral = '02'
              AND (est.correio_eletronico IS NOT NULL AND est.correio_eletronico != '')
            ORDER BY e.razao_social
            LIMIT 500
        """
    },
    'estatisticas_uf': {
        'label': '🗺️ Estatísticas por UF',
        'description': 'Total de empresas ativas e inativas por estado.',
        'sql': """
            SELECT
                est.uf,
                COUNT(*) AS total,
                SUM(CASE WHEN est.situacao_cadastral = '02' THEN 1 ELSE 0 END) AS ativas,
                SUM(CASE WHEN est.situacao_cadastral != '02' THEN 1 ELSE 0 END) AS inativas
            FROM estabelecimentos est
            WHERE est.uf IS NOT NULL AND est.uf != ''
            GROUP BY est.uf
            ORDER BY total DESC
        """
    },
    'historico_abertura': {
        'label': '📅 Histórico de Abertura de Empresas',
        'description': 'Quantidade de empresas abertas por ano.',
        'sql': """
            SELECT
                LEFT(est.data_de_inicio_da_atividade, 4) AS ano,
                COUNT(*) AS empresas_abertas
            FROM estabelecimentos est
            WHERE est.data_de_inicio_da_atividade IS NOT NULL
              AND est.data_de_inicio_da_atividade != '0'
              AND LENGTH(est.data_de_inicio_da_atividade) >= 4
            GROUP BY ano
            ORDER BY ano DESC
            LIMIT 50
        """
    },
    'consulta_completa': {
        'label': '🔍 Consulta Completa com Filtros',
        'description': 'Dados completos de empresas, estabelecimentos e sócios com filtros avançados.',
        'modal_filters': True,
        'sql': """
            SELECT {DISTINCT_CNPJ}
                CONCAT(e.cnpj_basico, est.cnpj_ordem, est.cnpj_dv) AS cnpj_completo,
                e.cnpj_basico,
                est.cnpj_ordem,
                est.cnpj_dv,
                e.razao_social,
                n.descricao_natureza_juridica,
                q_resp.descricao_qualificacao AS qualificacao_do_responsavel,
                e.capital_social,
                CASE e.porte_da_empresa
                    WHEN '01' THEN 'Micro Empresa'
                    WHEN '03' THEN 'Pequeno Porte'
                    WHEN '05' THEN 'Demais'
                    ELSE e.porte_da_empresa
                END AS porte_da_empresa,
                CASE e.porte_da_empresa
                    WHEN '01' THEN 'ME'
                    WHEN '03' THEN 'EPP'
                    WHEN '05' THEN 'Demais'
                    ELSE 'Não Informado'
                END AS tipo_empresa,
                e.ente_federativo_resposavel,
                est.identificador_matriz_filial,
                COALESCE(est.nome_fantasia, '') AS nome_fantasia,
                est.situacao_cadastral,
                est.data_situacao_cadastral,
                mot.descricao_motivo,
                est.nome_da_cidade_no_exterior,
                p_est.nome_pais AS pais_est,
                est.data_de_inicio_da_atividade,
                est.cnae_fiscal_principal AS codigo_cnae_principal,
                cp.descricao_cnae AS cnae_principal,
                est.cnae_fiscal_secundaria AS codigo_cnae_secundario,
                cs.descricao_cnae AS cnae_secundario,
                est.tipo_de_logradouro,
                est.logradouro,
                est.numero,
                est.complemento,
                est.bairro,
                est.cep,
                est.uf,
                m.nome_municipio,
                CASE
                    WHEN est.ddd1 IS NOT NULL AND est.ddd1 != '' AND est.telefone1 IS NOT NULL AND est.telefone1 != ''
                    THEN CONCAT('(', est.ddd1, ') ', est.telefone1)
                    ELSE ''
                END AS telefone1_completo,
                CASE
                    WHEN est.telefone1 LIKE '9%%' AND LENGTH(est.telefone1) = 9 THEN 'Celular'
                    WHEN LENGTH(est.telefone1) = 8 THEN 'Fixo'
                    ELSE 'Outro'
                END AS tipo_telefone1,
                CASE
                    WHEN est.ddd2 IS NOT NULL AND est.ddd2 != '' AND est.telefone2 IS NOT NULL AND est.telefone2 != ''
                    THEN CONCAT('(', est.ddd2, ') ', est.telefone2)
                    ELSE ''
                END AS telefone2_completo,
                est.correio_eletronico,
                est.situacao_especial,
                est.data_da_situacao_especial,
                s.identificador_de_socio,
                s.nome_do_socio,
                s.cnpj_ou_cpf_do_socio,
                q_soc.descricao_qualificacao AS qualificacao_do_socio,
                s.data_de_entrada_sociedade,
                p_soc.nome_pais AS pais_socio,
                s.representante_legal,
                s.nome_do_representante,
                q_rep.descricao_qualificacao AS qualificacao_do_representante_legal,
                s.faixa_etaria
            FROM empresas e
            JOIN estabelecimentos est ON e.cnpj_basico = est.cnpj_basico
            LEFT JOIN municipios m ON est.municipio = m.codigo_municipio
            LEFT JOIN natureza n ON e.natureza_juridica = n.codigo_natureza_juridica
            LEFT JOIN qualificacoes q_resp ON e.qualificacao_do_responsavel = q_resp.codigo_qualificacao
            LEFT JOIN socios s ON e.cnpj_basico = s.cnpj_basico
            LEFT JOIN qualificacoes q_soc ON s.qualificacao_do_socio = q_soc.codigo_qualificacao
            LEFT JOIN qualificacoes q_rep ON s.qualificacao_do_representante_legal = q_rep.codigo_qualificacao
            LEFT JOIN paises p_est ON est.pais = p_est.codigo_pais
            LEFT JOIN paises p_soc ON s.pais = p_soc.codigo_pais
            LEFT JOIN motivos mot ON est.motivo_situacao_cadastral = mot.codigo_motivo
            LEFT JOIN cnaes cp ON est.cnae_fiscal_principal = cp.codigo_cnae
            LEFT JOIN cnaes cs ON est.cnae_fiscal_secundaria = cs.codigo_cnae
            WHERE est.situacao_cadastral = '02'
            {WHERE_EXTRA}
            {ORDER_CLAUSE}
            {LIMIT_CLAUSE}
        """
    }
}

# ── Routes ────────────────────────────────────────────────────────────────────

@query_bp.route('/')
def query_page():
    return render_template('query.html', saved_queries=SAVED_QUERIES)

@query_bp.route('/run', methods=['POST'])
def run_query():
    data = request.json
    sql = data.get('sql', '').strip()
    if not sql:
        return jsonify({'ok': False, 'error': 'SQL vazio'})

    # Block destructive statements
    first_word = sql.split()[0].upper()
    if first_word not in ('SELECT', 'SHOW', 'DESCRIBE', 'EXPLAIN'):
        return jsonify({'ok': False, 'error': 'Apenas consultas SELECT são permitidas.'})

    try:
        rows = execute_query(sql)
        columns = list(rows[0].keys()) if rows else []
        return jsonify({'ok': True, 'columns': columns, 'rows': rows, 'total': len(rows)})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})

@query_bp.route('/saved/<key>', methods=['GET'])
def run_saved(key):
    q = SAVED_QUERIES.get(key)
    if not q:
        return jsonify({'ok': False, 'error': 'Query não encontrada'})
    try:
        rows = execute_query(q['sql'])
        columns = list(rows[0].keys()) if rows else []
        return jsonify({'ok': True, 'columns': columns, 'rows': rows,
                        'total': len(rows), 'label': q['label']})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})


@query_bp.route('/filtered', methods=['POST'])
def run_filtered():
    """Executa a consulta completa com filtros dinâmicos do modal."""
    data = request.json or {}
    q = SAVED_QUERIES.get('consulta_completa')
    if not q:
        return jsonify({'ok': False, 'error': 'Query não encontrada'})

    filters = []
    params  = []

    # Filtro: UF (estado)
    ufs = [u.strip().upper() for u in data.get('ufs', []) if u.strip()]
    if ufs:
        placeholders = ', '.join(['%s'] * len(ufs))
        filters.append(f"est.uf IN ({placeholders})")
        params.extend(ufs)

    # Filtro: período de abertura
    data_ini = data.get('data_inicio', '').strip()
    data_fim = data.get('data_fim', '').strip()
    if data_ini:
        filters.append("est.data_de_inicio_da_atividade >= %s")
        params.append(data_ini.replace('-', ''))
    if data_fim:
        filters.append("est.data_de_inicio_da_atividade <= %s")
        params.append(data_fim.replace('-', ''))

    # Filtro: apenas LTDA
    if data.get('apenas_ltda'):
        filters.append("UPPER(e.razao_social) LIKE %s")
        params.append('%LTDA%')

    # Filtro: tipo de telefone
    tipo_tel = data.get('tipo_telefone', '').strip()
    if tipo_tel == 'Celular':
        # Celular: começa com 9 (RF armazena sem o nono dígito, então 8 chars começando com 9 = celular)
        filters.append("est.telefone1 LIKE '9%%'")
    elif tipo_tel == 'Fixo':
        # Fixo: NÃO começa com 9
        filters.append("est.telefone1 NOT LIKE '9%%' AND est.telefone1 IS NOT NULL AND est.telefone1 != ''")

    # Filtro: apenas ativas (situacao_cadastral = '02' já está no WHERE base,
    # mas aqui garantimos também que telefone e cnpj não sejam nulos)
    if data.get('apenas_ativas'):
        filters.append("est.situacao_cadastral = '02'")

    # Filtro: sem telefone vazio/nulo
    if data.get('sem_tel_vazio') or data.get('sem_tel_duplicado'):
        filters.append("est.telefone1 IS NOT NULL AND est.telefone1 != ''")
        filters.append("est.ddd1 IS NOT NULL AND est.ddd1 != ''")

    where_extra = ''
    if filters:
        where_extra = 'AND ' + ' AND '.join(filters)

    # DISTINCT por CNPJ
    sem_cnpj_dup = data.get('sem_cnpj_duplicado', False)
    sem_tel_dup = data.get('sem_tel_duplicado', False)

    if sem_tel_dup:
        # DISTINCT ON telefone: cada número aparece só uma vez
        distinct_clause = 'DISTINCT ON (est.ddd1, est.telefone1)'
        order_clause = 'ORDER BY est.ddd1, est.telefone1, e.razao_social'
    elif sem_cnpj_dup:
        # DISTINCT ON CNPJ: cada empresa aparece só uma vez
        distinct_clause = 'DISTINCT ON (e.cnpj_basico, est.cnpj_ordem, est.cnpj_dv)'
        order_clause = 'ORDER BY e.cnpj_basico, est.cnpj_ordem, est.cnpj_dv, e.razao_social'
    else:
        distinct_clause = ''
        order_clause = 'ORDER BY e.razao_social'

    # Limite
    sem_limite = data.get('sem_limite', False)
    if sem_limite:
        limit_clause = ''
    else:
        limit = min(int(data.get('limit', 500)), 10000)
        limit_clause = f'LIMIT {limit}'

    sql_raw = (q['sql']
               .replace('{DISTINCT_CNPJ}', distinct_clause)
               .replace('{WHERE_EXTRA}', where_extra)
               .replace('{ORDER_CLAUSE}', order_clause)
               .replace('{LIMIT_CLAUSE}', limit_clause))

    try:
        rows = execute_query(sql_raw, params if params else None)
        columns = list(rows[0].keys()) if rows else []
        return jsonify({'ok': True, 'columns': columns, 'rows': rows, 'total': len(rows)})
    except Exception as e:
        import traceback
        return jsonify({'ok': False, 'error': str(e), 'detail': traceback.format_exc()})

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

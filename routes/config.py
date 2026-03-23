import os

# ── Banco de dados PostgreSQL ─────────────────────────────────────────────────
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres',
    'password': '33548084',
    'database': 'cnpj'
}

# DSN string para psycopg2
DB_DSN = "host=localhost port=5432 dbname=cnpj user=postgres password=33548084"

# ── Receita Federal ───────────────────────────────────────────────────────────
RF_BASE_URL = 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj'
RF_API_BASE  = RF_BASE_URL

# ── Pastas locais ─────────────────────────────────────────────────────────────
DOWNLOAD_DIR  = r'E:\CNPJs\arquivos'
FINALIZED_DIR = r'E:\CNPJs\finalizados'

# ── Configurações CSV ─────────────────────────────────────────────────────────
CSV_SEP   = ';'
CSV_DEC   = ','
CSV_QUOTE = '"'
CSV_ENC   = 'latin1'
CHUNK_SIZE = 100_000

# ── Mapeamento de prefixos → tabelas ──────────────────────────────────────────
TABLE_MAP = {
    'empresas':        'empresas',
    'estabelecimentos':'estabelecimentos',
    'socios':          'socios',
    'cnaes':           'cnaes',
    'motivos':         'motivos',
    'municipios':      'municipios',
    'natureza':        'natureza',
    'qualificacoes':   'qualificacoes',
    'paises':          'paises',
    'simples':         'simples',
}

# ── Colunas por tabela ────────────────────────────────────────────────────────
DTYPES = {
    'empresas': {
        'cnpj_basico': 'str', 'razao_social': 'str', 'natureza_juridica': 'str',
        'qualificacao_do_responsavel': 'str', 'capital_social': 'str',
        'porte_da_empresa': 'str', 'ente_federativo_resposavel': 'str'
    },
    'estabelecimentos': {
        'cnpj_basico': 'str', 'cnpj_ordem': 'str', 'cnpj_dv': 'str',
        'identificador_matriz_filial': 'str', 'nome_fantasia': 'str',
        'situacao_cadastral': 'str', 'data_situacao_cadastral': 'str',
        'motivo_situacao_cadastral': 'str', 'nome_da_cidade_no_exterior': 'str',
        'pais': 'str', 'data_de_inicio_da_atividade': 'str',
        'cnae_fiscal_principal': 'str', 'cnae_fiscal_secundaria': 'str',
        'tipo_de_logradouro': 'str', 'logradouro': 'str', 'numero': 'str',
        'complemento': 'str', 'bairro': 'str', 'cep': 'str', 'uf': 'str',
        'municipio': 'str', 'ddd1': 'str', 'telefone1': 'str',
        'ddd2': 'str', 'telefone2': 'str', 'ddd_do_fax': 'str', 'fax': 'str',
        'correio_eletronico': 'str', 'situacao_especial': 'str',
        'data_da_situacao_especial': 'str'
    },
    'socios': {
        'cnpj_basico': 'str', 'identificador_de_socio': 'str',
        'nome_do_socio': 'str', 'cnpj_ou_cpf_do_socio': 'str',
        'qualificacao_do_socio': 'str', 'data_de_entrada_sociedade': 'str',
        'pais': 'str', 'representante_legal': 'str', 'nome_do_representante': 'str',
        'qualificacao_do_representante_legal': 'str', 'faixa_etaria': 'str'
    },
    'cnaes':        {'codigo_cnae': 'str', 'descricao_cnae': 'str'},
    'motivos':      {'codigo_motivo': 'str', 'descricao_motivo': 'str'},
    'municipios':   {'codigo_municipio': 'str', 'nome_municipio': 'str'},
    'natureza':     {'codigo_natureza_juridica': 'str', 'descricao_natureza_juridica': 'str'},
    'qualificacoes':{'codigo_qualificacao': 'str', 'descricao_qualificacao': 'str'},
    'paises':       {'codigo_pais': 'str', 'nome_pais': 'str'},
    'simples': {
        'cnpj_basico': 'str', 'opcao_pelo_simples': 'str',
        'data_opcao_simples': 'str', 'data_exclusao_simples': 'str'
    }
}

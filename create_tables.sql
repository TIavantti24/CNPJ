-- ============================================================
--  CNPJ Database - PostgreSQL
--  Execute: psql -U postgres -f create_tables.sql
-- ============================================================

CREATE DATABASE cnpj
    WITH ENCODING 'UTF8'
    LC_COLLATE='Portuguese_Brazil.1252'
    LC_CTYPE='Portuguese_Brazil.1252'
    TEMPLATE=template0;

\connect cnpj

CREATE TABLE IF NOT EXISTS empresas (
    cnpj_basico                 VARCHAR(8)   NOT NULL,
    razao_social                VARCHAR(255),
    natureza_juridica           VARCHAR(4),
    qualificacao_do_responsavel VARCHAR(2),
    capital_social              VARCHAR(20),
    porte_da_empresa            VARCHAR(2),
    ente_federativo_resposavel  VARCHAR(50),
    PRIMARY KEY (cnpj_basico)
);

CREATE TABLE IF NOT EXISTS estabelecimentos (
    cnpj_basico                 VARCHAR(8)  NOT NULL,
    cnpj_ordem                  VARCHAR(4)  NOT NULL,
    cnpj_dv                     VARCHAR(2)  NOT NULL,
    identificador_matriz_filial VARCHAR(1),
    nome_fantasia               VARCHAR(55),
    situacao_cadastral          VARCHAR(2),
    data_situacao_cadastral     VARCHAR(8),
    motivo_situacao_cadastral   VARCHAR(2),
    nome_da_cidade_no_exterior  VARCHAR(55),
    pais                        VARCHAR(3),
    data_de_inicio_da_atividade VARCHAR(8),
    cnae_fiscal_principal       VARCHAR(7),
    cnae_fiscal_secundaria      TEXT,
    tipo_de_logradouro          VARCHAR(20),
    logradouro                  VARCHAR(60),
    numero                      VARCHAR(6),
    complemento                 VARCHAR(156),
    bairro                      VARCHAR(50),
    cep                         VARCHAR(8),
    uf                          VARCHAR(2),
    municipio                   VARCHAR(4),
    ddd1                        VARCHAR(4),
    telefone1                   VARCHAR(8),
    ddd2                        VARCHAR(4),
    telefone2                   VARCHAR(8),
    ddd_do_fax                  VARCHAR(4),
    fax                         VARCHAR(8),
    correio_eletronico          VARCHAR(115),
    situacao_especial           VARCHAR(23),
    data_da_situacao_especial   VARCHAR(8),
    PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv)
);
CREATE INDEX IF NOT EXISTS idx_est_situacao ON estabelecimentos(situacao_cadastral);
CREATE INDEX IF NOT EXISTS idx_est_uf ON estabelecimentos(uf);
CREATE INDEX IF NOT EXISTS idx_est_municipio ON estabelecimentos(municipio);
CREATE INDEX IF NOT EXISTS idx_est_cnae ON estabelecimentos(cnae_fiscal_principal);

CREATE TABLE IF NOT EXISTS socios (
    cnpj_basico                         VARCHAR(8),
    identificador_de_socio              VARCHAR(1),
    nome_do_socio                       VARCHAR(150),
    cnpj_ou_cpf_do_socio                VARCHAR(14),
    qualificacao_do_socio               VARCHAR(2),
    data_de_entrada_sociedade           VARCHAR(8),
    pais                                VARCHAR(3),
    representante_legal                 VARCHAR(11),
    nome_do_representante               VARCHAR(60),
    qualificacao_do_representante_legal VARCHAR(2),
    faixa_etaria                        VARCHAR(1)
);
CREATE INDEX IF NOT EXISTS idx_socios_cnpj ON socios(cnpj_basico);

CREATE TABLE IF NOT EXISTS cnaes (
    codigo_cnae    VARCHAR(7)  NOT NULL,
    descricao_cnae VARCHAR(150),
    PRIMARY KEY (codigo_cnae)
);

CREATE TABLE IF NOT EXISTS motivos (
    codigo_motivo    VARCHAR(2)  NOT NULL,
    descricao_motivo VARCHAR(100),
    PRIMARY KEY (codigo_motivo)
);

CREATE TABLE IF NOT EXISTS municipios (
    codigo_municipio VARCHAR(4)  NOT NULL,
    nome_municipio   VARCHAR(50),
    PRIMARY KEY (codigo_municipio)
);

CREATE TABLE IF NOT EXISTS natureza (
    codigo_natureza_juridica    VARCHAR(4)  NOT NULL,
    descricao_natureza_juridica VARCHAR(100),
    PRIMARY KEY (codigo_natureza_juridica)
);

CREATE TABLE IF NOT EXISTS qualificacoes (
    codigo_qualificacao    VARCHAR(2)  NOT NULL,
    descricao_qualificacao VARCHAR(100),
    PRIMARY KEY (codigo_qualificacao)
);

CREATE TABLE IF NOT EXISTS paises (
    codigo_pais VARCHAR(3)  NOT NULL,
    nome_pais   VARCHAR(70),
    PRIMARY KEY (codigo_pais)
);

CREATE TABLE IF NOT EXISTS simples (
    cnpj_basico           VARCHAR(8)  NOT NULL,
    opcao_pelo_simples    VARCHAR(1),
    data_opcao_simples    VARCHAR(8),
    data_exclusao_simples VARCHAR(8),
    PRIMARY KEY (cnpj_basico)
);

\echo 'Banco de dados CNPJ criado com sucesso!'

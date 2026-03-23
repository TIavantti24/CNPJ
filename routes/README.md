# CNPJ ⚡ Sistema de Dados da Receita Federal

Sistema Flask para download, importação e consulta dos dados públicos do CNPJ
disponibilizados pela Receita Federal do Brasil.

---

## 🚀 Instalação

### 1. Pré-requisitos
- Python 3.10+
- MySQL 8.0+
- MySQL rodando em localhost com usuário `root`

### 2. Instalar dependências

```bash
pip install -r requirements.txt
```

### 3. Criar banco de dados e tabelas

No MySQL, execute o script:

```bash
mysql -u root -p < create_tables.sql
```

Ou abra no MySQL Workbench e execute o arquivo `create_tables.sql`.

### 4. Ajustar configurações

Edite `config.py` se necessário:
- `DB_CONFIG` — credenciais do MySQL
- `DOWNLOAD_DIR` — pasta local para os ZIPs baixados
- `FINALIZED_DIR` — pasta para CSVs processados (legado)

### 5. Iniciar o sistema

```bash
python app.py
```

Acesse: **http://localhost:5000**

---

## 📋 Funcionalidades

### Dashboard `/`
- Contagem de registros em cada tabela
- Acesso rápido às consultas mais usadas

### Atualizar Remessa `/updater/`
1. Carrega os meses disponíveis na Receita Federal
2. Você escolhe o mês desejado (ex: `2026-03`)
3. Seleciona quais tabelas quer importar
4. O sistema baixa e importa os ZIPs diretamente no MySQL
5. Log em tempo real com Server-Sent Events (SSE)

### Consultas SQL `/query/`
8 consultas prontas:
- 🏢 Empresas ativas por estado
- 👥 Sócios por empresa
- 🏭 Empresas por CNAE/Atividade
- 📋 Optantes do Simples Nacional
- 💰 Ranking por Capital Social
- 📞 Empresas com e-mail/telefone
- 🗺️ Estatísticas por UF
- 📅 Histórico de abertura por ano

Também possui editor SQL livre (apenas SELECT permitido).  
Exportação para CSV com encoding UTF-8 BOM (compatível com Excel).

---

## 🗃️ Estrutura do Projeto

```
cnpj_system/
├── app.py              # Entrada Flask
├── config.py           # Configurações centralizadas
├── db.py               # Helper de conexão MySQL
├── create_tables.sql   # Script SQL de criação
├── requirements.txt
├── routes/
│   ├── dashboard.py    # Página inicial
│   ├── updater.py      # Download + importação
│   └── query.py        # Consultas SQL
└── templates/
    ├── base.html       # Layout com sidebar
    ├── dashboard.html
    ├── updater.html
    └── query.html
```

---

## ⚙️ Notas Técnicas

- Download em streaming (não sobrecarrega memória)
- Importação em chunks de 100.000 linhas
- `INSERT IGNORE` para evitar duplicatas
- SSE para progresso em tempo real no browser
- Encoding `latin1` na leitura dos CSVs (padrão da Receita Federal)

from flask import Flask
from routes.dashboard import dashboard_bp
from routes.updater import updater_bp
from routes.query import query_bp

app = Flask(__name__)
app.secret_key = 'cnpj_system_secret_2026'

app.register_blueprint(dashboard_bp)
app.register_blueprint(updater_bp, url_prefix='/updater')
app.register_blueprint(query_bp, url_prefix='/query')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)

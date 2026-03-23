from flask import Blueprint, render_template
from db import get_table_counts

dashboard_bp = Blueprint('dashboard', __name__)

@dashboard_bp.route('/')
def index():
    try:
        counts = get_table_counts()
    except Exception:
        counts = {}
    return render_template('dashboard.html', counts=counts)

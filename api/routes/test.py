from flask import Blueprint, jsonify
from ..auth.authentication import verify_token_middleware

test_bp = Blueprint('test_bp', __name__)

admin_users_id=[1,3,5]

@test_bp.route("/test/listar", methods=["GET", "POST"])
# @verify_token_middleware
def get_catalog_summary():#(payload):
    return jsonify({
        "response": "respuesta desde ras docker service",
    })




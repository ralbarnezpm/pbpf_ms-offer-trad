from flask import Blueprint, jsonify, request
from api.auth.authentication import verify_token_middleware
from api.controllers.traditional.offer import pull_products


product_bp = Blueprint('product_bp', __name__)

@product_bp.route("/listall/<offer_id>", methods=["GET"])
# @verify_token_middleware
# def get_test(payload):
def get_test(offer_id):
    products=pull_products(offer_id)
    return jsonify(products)
from flask import Blueprint, jsonify, request
from api.auth.authentication import verify_token_middleware
from api.controllers.traditional.comment import create_promotion_comment, pull_comment_comment
from api.controllers.traditional.notification import create_notification
from api.controllers.traditional.promotion import pull_promotion_by_id

comment_bp = Blueprint('comment_bp', __name__)

@comment_bp.route("/create", methods=["POST"])
@verify_token_middleware
def create_comment_pb_route(payload):
    """ Creates new comments """
    comment_text=request.json.get("comment_text")
    promotion_id=request.json.get("promotion_id")
    promotion=pull_promotion_by_id(promotion_id)
    comment=pull_comment_comment(payload["id"], promotion.promotion_name, comment_text)
    created, err, new_comment_id=create_promotion_comment(promotion_id, payload["id"], comment, promotion.promotional_state_id)
    if created:
        notification_created, err = create_notification(2, "Comentario", new_comment_id, payload["id"])
        if not err:
            #return jsonify(pull_comment_by_id(new_comment_id).retrieve_data())
            return {"msg": "comment created successfully"}, 200
    return {"msg": "comment creation failed", "error": str(err)}, 500
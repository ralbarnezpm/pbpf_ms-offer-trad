
from api.controllers.traditional.comment import pull_comment_data
from api.controllers.traditional.notification import time_ago_from_now
from api.controllers.traditional.offer import pull_offer_data
from api.controllers.utils import datetime_format, pull_outcome_query


def pull_promotion_activity(promotion_id):
    """Pull the promotion comments"""
    query = f"""SELECT c.*, ps.phase_str, ps.state_phase_str
                FROM pb_comment c
                LEFT JOIN pb_notification n ON c.id=n.comment_id
                LEFT JOIN promotional_state ps ON c.promotional_state_id=ps.id
                WHERE c.offer_id={promotion_id} -- AND n.notification_type_id=2
                GROUP BY n.comment_id;"""
    outcome=pull_outcome_query(query)
    comments=[]
    for row in outcome:
        user, message, catalog_name, comment = pull_comment_data(row["comment_text"])
        name, last_name = user.split(" ", 1)
        comments.append({
            "comment_text": f"@{name}_{last_name} {message} {catalog_name}" if len(comment)<3 else comment,
            "name_user": name,
            "last_name_user" : last_name,
            "id_user": row["user_id"],
            "date_created": datetime_format(row["posted_at"]),
            "id_comment": row["id"],
            "id_promotion": row["promotion_id"],
            "promotional_state": {
                'id': int(row['promotional_state_id']),
                'phase_str' : row['phase_str'] if row['phase_str'] else "" ,
                'state_phase_str' : row['state_phase_str'] if row['state_phase_str'] else "" ,
            }
        })

    return comments

def pull_activity_users(offer_id):
    query = f"""SELECT CONCAT(u.name_user, ' ', u.last_name) AS username,
                    u.id AS user_id, u.photo_url, up.rol, c.posted_at
                FROM pb_comment c
                LEFT JOIN user_account u ON c.user_id=u.id
                JOIN user_permission up ON u.user_rol=up.id
                WHERE c.offer_id={offer_id}
                GROUP BY u.id;"""
    outcome=pull_outcome_query(query)
    users=[]
    for row in outcome:
        d=dict(row)
        d["ago_format"]=time_ago_from_now(d["posted_at"])
        users.append(dict(d))
    return users

def pull_promotion_details(offer_id):
    query = f"""SELECT p.promotion_name, p.distributors_name AS customer,
                    CONCAT(p.month_str, ' ', p.year_str) AS promotion_date,
                    ps.phase_str AS `phase`,
                    ps.state_phase_str AS `state`,
                    DATE_FORMAT(p.start_sellin, '%d/%m/%Y') AS start_sellin,
                    DATE_FORMAT(p.end_sellin, '%d/%m/%Y') AS end_sellin,
                    DATE_FORMAT(p.start_sellout, '%d/%m/%Y') AS start_sellout,
                    DATE_FORMAT(p.end_sellout, '%d/%m/%Y') AS end_sellout,
                    (SELECT COUNT(*) FROM pb_promotion_product WHERE promotion_id = {offer_id}) AS sku_amount
                FROM pb_promotion p
                JOIN promotional_state ps ON p.promotion_type_id = ps.id
                WHERE p.id = {offer_id};"""
    outcome=pull_outcome_query(query)
    return dict(outcome[0])

def pull_activity_versions(promotion_id):
    query = f"""SELECT pss.phase_str, plh.updated_at
                FROM pb_promotion_product_historic plh
                LEFT JOIN promotional_state pss ON plh.promotionalstate_id=pss.id
                WHERE plh.promotion_id={promotion_id}
                GROUP BY plh.promotional_state_id;"""
    outcome=pull_outcome_query(query)
    versions=[]
    for row in outcome:
        version=dict(row)
        version["updated_at"]=datetime_format(version["updated_at"])
        versions.append(version)
    return versions


def pull_offer_activity(offer_id):
    """Pull the offer activity"""
    return {
        "comments": pull_promotion_activity(offer_id),
        "users": pull_activity_users(offer_id),
        "details": pull_promotion_details(offer_id),
        "versions": [],#pull_activity_versions(offer_id),
        "offer_data": pull_offer_data(offer_id),
    }
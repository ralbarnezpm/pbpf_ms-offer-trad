from ..extensions import db
from ..routes.utils import pull_timestamp
from ..models.comment import Comment
from ..models.user import User
from sqlalchemy.exc import SQLAlchemyError
from .utils import datetime_format, pull_outcome_query

def pull_user(user_id):
    """Pulls a user from the database, given a user_id."""
    return User.query.filter_by(id=user_id).first()

def pull_update_comment(user_id, promotion_name):
    user = pull_user(user_id)
    username = f"{user.name_user} {user.last_name}"
    return f"{username.title()}:ha aplicado cambios en el catálogo:{promotion_name}.:"

def pull_created_offer_comment(user_id, promotion_name):
    user = pull_user(user_id)
    username = f"{user.name_user} {user.last_name}"
    return f'{username.title()}:ha creado una nueva oferta con nombre:"{promotion_name}".:'

def pull_discard_comment(user_id, promotion_name):
    user = pull_user(user_id)
    username = f"{user.name_user} {user.last_name}"
    return f"{username.title()}:ha descartado el catálogo:{promotion_name}.:"

def pull_approve_comment(user_id, promotion_name):
    user = pull_user(user_id)
    username = f"{user.name_user} {user.last_name}"
    return f"{username.title()}:ha aprobado el catálogo:{promotion_name}.:"

def pull_comment_comment(user_id, promotion_name, comment):
    user = pull_user(user_id)
    username = f"{user.name_user} {user.last_name}"
    return f"{username.title()}:ha comentado en el catálogo:{promotion_name}.:{comment}"

def pull_comment_state_update(user_id, promotion_name, phase_name, state_name):
    user = pull_user(user_id)
    username = f"{user.name_user} {user.last_name}"
    return f"{username.title()}:ha actualizado el catálogo :{promotion_name} a fase {phase_name}, estado {state_name}.:"

def pull_reject_comment(user_id, promotion_name):
    user = pull_user(user_id)
    username = f"{user.name_user} {user.last_name}"
    return f"{username.title()}:ha rechazado la promocion:{promotion_name}.:"

def comment_create(user_id, promotion_id, promotional_state_id, posted_at, comment_text):
    created=False
    err=None

    new_comment_pb=Comment(user_id = user_id, offer_id = promotion_id, promotional_state_id = promotional_state_id, comment_text = comment_text, posted_at = posted_at)
    try:

        db.session.add(new_comment_pb)
        db.session.commit()
        created=True
        print("new comment id:", new_comment_pb.id)
        return created, err, new_comment_pb.id
    except SQLAlchemyError as e:
        print("error trying to insert a new comment:\n", e)
        err=e
        db.session.rollback()
        return created, err, None

def pull_comment_by_id(comment_id):
    return Comment.query.filter_by(id=comment_id).first()

def parse_comment(comment_text):
    user, message, catalog_name, comment = comment_text.split(":")
    return f"{user} {message.rstrip()} {catalog_name.rstrip()} {comment}"

def pull_comment_data(comment_text):
    user, message, catalog_name, comment = [data.rstrip() for data in comment_text.split(":")]
    return user, message, catalog_name, comment

def create_promotion_comment(promotion_id, user_id, comment_text, promotional_state_id):
    """ Creates new comments pb """
    date_created = pull_timestamp() 
    if promotion_id is not None:
        print("creating comment...")
        created, err, new_comment_id = comment_create(user_id, promotion_id, promotional_state_id, date_created, comment_text) #,send_notification
        return created, err, new_comment_id
    return False, None, None

def retrieve_all_comments():
    comments = Comment.query.all()
    return [c.retrieve_data_list() for c in comments]

# def retrieve_comments_promotion(promotion_id):
#     comments = Comment.query.filter_by(id=id).first()
#     if tp is not None:
#         return jsonify(tp.retrieve_data())
#     comments = Comment.query.all()
#     return [c.retrieve_data_list() for c in comments]

def pull_promotion_comments(promotion_id):
    """Pull the promotion comments"""
    query = f"""SELECT c.*
                FROM pb_comment c
                LEFT JOIN pb_notification n ON c.id=n.comment_id
                WHERE c.promotion_id={promotion_id} AND n.notification_type_id=2
                GROUP BY n.comment_id;"""
    outcome=pull_outcome_query(query)
    comments=[]
    for row in outcome:
        user, message, catalog_name, comment = pull_comment_data(row["comment_text"])
        name, last_name = user.split(" ")
        comments.append({
            "comment_text": comment,
            "name_user": name,
            "last_name_user" : last_name,
            "id_user": row["user_id"],
            "date_created": row["posted_at"],
            "id_comment": row["id"],
            "id_promotion": row["promotion_id"],
            "promotional_state": {
                'id': int(row['promotional_state']),
            }
        })

    return comments

def pull_promotion_activity(promotion_id):
    """Pull the promotion comments"""
    query = f"""SELECT c.*, ps.phase_str, ps.state_phase_str
                FROM pb_comment c
                LEFT JOIN pb_notification n ON c.id=n.comment_id
                LEFT JOIN promotional_state ps ON c.promotional_state_id=ps.id
                WHERE c.promotion_id={promotion_id} -- AND n.notification_type_id=2
                GROUP BY n.comment_id;"""
    outcome=pull_outcome_query(query)
    comments=[]
    for row in outcome:
        user, message, catalog_name, comment = pull_comment_data(row["comment_text"])
        name, last_name = user.split(" ")
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

def pull_users_promotion_activity(promotion_id):
    query = f"""SELECT CONCAT(u.name_user, ' ', u.last_name) AS username,
                    u.id AS user_id, u.photo_url 
                FROM pb_comment c
                LEFT JOIN user_account u ON c.user_id=u.id
                WHERE c.promotion_id={promotion_id}
                GROUP BY u.id;"""
    outcome=pull_outcome_query(query)
    users=[]
    for row in outcome:
        users.append(dict(row))
    return users

def comment_delete(comment_id):
    comment = Comment.query.filter_by(id=comment_id).first()
    err = None
    if comment is not None:
        try:
            db.session.delete(comment)
            db.session.commit()
            return True, err
        except SQLAlchemyError as e:
            print(e)
            db.session.rollback()
            err=e
            return False, err
    return False, "comment not found"
        
    

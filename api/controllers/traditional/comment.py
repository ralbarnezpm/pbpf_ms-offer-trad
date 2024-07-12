from api.controllers.traditional.user import pull_user
from api.models.comment import Comment
from api.extensions import db
from sqlalchemy.exc import SQLAlchemyError
from api.routes.utils import pull_timestamp
from re import split

def pull_comment_state_update(user_id, promotion_name, phase_name, state_name):
    user = pull_user(user_id)
    username = f"{user.name_user} {user.last_name}"
    return f"{username.title()}:ha actualizado la oferta :{promotion_name} a fase {phase_name}, estado {state_name}.:"

def pull_comment_comment(user_id, promotion_name, comment):
    user = pull_user(user_id)
    username = f"{user.name_user} {user.last_name}"
    return f"{username.title()}:ha comentado en la oferta:{promotion_name}.:{comment}"

def pull_comment_data(comment_text):
    comment_splitted = split(r'(?<!\d):(?!\d)', comment_text)
    if len(comment_splitted)==3:
        try: 
            promotion, action = comment_splitted[2].split(':', 1)
        except Exception as e:
            print(e)
            promotion, action = comment_splitted[1].split(':', 1)
            
        user, comment = (comment_splitted[0], comment_splitted[-1])
        comment_splitted = [user, action, promotion, comment]

    user, message, catalog_name, comment = [data.rstrip() for data in comment_splitted]
    return user, message, catalog_name, comment

def comment_create(user_id, offer_id, promotional_state_id, posted_at, comment_text):
    created=False
    err=None

    new_comment_pb=Comment(
        user_id = user_id,
        offer_id = offer_id,
        promotional_state_id = promotional_state_id,
        comment_text = comment_text,
        posted_at = posted_at
        )
    try:

        db.session.add(new_comment_pb)
        db.session.commit()
        created=True

        return created, err, new_comment_pb.id
    except SQLAlchemyError as e:
        print("error trying to insert a new comment:\n", e)
        err=e
        db.session.rollback()
        return created, err, None

def create_promotion_comment(promotion_id, user_id, comment_text, promotional_state_id):
    """ Creates new comments pb """
    date_created = pull_timestamp() 
    if promotion_id is not None:
        print("creating comment...")
        created, err, new_comment_id = comment_create(user_id, promotion_id, promotional_state_id, date_created, comment_text) #,send_notification
        return created, err, new_comment_id
    return False, None, None
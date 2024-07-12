
from api.models.user import User


def pull_user(user_id):
    """Pulls a user from the database, given a user_id."""
    return User.query.filter_by(id=user_id).first()

def pull_user_name_last_name_list():
    """ Pulls user name and last name list """

    users=User.query.all()
    id_and_name_last_name=[(user.id, user.name_user + "_" + user.last_name) for user in users]

    return id_and_name_last_name
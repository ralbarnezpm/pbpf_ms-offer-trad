

from api.controllers.user import pull_user_name_last_name_list
from api.controllers.utils import datetime_now
from api.models.notifications import Notification
from pytz import timezone
from api.extensions import db


def create_notification(notification_type_id, notification_type, comment_id, commented_by):
    """ Creates new comment notification """
    notifications_created=0
    names_last_names = pull_user_name_last_name_list()
    err=None
    for user_id, _ in names_last_names:
        if user_id != commented_by:
            new_notification = Notification(
                notification_type_id=notification_type_id,
                comment_id=comment_id,
                receiver_id=user_id,
                subject=notification_type,
                emailed=0,
                read=0
            )
            try:
                db.session.add(new_notification)
                db.session.commit()
                notifications_created+=1

            except Exception as e:
                print("error trying to insert a new notification:\n", e)
                db.session.rollback()
                err=e

    return notifications_created, err

def time_ago_from_now(target_datetime):
    target_datetime=target_datetime.astimezone(timezone('Chile/Continental'))#.replace(tzinfo=utc)#.astimezone(timezone('Chile/Continental'))
    now = datetime_now()
    time_difference = now - target_datetime

    # print("target_datetime:", target_datetime)
    # print("now:", now)
    # print("time_difference:", time_difference.seconds)

    if time_difference.days > 0:
        if time_difference.days == 1:
            return "hace 1 dia"
        else:
            return f"hace {time_difference.days} dias"
    elif time_difference.seconds >= 3600:
        hours = time_difference.seconds // 3600
        if hours == 1:
            return "hace 1 hora"
        else:
            return f"hace {hours} horas"
    else:
        minutes = time_difference.seconds // 60
        if minutes <= 1:
            return "hace 1 minuto"
        else:
            return f"hace {minutes} minutos"
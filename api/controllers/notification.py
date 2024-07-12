# from api.controllers.traditional.catalog_details import pull_traditional_month_year_offices_promo
from ..routes.utils import pull_timestamp
from ..extensions import db
from ..models.notifications import Notification
from .user import pull_user_name_last_name_list
from datetime import datetime
from ..consts import NOTIFICATION_SUMMARY
from .comment import pull_comment_data

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
    now = datetime.now()
    time_difference = now - target_datetime

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

def pull_notification_summary(subject, catalog_name):
    return f"{NOTIFICATION_SUMMARY[subject]} {catalog_name}"

def __pull_user_notifications_filters(limit=None, promotion_id=None):
    limit_num=f" LIMIT {limit}" if limit is not None else ""
    promotion_filter=f" AND c.promotion_id={promotion_id}" if promotion_id is not None else ""
    return limit_num, promotion_filter

# def pull_user_notifications(user_id, read=0, limit=None, promotion_id=None):
#     limit_num, promotion_filter=__pull_user_notifications_filters(limit, promotion_id)
#     query = f"""SELECT n.id, n.comment_id, n.subject, c.promotion_id, c.promotional_state_id, c.comment_text, c.posted_at, -- , n.emailed, n.emailed_at, n.`read`, n.read_at
#                 	p.month_promotion_str AS promotion_month, p.year_promotion_str AS promotion_year,
#                     CASE 
#                         WHEN p.id_customer=13 THEN 'Catálogo - Tradicional' 
#                         WHEN p.id_customer<>13 AND p.id_customer IS NOT NULL then 'Catálogo - Moderno' 
#                     END AS 'tipo_promocion' -- , p.name_promotion
#                 FROM pb_notification n
#                 LEFT JOIN pb_comment c ON n.comment_id=c.id
#                 LEFT JOIN promotion p ON c.promotion_id=p.id
#                 WHERE n.receiver_id={user_id} AND n.read={read} {promotion_filter} {limit_num};"""
#     outcome=pull_outcome_query(query)
#     notifications=[]
#     for row in outcome:
#         notification={key: value for key, value in dict(row).items() if key not in ["comment_text", "posted_at"]}
#         notification["time_ago"]=time_ago_from_now(row["posted_at"])
#         notification["posted_at"]=datetime_format(row["posted_at"])
#         user, message, catalog_name, comment = pull_comment_data(row["comment_text"])
#         notification["notification"]=f"{user} {message} {catalog_name}" #pull_notification_summary(row["subject"], catalog_name) if len(comment)<2 else comment
#         if row['tipo_promocion'] == "Catálogo - Tradicional":
#             year_month_promotion, oficinas, oficinas_id, linea, year, month = pull_traditional_month_year_offices_promo(536)
#             notification["oficinas"] = oficinas
#             notification["linea"] = linea
#         notifications.append(notification)
#     return notifications

# def mark_as_read(user_id):
#     timestamp=pull_timestamp()
#     query = f"""UPDATE pb_notification
#                 SET `read`=1, read_at="{timestamp}"
#                 WHERE receiver_id={user_id};"""
#     exceuted=query_execute(query)
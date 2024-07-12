from ..extensions import db
from .notification_type import NotificationType

class Notification(db.Model):
    __tablename__ = 'notification'
    id = db.Column(db.BigInteger, primary_key = True)
    # user_id = db.Column(db.Integer, db.ForeignKey('user_account.id'), unique=True)
    # user = db.relationship("User", backref=db.backref("notifications", uselist=False))
    
    #body = db.Column(db.String(length=250))
    # comment_id = db.Column(db.Integer, db.ForeignKey('comment_pb.id'), unique=True)
    # comment = db.relationship("CommentPB", backref=db.backref("notifications", uselist=False))
    
    read = db.Column(db.Integer)
    emailed = db.Column(db.Integer)
    read_at = db.Column(db.DateTime)
    emailed_at = db.Column(db.DateTime)


    id_notification_type = db.Column(db.Integer, db.ForeignKey('notification_type.id'), unique=True)
    notification_type = db.relationship("NotificationType", backref=db.backref("notification", uselist=False)) 

    id_comment = db.Column(db.Integer, db.ForeignKey('comment_pb.id'), unique=True)
    comment = db.relationship("Comment", backref=db.backref("notification", uselist=False)) 

    id_user = db.Column(db.Integer, db.ForeignKey('user_account.id'), unique=True)
    user = db.relationship("User", backref=db.backref("notifications", uselist=False)) 

    # promotion_id = db.Column(db.Integer, db.ForeignKey('promotion.id'), unique=True)
    # promotion = db.relationship("Promotion", backref=db.backref("notifications", uselist=False)) 

    def retrieve_data(self):
        return { 
            "id": self.id, 
            # "user_id": self.user_id,
            # "comment_id": self.comment_id,
            # "comment": self.comment.retrieve_data(),
            "read": self.read,
            "emailed": self.emailed,
            "read_at": self.read_at,
            "emailed_at": self.emailed_at,
            "id_notification_type": self.id_notification_type,
            # "promotion_id": self.promotion_id
            "id_comment": self.id_comment,
            "promotion_id": self.comment.id_promotion,
            "id_user": self.id_user
        }

    # def __repr__(self) -> str:
    #     return f"""<Notification: {self.id}, {self.name_customer}, {self.description_customer}, {self.date_created}, {self.channel}, {self.active}, {self.photo_url}>"""

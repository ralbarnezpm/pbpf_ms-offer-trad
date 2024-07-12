from ..extensions import db

class CommentPB(db.Model):
    __tablename__ = 'comment_pb'
    id = db.Column(db.BigInteger, primary_key = True)
    id_user = db.Column(db.BigInteger, db.ForeignKey('user_account.id'))
    comment_user = db.relationship("User", backref=db.backref("comments", uselist=False))

    id_promotion = db.Column(db.BigInteger, db.ForeignKey('promotion.id'))
    id_promotional_state = db.Column(db.BigInteger, db.ForeignKey('promotional_state.id'))
    promotional_state = db.relationship("PromotionalState", backref=db.backref("comments", uselist=False))
    # id_promotion_line = db.Column(db.BigInteger, db.ForeignKey('promotion.id'))
    date_created = db.Column(db.DateTime)
    comment_text = db.Column(db.TEXT)
    send_notification = db.Column(db.Integer)
    datetime_send_notification = db.Column(db.DateTime)

    # notification_id = db.Column(db.BigInteger, db.ForeignKey('notification.id'))
    # notification = db.relationship("Notification", backref=db.backref("comments", uselist=False))


    def retrieve_data(self):
        return { 
            "id_comment": self.id, 
            "id_user": self.id_user,
            "name_user": self.comment_user.name_user,
            "last_name_user": self.comment_user.last_name,
            "photo_url": self.comment_user.photo_url,
            "id_promotion": self.id_promotion,
            #"id_promotional_state": self.id_promotional_state,
            "promotional_state": self.promotional_state.retrieve_data2(),
            "date_created": self.date_created,
            "comment_text": self.comment_text,
            "send_notification": self.send_notification,
            "datetime_send_notification": self.datetime_send_notification,
            # "notification_id": self.notification_id
        }

    def retrieve_with_notification_data(self):
        return { 
            "id_comment": self.id, 
            "id_user": self.id_user,
            "name_user": self.comment_user.name_user,
            "last_name_user": self.comment_user.last_name,
            "photo_url": self.comment_user.photo_url,
            "id_promotion": self.id_promotion,
            #"id_promotional_state": self.id_promotional_state,
            "promotional_state": self.promotional_state.retrieve_data2(),
            "date_created": self.date_created,
            "comment_text": self.comment_text,
            "send_notification": self.send_notification,
            "datetime_send_notification": self.datetime_send_notification,
            "notification_id": self.notification_id,
            "notification": self.notification.retrieve_data()
        }

    # def __repr__(self) -> str:
    #     return f"""<PromotionalState: {self.id}, {self.phase_str}, {self.description_phase}, {self.state_phase_str}, {self.description_state}, {self.date_created}, {self.last_updated}>"""

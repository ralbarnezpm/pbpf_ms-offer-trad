from ..extensions import db
from .notification_type import NotificationType

class Notification(db.Model):
    __tablename__ = 'pb_notification'
    id = db.Column(db.BigInteger, primary_key = True)
    notification_type_id = db.Column(db.BigInteger, db.ForeignKey('notification_type.id'))
    notification_type = db.relationship("NotificationType", backref=db.backref("notificaciones", uselist=False))
    comment_id = db.Column(db.BigInteger)
    receiver_id = db.Column(db.Integer)
    subject = db.Column(db.String)
    emailed = db.Column(db.Integer)
    emailed_at = db.Column(db.DateTime)
    read = db.Column(db.Integer)
    read_at = db.Column(db.DateTime)


    def retrieve_data(self):
        return { 
            "id": self.id, 
            "subject" : self.subject,
            "receiver_id" : self.receiver_id,
            "emailed": self.emailed,
            "emailed_at": self.emailed_at,
            "read_pm": self.read,
            "read_at": self.read_at,
        }
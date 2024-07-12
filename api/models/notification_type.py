from ..extensions import db

class NotificationType(db.Model):
    __tablename__ = 'notification_type'
    id = db.Column(db.BigInteger, primary_key = True)
    type = db.Column(db.String)

    def retrieve_data(self):
        return { 
            "id": self.id, 
            "type": self.type
        }

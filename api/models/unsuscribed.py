from ..extensions import db

class Unsuscribed(db.Model):
    __tablename__ = 'unsuscribed'
    id = db.Column(db.BigInteger, primary_key = True)
    notification_id = db.Column(db.BigInteger)
    user_id = db.Column(db.Integer)

    def retrieve_data(self):
        return { 
            "id": self.id, 
            "notification_id" : self.notification_id,
            "user_id" : self.user_id,
        }
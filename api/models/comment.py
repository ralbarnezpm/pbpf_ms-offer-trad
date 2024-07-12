from ..extensions import db

def parse_comment(comment_text):
    user, message, catalog_name, comment = comment_text.split(":")
    return comment.rstrip()

class Comment(db.Model):
    __tablename__ = 'pb_comment'
    id = db.Column(db.BigInteger, primary_key = True)
    user_id = db.Column(db.BigInteger, db.ForeignKey('user_account.id'))
    comment_user = db.relationship("User", backref=db.backref("comments", uselist=False))
    promotion_id = db.Column(db.BigInteger)
    offer_id = db.Column(db.BigInteger)
    promotional_state_id = db.Column(db.BigInteger)
    comment_text = db.Column(db.TEXT)
    posted_at = db.Column(db.DateTime)

    def retrieve_data(self):
        return {
            "comment_id": self.id,
            "user_id" : self.user_id ,
            "promotion_id" :  self.promotion_id,
            "offer_id" : self.offer_id,
            "promotion_state_id" :  self.promotional_state_id,
            "comment": parse_comment(self.comment_text)      
        }
    
    def retrieve_data_list(self):
        return { 
            "id_comment": self.id, 
            "id_user": self.user_id,
            "name_user": self.comment_user.name_user,
            "last_name_user": self.comment_user.last_name,
            "photo_url": self.comment_user.photo_url,
            "id_promotion": self.promotion_id,
            "promotional_state_id": self.promotional_state_id,
            "date_created": self.posted_at,
            "comment_text": self.comment_text,
        }
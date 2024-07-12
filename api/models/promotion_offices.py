from ..extensions import db

class PromotionOffices(db.Model):
    __tablename__ = 'promotion_offices'
    id = db.Column(db.BigInteger, primary_key = True)
    id_traditional_promotion = db.Column(db.BigInteger, db.ForeignKey('traditional_promotion.id'))
    id_office = db.Column(db.BigInteger, db.ForeignKey('office.id'))

    def retrieve_data(self):
        return { 
            "id": self.id, 
            "id_traditional_promotion": self.id_traditional_promotion,
            "id_office": self.id_office,

        }
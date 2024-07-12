from ..extensions import db

class PromorionOffices(db.Model):
    __tablename__ = 'promotion_offices'
    id = db.Column(db.Integer, primary_key = True)

    id_promotion = db.Column(db.Integer, db.ForeignKey('promotion.id'))
    promotion = db.relationship("Promotion", backref=db.backref("promotion_offices", uselist=False))

    id_office = db.Column(db.Integer, db.ForeignKey('promotion.id'))
    office = db.relationship("Office", backref=db.backref("promotion_offices", uselist=False))

    def retrieve_data(self):
        return { 
            "id": self.id, 
            "id_promotion": self.id_promotion,
            "id_office": self.id_office
        }

    # def __repr__(self) -> str:
    #     return f"""<Promo: {self.id}, {self.channel}, {self.description_channel}>"""
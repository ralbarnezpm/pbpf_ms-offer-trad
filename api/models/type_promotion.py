from enum import unique
from ..extensions import db

class TypePromotion(db.Model):
    __tablename__ = 'type_promotion'
    id = db.Column(db.Integer, primary_key = True)
    type_promotion = db.Column(db.String(length=191), unique=True)
    description = db.Column(db.TEXT)
    date_created = db.Column(db.DateTime)

    def retrieve_data(self):
        return { 
            "id": self.id, 
            "type_promotion": self.type_promotion,
            "description": self.description,
            "date_created": self.date_created,
        }

    def __repr__(self) -> str:
        return f"""<TypePromotion: {self.id}, {self.type_promotion}, {self.description}, {self.date_created}>"""

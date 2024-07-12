from ..extensions import db

class PromotionalVariable(db.Model):
    __tablename__ = 'promotional_variable'
    id = db.Column(db.BigInteger, primary_key = True)
    promotional_variable = db.Column(db.String(length=100), unique=True)
    description = db.Column(db.TEXT)
    date_created = db.Column(db.DateTime)
    last_update = db.Column(db.DateTime)

    def retrieve_data(self):
        return { 
            "id": self.id, 
            "promotional_variable": self.promotional_variable,
            "description": self.description,
            "date_created": self.date_created,
            "last_update": self.last_update
        }

    def __repr__(self) -> str:
        return f"""<PromotionalVariableValue: {self.id}, {self.promotional_variable}, {self.description}, {self.date_created}, {self.last_update}>"""
from ..extensions import db

class PromotionalVariableValue(db.Model):
    __tablename__ = 'promotional_variable_value'
    id = db.Column(db.BigInteger, primary_key = True)
    id_promotional_variable = db.Column(db.BigInteger, db.ForeignKey('promotional_variable.id'))
    variable_value = db.Column(db.String(length=100), unique=True)
    date_created = db.Column(db.DateTime)
    last_update = db.Column(db.DateTime)

    def retrieve_data(self):
        return { 
            "id": self.id, 
            "id_promotional_variable": self.id_promotional_variable,
            "variable_value": self.variable_value,
            "date_created": self.date_created,
            "last_update": self.last_update
        }

    def __repr__(self) -> str:
        return f"""<PromotionalVariableValue: {self.id}, {self.id_promotional_variable}, {self.variable_value}, {self.date_created}, {self.last_update}>"""
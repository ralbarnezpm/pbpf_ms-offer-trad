from ..extensions import db

class SubCategory(db.Model):
    __tablename__ = 'subcategory'
    id = db.Column(db.BigInteger, primary_key = True)
    id_category = db.Column(db.BigInteger, db.ForeignKey('category.id'))
    subcategory = db.Column(db.String(length=100))
    description = db.Column(db.TEXT)
    date_created = db.Column(db.DateTime)
    last_update = db.Column(db.DateTime)

    def retrieve_data(self):
        return { 
            "id": self.id, 
            "id_category": self.id_category,
            "subcategory": self.subcategory,
            "description": self.description,
            "date_created": self.date_created,
            "last_update": self.last_update
        }

    def __repr__(self) -> str:
        return f"""<SubCategory: {self.id}, {self.id_category},{self.subcategory},{self.description}, {self.date_created}, {self.last_update}>"""
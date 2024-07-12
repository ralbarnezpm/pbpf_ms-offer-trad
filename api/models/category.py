from ..extensions import db

class Category(db.Model):
    __tablename__ = 'category'
    id = db.Column(db.Integer, primary_key = True)
    category = db.Column(db.String(length=100))
    description = db.Column(db.String)
    date_created = db.Column(db.DateTime)
    last_update = db.Column(db.DateTime)

    def retrieve_data(self):
        return { 
            "id": self.id, 
            "category": self.category,
            "description": self.description,
            "date_created": self.date_created,
            "last_updated": self.last_update
        }

    def __repr__(self) -> str:
        return f"""<Category: {self.id}, {self.category}, {self.description}, {self.date_created}, {self.last_update}>"""

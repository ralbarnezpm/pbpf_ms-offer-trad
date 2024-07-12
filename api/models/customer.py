from ..extensions import db

class Customer(db.Model):
    __tablename__ = 'customer'
    id = db.Column(db.Integer, primary_key = True)
    name_customer = db.Column(db.String(length=250))
    description_customer = db.Column(db.TEXT)
    date_created = db.Column(db.DateTime)
    channel = db.Column(db.Integer)
    active = db.Column(db.SmallInteger)
    photo_url = db.Column(db.TEXT)

    # def retrieve_data(self):
    #     return { 
    #         "id": self.id, 
    #         "name_customer": self.name_customer,
    #         "description_customer": self.description_customer,
    #         "date_created": self.date_created,
    #         #"channel": self.channel,
    #         "channel": self.channel_desc.retrieve_data(),
    #         "active": self.active,
    #         "photo_url": self.photo_url
    #     }

    def __repr__(self) -> str:
        return f"""<Customer: {self.id}, {self.name_customer}, {self.description_customer}, {self.date_created}, {self.channel}, {self.active}, {self.photo_url}>"""

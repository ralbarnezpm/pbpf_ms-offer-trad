from ..extensions import db
from .office_group import OfficeGroup

class Office(db.Model):
    __tablename__ = 'office'
    id = db.Column(db.BigInteger, primary_key = True)
    office_name = db.Column(db.String(length=250))
    office_description = db.Column(db.TEXT)
    date_created = db.Column(db.DateTime)
    channel = db.Column(db.Integer)
    active = db.Column(db.SmallInteger)
    photo_url = db.Column(db.TEXT)
    office_code = db.Column(db.TEXT)
    id_office_group = db.Column(db.Integer, db.ForeignKey('office_group.id'))
    office_group = db.relationship("OfficeGroup", backref=db.backref("office", uselist=False))

    def retrieve_data(self):
        return { 
            "id": self.id, 
            "office_name": self.office_name,
            "office_description": self.office_description,
            # "date_created": self.date_created,
            "channel": self.channel,
            "active": self.active,
            "photo_url": self.photo_url,
            # "office_code": self.office_code,
            "id_office_group": self.id_office_group,
            "group": self.office_group.group_name
        }

    # def __repr__(self) -> str:
    #     return f"""<Office: {self.id}, {self.name_customer}, {self.description_customer}, {self.date_created}, {self.channel}, {self.active}, {self.photo_url}>"""

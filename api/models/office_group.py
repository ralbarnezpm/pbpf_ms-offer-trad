from ..extensions import db

class OfficeGroup(db.Model):
    __tablename__ = 'office_group'
    id = db.Column(db.BigInteger, primary_key = True)
    group_name = db.Column(db.String(length=250))

    def retrieve_data(self):
        return {
            "id": self.id,
            "group_name": self.office_name,

        }

    # def __repr__(self) -> str:
    #     return f"""<Office: {self.id}, {self.name_customer}, {self.description_customer}, {self.date_created}, {self.channel}, {self.active}, {self.photo_url}>"""

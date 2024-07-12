from enum import unique
from ..extensions import db

class PromotionalState(db.Model):
    __tablename__ = 'promotional_state'
    id = db.Column(db.BigInteger, primary_key = True)
    phase_str = db.Column(db.String(length=191), unique=True)
    description_phase = db.Column(db.TEXT)
    state_phase_str = db.Column(db.String(length=191), unique=True)
    description_state = db.Column(db.TEXT)
    date_created = db.Column(db.DateTime)
    last_update = db.Column(db.DateTime)

    def retrieve_data(self):
        return { 
            "id": self.id, 
            "phase_str": self.phase_str,
            "description_phase": self.description_phase,
            "state_phase_str": self.state_phase_str,
            "description_state": self.description_state,
            #"date_created": self.date_created,
            #"last_updated": self.last_update
        }

    def retrieve_data2(self):
        return { 
            "id": self.id, 
            "phase_str": self.phase_str,
            "state_phase_str": self.state_phase_str
        }

    def __repr__(self) -> str:
        return f"""<PromotionalState: {self.id}, {self.phase_str}, {self.description_phase}, {self.state_phase_str}, {self.description_state}, {self.date_created}, {self.last_update}>"""

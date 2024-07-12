from ..extensions import db

class ChannelProduct(db.Model):
    __tablename__ = 'channel_product'
    id = db.Column(db.BigInteger, primary_key = True)
    product_id = db.Column(db.BigInteger, db.ForeignKey('product_list.id'), unique=True)
    product = db.relationship("ProductList", backref=db.backref("channel_product", uselist=False))

    channel_id = db.Column(db.Integer, db.ForeignKey('channel.id'), unique=True)
    channel = db.relationship("Channel", backref=db.backref("channel_product", uselist=False))

    def retrieve_data(self):
        return { 
            "id": self.id,
            "product": self.product,
            "channel": self.channel,
        }

    def __repr__(self) -> str:
        return f"""<ChannelProduct: {self.id}, {self.product_id}, {self.channel_id}>"""

from ..extensions import db
from sqlalchemy.dialects.mysql import DOUBLE

class PromotionLineVersion(db.Model):
    __tablename__ = 'promotion_line_version'
    id = db.Column(db.BigInteger, primary_key = True)

    id_promotion = db.Column(db.BigInteger, db.ForeignKey('promotion.id'))
    promotion = db.relationship("Promotion", backref=db.backref("promotion_line_version", uselist=False))


    id_product = db.Column(db.BigInteger, db.ForeignKey('product_list.id'))
    product = db.relationship("ProductList", backref=db.backref("promotion_line_version", uselist=False))

    id_user = db.Column(db.BigInteger, db.ForeignKey('user_account.id'))
    user = db.relationship("User", backref=db.backref("promotion_line_version_version", uselist=False))


    type_promotion_id = db.Column(db.Integer, db.ForeignKey('type_promotion.id'))
    type_promotion = db.relationship("TypePromotion", backref=db.backref("promotion_line_version", uselist=False))

    recommendation = db.Column(db.Integer)
    current_description_product = db.Column(db.String)
    current_volume_sold = db.Column(DOUBLE)
    current_volume_optimization = db.Column(DOUBLE)
    current_volume_proposed = db.Column(DOUBLE)
    volume_variation = db.Column(DOUBLE)
    base_price = db.Column(DOUBLE)
    current_price = db.Column(DOUBLE)
    current_optimization_price = db.Column(DOUBLE)
    proposed_price = db.Column(DOUBLE)
    variation_current_opt_price = db.Column(DOUBLE)
    variation_base_prosed_price = db.Column(DOUBLE)
    discount = db.Column(DOUBLE)
    proposed_ro = db.Column(DOUBLE)
    customer_margin = db.Column(DOUBLE)
    recommend_pvp = db.Column(DOUBLE)
    start_sellin = db.Column(db.DateTime)
    end_sellin = db.Column(db.DateTime)
    start_sellout = db.Column(db.DateTime)
    end_sellout = db.Column(db.DateTime)
    version_update_date = db.Column(db.DateTime)
    promotional_variables_json = db.Column(db.String)
    id_promotional_state = db.Column(db.BigInteger, db.ForeignKey('promotional_state.id'))
    promotional_state = db.relationship("PromotionalState", backref=db.backref("promotion_line_version", uselist=False))
    


    def retrieve_data(self):
        try:
            start_sellin=self.start_sellin.strftime("%d/%b/%Y")
        except:
            # start_sellin=self.start_sellin
            start_sellin=""
        
        try:
            end_sellin=self.end_sellin.strftime("%d/%b/%Y")
        except:
            # end_sellin=self.end_sellin
            end_sellin=""

        try:
            start_sellout=self.start_sellout.strftime("%d/%b/%Y")
        except:
            # start_sellout=self.start_sellout
            start_sellout=""

        try:
            end_sellout=self.end_sellout.strftime("%d/%b/%Y")
        except:
            # end_sellout=self.end_sellout
            end_sellout=""

        # try:
        #     version_update_date=self.version_update_date.strftime("%d/%b/%Y")
        # except:
        version_update_date=self.version_update_date
        

        return { 
                "id": self.id,
                "id_promotion": self.id_promotion,
                #"promotion": self.promotion.retrieve_data(),
                "id_product": self.id_product,
                #"product": self.product.retrieve_data(),
                "id_user": self.id_user,
                "type_promotion_id": self.type_promotion_id,
                #"type_promotion": self.type_promotion.retrieve_data(),
                "recommendation": self.recommendation,
                "current_description_product": self.current_description_product,
                "current_volume_sold": float(self.current_volume_sold),
                "current_volume_optimization": float(self.current_volume_optimization),
                "current_volume_proposed": float(self.current_volume_proposed),
                "volume_variation": float(self.volume_variation),
                "base_price": float(self.base_price),
                "current_price": float(self.current_price),
                "current_optimization_price": float(self.current_optimization_price),
                "proposed_price": float(self.proposed_price),
                "discount": float(self.discount),
                "customer_margin": float(self.customer_margin),
                "recommend_pvp": float(self.recommend_pvp),
                "start_sellin": start_sellin,
                "end_sellin": end_sellin,
                "start_sellout": start_sellout,
                "end_sellout": end_sellout,
                "version_update_date": version_update_date,
                "promotional_variables_json": self.promotional_variables_json,
                "id_promotional_state": self.id_promotional_state,
                #"promotional_state": self.promotional_state.retrieve_data(),
                "variation_current_opt_price": self.variation_current_opt_price,
                "variation_base_prosed_price": self.variation_base_prosed_price,
                "proposed_ro": self.proposed_ro

        }

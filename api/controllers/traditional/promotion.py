


from api.models.pb_promotion import Promotion

def pull_promotion(promotion_id):
    customers = Promotion.query.filter(Promotion.id==promotion_id)
    customers_dict = [customer.retrieve_data() for customer in customers]
    return customers_dict

def pull_promotion_by_id(promotion_id):
    """Get a promotion by id"""
    return Promotion.query.filter_by(id=promotion_id).first()
from api.controllers.utils import pull_outcome_query
from api.models.customer import Customer


def pull_customers():
    customers = Customer.query.filter(Customer.id!=13)
    customers_dict = [customer.retrieve_data() for customer in customers]
    return customers_dict

def pull_offer_customer(offer_id):
    query = f"SELECT distributors_id, distributors_name FROM pb_promotion WHERE id={offer_id};"
    outcome = pull_outcome_query(query)
    return {'customer_id': outcome[0][0], 'customer_name': outcome[0][1]}
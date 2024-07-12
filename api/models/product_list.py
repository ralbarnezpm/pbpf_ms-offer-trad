from ..extensions import db

class ProductList(db.Model):
    __tablename__ = 'pb_product_list'

    id = db.Column(db.BIGINT, primary_key=True)
    code_product = db.Column(db.String(191), nullable=False, unique=True, collation='utf8mb4_general_ci')
    description = db.Column(db.Text, nullable=False, collation='utf8mb4_general_ci')
    category = db.Column(db.BIGINT, nullable=False)
    category_name = db.Column(db.String(30), nullable=False, default='', collation='utf8mb4_general_ci')
    subcategory = db.Column(db.BIGINT, nullable=False)
    subcategory_name = db.Column(db.String(30), nullable=False, default='0', collation='utf8mb4_general_ci')
    brand = db.Column(db.String(100), nullable=False, collation='utf8mb4_general_ci')
    um = db.Column(db.String(10), nullable=False, collation='utf8mb4_general_ci')
    avg_weight = db.Column(db.FLOAT, nullable=False)
    units_x_product = db.Column(db.FLOAT, nullable=False)
    short_brand = db.Column(db.String(50), default='', collation='utf8mb4_general_ci')
    subfamily = db.Column(db.String(50), default='', collation='utf8mb4_general_ci')
    product_state = db.Column(db.String(50), nullable=True, collation='utf8mb4_general_ci')
    brand_code = db.Column(db.TINYINT, nullable=True)

    def retrieve_data(self):
        data = {
            'id': self.id,
            'code_product': self.code_product,
            'description': self.description,
            'category': self.category,
            'category_name': self.category_name,
            'subcategory': self.subcategory,
            'subcategory_name': self.subcategory_name,
            'brand': self.brand,
            'um': self.um,
            'avg_weight': self.avg_weight,
            'units_x_product': self.units_x_product,
            'short_brand': self.short_brand,
            'subfamily': self.subfamily,
            'product_state': self.product_state,
            'brand_code': self.brand_code
        }
        return data
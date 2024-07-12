from flask import Blueprint, jsonify, make_response, request, send_file
from api.auth.authentication import verify_token_middleware
from api.controllers.traditional.activity import pull_offer_activity, pull_promotion_activity
from api.controllers.traditional.catalog import check_created_promotion_offices, distributors_name, pull_brands, pull_offices
from api.controllers.traditional.massive_load import bulk_update_promotion_products, insert_massive_load_to_promotion_products
from api.controllers.traditional.offer import check_duplicate_name, create_promotion_controller, data_create, delete_offer_product, insert_offer_product, pull_brand_tracking, pull_product_optimization_view, pull_product_view, pull_products, pull_pvp, save_offer, simulator_handler
from datetime import datetime
from io import BytesIO

from api.controllers.traditional.offer_home import listall_offer, pull_offer_current_products, update_offer_phase
from api.utils import back_to_format, to_number_format
from ...extensions import admin_users_id
from json import loads

from api.controllers.utils import pull_dataframe_from_sql
from api.routes.utils import MONTH_TO_NUMBER, month_year_promo_str, pull_timestamp

offer_bp = Blueprint('offer_bp', __name__)

def to_date(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d")
     

@offer_bp.route("/create/data_create", methods=["GET"])
@verify_token_middleware
def get_data_create(payload): 
    offices=pull_offices()
    brands=pull_brands()
    
    data={"offices":offices,"brands":brands}
    response=jsonify({"status":"success","data":data, "error": None, "allowed": True})
    return response


@offer_bp.route("/create/validate", methods=["POST"])
@verify_token_middleware
def get_create_validation(payload):

    year=request.json.get('promotion_year_str')
    month=request.json.get('promotion_month_str')
    offices=request.json.get('office_codes')
    brand = request.json.get('brand_id')

    validated_offices = check_created_promotion_offices(year, month, brand, offices)
    if len(validated_offices)==0:
        return {"message": "promotion exists", "new_offices": validated_offices, "exists": True}
    return {"message": "promotion does not exist", "new_offices": validated_offices, "exists": False} 


@offer_bp.route("/create/create", methods=["POST"])
@verify_token_middleware
def get_catalog_summary(payload):
    if payload["rol"] in admin_users_id:
        year_month = month_year_promo_str(request.json.get('promotion_month_str'), request.json.get('promotion_year_str'))
        year_month01_str=f"{year_month}-01"
        created_at=updated_at=pull_timestamp()
        user_id=payload['id']
        year_str=request.json.get('promotion_month_str')
        month_str=request.json.get('promotion_year_str')
        brand_id=request.json.get('brand_id')
        office_codes=request.json.get('office_codes')
        validated_offices = office_codes#check_created_promotion_offices(year_str, month_str, brand_id, office_codes)
        offices_str = ','.join([f"{str(valor)}" for valor in validated_offices])

        if len(validated_offices)==0:
            return jsonify({"promotion_id": None, "created": False, "error": "promocion ya existe para las oficinas dadas"}), 500
        
        distributors = distributors_name(offices_str)
        print("distributors:", distributors)
        new_promotion = {
            "promotion_name": check_duplicate_name(request.json.get('promotion_name')),
            "promotion_type_id": 2,
            "promotional_state_id": 1,
            "channel_id": 2,
            "created_by": user_id,
            "brand_id": brand_id,
            "distributors_id": offices_str,
            "distributors_name": distributors,
            "start_sellin": to_date(request.json.get('start_sellin')),
            "end_sellin": to_date(request.json.get('end_sellin')),
            "month_str": request.json.get('promotion_month_str'),
            "year_str": request.json.get('promotion_year_str'),
            "month_num": MONTH_TO_NUMBER[request.json.get('promotion_month_str')],
            "year_month_str": year_month,
            "year_month01_str": year_month01_str,
            "created_at": created_at,
            "updated_at": updated_at,
        }
        try:
            promotion_id = create_promotion_controller(user_id, new_promotion)
            return jsonify({"promotion_id": promotion_id, "created": True, "error": None})
        except Exception as e:
            return jsonify({"promotion_id": None, "created": False, "error": str(e)}), 500
    else:
        return jsonify({"created": False, "error": "No tiene permisos para crear promociones"}), 401


# @offer_bp.route("/products/listall/<offer_id>", methods=["GET"])
# # @verify_token_middleware
# # def get_test(payload):
# def get_test(customer):
#     print(customer)
#     customer="JUMBO"
#     products=pull_products(customer)
#     return jsonify(products)

@offer_bp.route("/products/update/<offer_id>", methods=["POST"])
@verify_token_middleware
def update_offer_products(payload, offer_id):
    if payload["rol"] in admin_users_id:
        data_rows = request.json#.get("data_rows")
        insert_messages = insert_offer_product(offer_id, data_rows)
        return jsonify(insert_messages)
    else:
        return jsonify({"created": False, "error": "No tiene permisos para crear promociones"}), 401
    
@offer_bp.route("/products/<offer_id>", methods=["GET"])
@verify_token_middleware
def pull_offer_products_view(payload, offer_id):
    json_data_python = pull_product_view(offer_id)
    return jsonify(json_data_python)

@offer_bp.route("/products/current/<offer_id>", methods=["GET"])
@verify_token_middleware
def pull_offer_current_products_hdlr(payload, offer_id):
    json_data_python = pull_offer_current_products(offer_id)
    return jsonify(json_data_python)

@offer_bp.route("/product/<product_code>/<customer>/<offer_id>", methods=["GET"])
@verify_token_middleware
def pull_product_view_(payload, product_code, customer, offer_id):
    json_data_python = pull_product_optimization_view(product_code, customer, offer_id)
    return jsonify(json_data_python)

@offer_bp.route("/save/<offer_id>", methods=["POST"])
@verify_token_middleware
def save_offer_products(payload, offer_id):
    data_rows = request.json.get("data_rows")
    save_msg, save_status = save_offer(offer_id, data_rows)
    return {"msg": save_msg}, 200 if save_status else 400

@offer_bp.route("/product/delete/<offer_product_id>", methods=["POST"])
@verify_token_middleware
def delete_offer_product_by_id(payload, offer_product_id):
    response, code = delete_offer_product(offer_product_id)
    return {"msg": response}, code

@offer_bp.route("/simulate/<offer_id>", methods=["POST"])
@verify_token_middleware
def simulate_offer_products(payload, offer_id):
    data_rows = request.json.get("data_rows")
    offer_data = request.json.get("offer_data")
    json_data_python = simulator_handler(offer_id, data_rows, offer_data)
    return jsonify(json_data_python)

@offer_bp.route("/listall", methods=["GET"])
@verify_token_middleware
def listall_toshow_offer(payload):
    offers = listall_offer()
    return jsonify(offers)

@offer_bp.route("/activity/<offer_id>", methods=["GET"])
@verify_token_middleware
def pull_activity(payload, offer_id):
    activity=pull_promotion_activity(offer_id)
    return jsonify(activity)

@offer_bp.route("/update_phase", methods=["POST"])
@verify_token_middleware
def update_phase(payload):
    promotional_state_id=request.json.get('promotionalstate_id')
    offer_id=request.json.get('promotion_id')
    update_comment=request.json.get('comment_text')
    user_id = payload["id"]
    msg, err = update_offer_phase(promotional_state_id, offer_id, update_comment, user_id)
    return jsonify({"msg": msg, "error": err}), 200 if err is None else 400

@offer_bp.route("/brand_tracking/<offer_id>", methods=["GET"])
@verify_token_middleware
def brand_tracking(payload, offer_id):
#def brand_tracking(offer_id):
    resp = pull_brand_tracking(offer_id)
    return jsonify(resp)

@offer_bp.route("/summary/<offer_id>", methods=["GET"])
@verify_token_middleware
def offer_activity(payload, offer_id):
    resp = pull_offer_activity(offer_id)
    return jsonify(resp)



@offer_bp.route("/products/massive/validation/<offer_id>", methods=["POST"])
@verify_token_middleware
# def create_promotion_bulk_validate(payload, offer_id):
def validate_promotion_bulk_validate(payload, offer_id):
    """ Validates a new bulk promotion """
    if 'file' not in request.files:
        return {'message': 'No Excel file has been provided.', }, 400
    
    user_id = payload["id"]
    file = request.files['file']
    json_errors, validations, success = bulk_update_promotion_products(file, user_id, offer_id)

    excel_output = BytesIO()
    validations.rename(columns={'product_code': 'Codigo Producto', 'on_offer': 'En Oferta', 'price_error': 'Error Precio'}).to_excel(excel_output, index=False, sheet_name='Detalle Validaciones')
    excel_output.seek(0)

    json_errors["allowed"] = 1

    if success:
        # Crear una respuesta HTTP
        response = make_response((jsonify(json_errors), 200))

        # Adjuntar el archivo Excel a la respuesta
        response.headers['Content-Disposition'] = 'attachment; filename=validaciones.xlsx'
        response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

        return response
    else:
        return json_errors, 500 
    
@offer_bp.route("/products/massive/create/<offer_id>", methods=["POST"])
@verify_token_middleware
# def create_promotion_bulk_validate(payload, offer_id):
def create_promotion_bulk_validate(payload, offer_id):
    user_id = payload["id"]
    message, success = insert_massive_load_to_promotion_products(user_id, offer_id)
    if success:
        return message, 200
    
    return message, 500


@offer_bp.route("/update_pvp", methods=["POST"])
@verify_token_middleware
def update_pvp(payload):
    data_rows = request.json["data_rows"]
    mg_pvp = request.json["mg_pvp"]
    for grouped_rows in data_rows:
        for office_product in grouped_rows["product_rows"]:
            print('grouped_rows["strat_price"]:', grouped_rows["__strat_price"])
            print('office_product["strat_price"]:', office_product["__strat_price"])
            try:
                if isinstance(office_product["strat_price"], str):
                    print(type(office_product["strat_price"]))
                    strat_price=int(back_to_format(office_product["strat_price"].replace("$", "")))
                else:
                    strat_price=office_product["strat_price"]
            except Exception as e:
                print(office_product["strat_price"], "\n"*3)
                print(e)

            if office_product["strat_price_modified"] == 1 and office_product["active_office"]==1:
                pvp_sug = pull_pvp(grouped_rows["brand_code"], office_product["product_state"], strat_price, office_product["avg_weight"], office_product["units_x_product"], mg_pvp)
                office_product["pvp_sug"] = to_number_format(pvp_sug, 0)
                office_product["__pvp_sug"] = pvp_sug
                office_product["strat_price_modified"] = 0

        if not (isinstance(office_product["__strat_price"], int) or isinstance(office_product["__strat_price"], float)):
            strat_price=int(back_to_format(grouped_rows["__strat_price"]))
        else:
            strat_price=grouped_rows["__strat_price"]

        pvp_sug = pull_pvp(grouped_rows["brand_code"], office_product["product_state"], strat_price, office_product["avg_weight"], office_product["units_x_product"], mg_pvp)

        grouped_rows["__recommended_pvp"] = pvp_sug
        grouped_rows["recommended_pvp"] = to_number_format(pvp_sug, 0)
        grouped_rows["strat_price_modified"] = 0
        grouped_rows["ever_modified"] = 1

    request.json["data_rows"] = data_rows 
    return jsonify(request.json)

from json import dumps, loads
from math import ceil, isnan
from api.controllers.comment import create_promotion_comment, pull_created_offer_comment
from api.controllers.traditional.catalog import pull_catalog_offer_products_coincidence
from api.controllers.traditional.customers import pull_customers, pull_offer_customer
from api.controllers.traditional.simulator import offer_simulator
from api.controllers.notification import create_notification
from api.controllers.utils import get_months, pull_dataframe_from_sql, pull_outcome_query, query_execute_w_err
from api.models.pb_promotion import Promotion
from api.routes.utils import pull_timestamp
from api.utils import back_to_format, format_number, to_number_format
from ...extensions import db
import pandas as pd
from sqlalchemy import MetaData, Table, update, exc


def data_create():
    customers = pull_customers()
    dates = get_months(6, False)
    return {
        "customers": customers,
        "dates": dates
    }


def prods_on_catalog(promotion_id, distributors_name):
    query = f"""SELECT p.id AS catalog_id, pr.code_product AS product_code, 
                    p.year_month_str, CASE WHEN COUNT(*)>0 THEN 1 ELSE 0 END AS on_catalog
                FROM traditional_promotion_line pl
                JOIN promotion p ON pl.id_promotion=p.id
                JOIN pb_product_list pr ON pl.id_product=pr.id
                JOIN office o ON pl.office=o.id
                WHERE p.year_month_str=(
                SELECT year_month_str FROM pb_promotion WHERE id={promotion_id}
                ) AND o.office_name IN ({distributors_name})
                GROUP BY pr.code_product;"""
    df_prods_catalogs = pull_dataframe_from_sql(query)
    return df_prods_catalogs

def prods_on_offer(promotion_id):
    query = f"""SELECT p.id as offer_id, p.distributors_name, pp.product_code, p.year_month_str
                FROM pb_promotion_product pp
                JOIN pb_promotion p ON pp.promotion_id=p.id
                WHERE p.year_month_str=(
                    SELECT year_month_str FROM pb_promotion WHERE id={promotion_id}
                ) AND p.promotion_type_id=2 and p.channel_id=2;"""
    df_prods_offers = pull_dataframe_from_sql(query)
    print(query)
    return df_prods_offers

def pull_products_on_catalog_offer(promotion_id):
    distributors = pull_dataframe_from_sql(f"SELECT distributors_name FROM pb_promotion WHERE id={promotion_id}")
    distributors_name = distributors['distributors_name'][0].split(",")
    distributors_name = [f"'{d}'" for d in distributors_name]
    distributors_name = ",".join(distributors_name)
    df_oncatalog=prods_on_catalog(promotion_id, distributors_name)
    df_offer=prods_on_offer(promotion_id)
    df=pd.merge(df_oncatalog, df_offer, on='product_code', how='left')
    df['on_catalog']=df['catalog_id'].apply(lambda x: 1 if x and x>0 else 0)
    df['on_offer']=df['offer_id'].apply(lambda x: 1 if x and x>0 else 0)
    df_prodon=df[['product_code', 'on_catalog', 'on_offer']]

    del df_oncatalog
    del df_offer
    del df

    return df_prodon


def pull_customer_products(promotion_id):
    distributors = pull_dataframe_from_sql(f"SELECT distributors_name FROM pb_promotion WHERE id={promotion_id}")
    distributors_name = distributors['distributors_name'][0].split(",")
    distributors_name = [f"'{d}'" for d in distributors_name]
    distributors_name = ",".join(distributors_name)

    q = f"""SELECT o.id_producto_estrategia AS offer_product_id,
                codigo_producto AS product_code,
                p.description,
                p.category as category_id,
                p.category_name,
                p.subcategory as subcategory_id,
                p.subcategory_name,
                p.short_brand,
                max(p.avg_weight) as avg_weight,
                max(p.units_x_product) as units_x_product,
                p.brand_code,
                -- oficina AS office,
                tipo_estrategia AS strategy_type,
                nombre_estrategia AS strategy_name,
                avg(volumen_estrategico) AS strategic_volume,
                avg(precio_estrategico) AS strategic_price,
                avg(precio_base) AS base_price,
                avg(precio_actual) AS current_price,
                avg(volumen_actual) AS current_volume,
                -- margen_cadena AS customer_margin,
                avg(elasticidad) AS elasticity,
                avg(costo_directo) AS direct_cost,
                avg(oc_pesos) as oc_pesos, 
                avg(venta) AS sale, 
                avg(oc_adim) as oc_adim,
                avg(kilos) AS kilos,
                avg(oc_pesos_kilos) AS oc_pesos_kilos,
                avg(oc_adim_venta) AS oc_adim_sale,
                avg(pc) AS critical_price
                -- ,CASE WHEN pp.id IS NULL then 0 ELSE 1 END AS 'on_offer'
            FROM oferta_tradicional_base o
            LEFT JOIN pb_product_list p ON o.codigo_producto=p.code_product
            LEFT JOIN (SELECT id, product_code FROM pb_promotion_product WHERE promotion_id={promotion_id}) pp ON o.codigo_producto=pp.product_code
            WHERE oficina IN ({distributors_name}) AND 
                o.id_pb not in (SELECT product_id FROM pb_promotion_product WHERE promotion_id={promotion_id})
            GROUP BY o.codigo_producto, o.tipo_estrategia, o.nombre_estrategia;"""
    df=pull_dataframe_from_sql(q)
    print(q)
    return df, distributors_name

def pull_products_on_offer(promotion_id):
    query = f"SELECT product_id FROM pb_promotion_product WHERE promotion_id={promotion_id};"
    df=pull_dataframe_from_sql(query)
    return df

###############################################################################################
#   Products
###############################################################################################

def product_view_discount(row):
    row['discount'] = format_number((1 - row['strategic_price']/row['base_price'])*100, 1)
    return row

def product_view_critical_price(row):
    row['critical_price2'] = format_number((row['direct_cost'] + row['oc_pesos_kilos'])/(1 - row['oc_adim_sale']), 0)
    return row

def umd_to_kg_price(price_key, brand_code, row):
    if brand_code>6:
        kg_price=row[price_key]*row['units_x_product']/row['avg_weight']
    else:
        kg_price=row[price_key]
    return kg_price

def product_view_strategic_ro(row):
    strategic_price_kg = umd_to_kg_price('strategic_price', row['brand_code'], row)
    try:
        strategic_ro = (strategic_price_kg - row['direct_cost'] - row['oc_pesos_kilos'] - (row['oc_adim_sale']*strategic_price_kg)) / strategic_price_kg
    except ZeroDivisionError:
        strategic_ro = 0
    row['strategic_ro'] = format_number(strategic_ro*100, 1)
    return row

def pull_products(promotion_id):
    df, distributors_name = pull_customer_products(promotion_id)

    df=df.apply(product_view_strategic_ro, axis=1)
    #df=df.apply(product_view_critical_price, axis=1)
    df=df.apply(product_view_discount, axis=1)

    # df_oncatalog=prods_on_catalog(promotion_id, distributors_name)
    # df=pd.merge(df, df_oncatalog[['product_code', 'on_catalog']], on='product_code', how='left')

    df_oncatalog_onoffer = pull_products_on_catalog_offer(promotion_id)
    df=pd.merge(df, df_oncatalog_onoffer, on='product_code', how='left')

    cols_round_0 = ["base_price", "current_price", "direct_cost", "critical_price", "strategic_price", "oc_pesos", "sale", "oc_adim", "kilos"]
    df[cols_round_0] = df[cols_round_0].applymap(lambda x: format_number(x, 0))
    cols_round_1 = ["current_volume", "strategic_volume", "elasticity", "oc_pesos_kilos", "oc_adim_sale"]
    df[cols_round_1] = df[cols_round_1].applymap(lambda x: format_number(x, 1))

    df['category_id']=df['category_id'].fillna(-1).astype(int)
    df['subcategory_id']=df['subcategory_id'].fillna(-1).astype(int)
    df['brand_code']=df['brand_code'].fillna(-1).astype(int)
    df['on_catalog']=df['on_catalog'].fillna(-1).astype(int)
    df['on_offer']=df['on_offer'].fillna(-1).astype(int)

    

    json_data_pandas = df.to_json(orient='records')
    json_data_python = loads(json_data_pandas)

    del df
    # del df_oncatalog
    del df_oncatalog_onoffer

    return json_data_python

def insert_promotion(promotion_dict):
    new_promotion = Promotion(**promotion_dict)
    db.session.add(new_promotion)
    db.session.commit()
    inserted_id = new_promotion.id
    return inserted_id

def check_duplicate_name(promotion_name):
    promotions = Promotion.query.filter_by(promotion_name=promotion_name).all()
    if len(promotions) > 0:
        promotion_name = f"{promotion_name} - {pull_timestamp()}"
    return promotion_name


def create_promotion_controller(user_id, promotion_dict):
    promotional_state_id=1
    promotion_id=insert_promotion(promotion_dict)
    comment = pull_created_offer_comment(user_id, promotion_dict['promotion_name'])
    created, comment_err, new_comment_id = create_promotion_comment(promotion_id, user_id, comment, promotional_state_id)
    if created: 
        notification_created, err = create_notification(4, "Creacion", new_comment_id, user_id)
        if not notification_created:
            print("error trying to insert a new notification:\n", err)
        else:
            print("notification successfully created!")
    else:
        print("error trying to insert a new comment:\n", comment_err)
    #created, err, new_comment_id = comment_create(user_id, promotion_id, promotional_state_id, date_created, comment) 
    return promotion_id


def insert_offer_product(offer_id, data_rows):
    # customer_info=pull_offer_customer(offer_id)
    # customer_id, customer_name = customer_info.values()
    product_codes=[f"'{data['offer_product_id']}'" for data in data_rows]
    
    #df_current_products=pull_products_on_offer(offer_id)
    #current_product_codes=df_current_products['product_id'].tolist()
    #new_product_codes = [code for code in product_codes if code not in current_product_codes]
    #del df_current_products

    new_product_codes=product_codes

    new_product_codes= ",".join(map(str, new_product_codes))
    inserted=insert_statetment(offer_id, new_product_codes)
    if inserted:
        return {'message': 'products inserted successfully'}
    else:
        return {'message': 'error while inserting products'}

def insert_statetment(offer_id, product_codes):
    if len(product_codes)>0:
        promotion_vars = pull_dataframe_from_sql(f"select p.start_sellin, p.end_sellin, p.start_sellout, p.end_sellout, p.distributors_name from pb_promotion p where p.id={offer_id}")
        start_sellin = f"'{promotion_vars['start_sellin'][0]}'" if promotion_vars['start_sellin'][0] else 'NULL'
        end_sellin = f"'{promotion_vars['end_sellin'][0]}'" if not promotion_vars['end_sellin'][0] else 'NULL'
        start_sellout = f"'{promotion_vars['start_sellout'][0]}'" if promotion_vars['start_sellout'][0] else 'NULL'
        end_sellout = f"'{promotion_vars['end_sellout'][0]}'" if promotion_vars['end_sellout'][0] else 'NULL'

        distributors_name = promotion_vars['distributors_name'][0].split(",")
        distributors_name = [f"'{d}'" for d in distributors_name]
        distributors_name = ",".join(distributors_name)

        query = f"""INSERT INTO pb_promotion_product (
                        promotion_id,
                        product_id,
                        product_code,
                        product_description,
                        promotion_channel,
                        promotion_type_name,
                        recommendation_id,
                        recommendation_name,
                        current_volume,
                        optimization_volume,
                        strategic_volume,
                        base_price,
                        current_price,
                        optimization_price,
                        strategic_price,
                        critical_price,
                        oc_adim,
                        oc_adim_sale,
                        oc_pesos,
                        oc_pesos_kilos,
                        product_state,
                        brand_code,
                        units_x_product,
                        avg_weight,
                        start_sellin,
                        end_sellin,
                        start_sellout,
                        end_sellout,
                        promotionalstate_id,
                        promotionalstate_phase,
                        tooltip_strategic_pxu,
                        tooltip_strategic_sp,
                        tooltip_optimization_pxu,
                        tooltip_optimization_sp,
                        tooltip_current_pxu,
                        tooltip_current_sp,
                        -- tooltip_base_pxu,
                        -- tooltip_base_sp,
                        short_brand,
                        brand,
                        family,
                        subfamily,
                        strategy_name,
                        elasticity,
                        strategic_volume_kg,
                        distributor_name,
                        active
                    )
                    SELECT
                        {offer_id} AS promotion_id,
                        o.id_pb AS product_id,
                        o.codigo_producto AS product_code,
                        pl.description AS product_description,
                        'Tradicional' AS promotion_channel,
                        'Oferta' AS promotion_type_name,
                        o.tipo_estrategia AS recommendation_id,
                        o.nombre_estrategia AS recommendation_name,
                        o.volumen_actual AS current_volume,
                        o.volumen_optimo AS optimization_volume,
                        o.volumen_estrategico AS strategic_volume,
                        o.precio_base AS base_price,
                        o.precio_actual AS current_price,
                        o.precio_optimo AS optimization_price,
                        o.precio_estrategico AS strategic_price,
                        o.pc AS critical_price,
                        o.oc_adim,
                        o.oc_adim_venta AS oc_adim_sale,
                        o.oc_pesos,
                        o.oc_pesos_kilos,
                        o.estado_producto AS product_state,
                        o.codigo_linea AS brand_code,
                        o.unidad_x_producto AS units_x_product,
                        o.peso_promedio AS avg_weight,
                        {start_sellin},
                        {end_sellin},
                        {start_sellout},
                        {end_sellout},
                        1 AS promotionalstate_id,
                        1 AS promotionalstate_phase,
                        o.pxu_estrategico AS tooltip_strategic_pxu,
                        o.pv_estrategico AS tooltip_strategic_sp,
                        o.pxu_optimo AS tooltip_optimization_pxu,
                        o.pv_optimo AS tooltip_optimization_sp,
                        o.pxu_actual AS tooltip_current_pxu,
                        o.pv_actual AS tooltip_current_sp,
                        -- o.pxu_base AS tooltip_base_pxu,
                        -- o.pv_base AS tooltip_base_sp,
                        pl.short_brand,
                        c.category AS brand,
                        s.subcategory AS family,
                        pl.subfamily,
                        o.nombre_estrategia AS strategy_name,
                        o.elasticidad AS elasticity,
                        o.volumen_estrategico_kg AS strategic_volume_kg,
                        o.oficina AS distributor_name,
                        1 as active
                    FROM
                        oferta_tradicional_base o
                    JOIN
                        pb_product_list pl ON pl.code_product = o.codigo_producto
                    JOIN
                        category c ON pl.category = c.id
                    JOIN
                        subcategory s ON pl.subcategory = s.id
                    WHERE o.id_producto_estrategia IN ({product_codes}) AND o.oficina IN ({distributors_name})
                    ON DUPLICATE KEY UPDATE
                        product_id = VALUES(product_id),
                        product_description = VALUES(product_description),
                        promotion_channel = VALUES(promotion_channel),
                        promotion_type_name = VALUES(promotion_type_name),
                        recommendation_id = VALUES(recommendation_id),
                        recommendation_name = VALUES(recommendation_name),
                        current_volume = VALUES(current_volume),
                        optimization_volume = VALUES(optimization_volume),
                        strategic_volume = VALUES(strategic_volume),
                        base_price = VALUES(base_price),
                        current_price = VALUES(current_price),
                        optimization_price = VALUES(optimization_price),
                        strategic_price = VALUES(strategic_price),
                        critical_price = VALUES(critical_price),
                        oc_adim = VALUES(oc_adim),
                        oc_adim_sale = VALUES(oc_adim_sale),
                        oc_pesos = VALUES(oc_pesos),
                        oc_pesos_kilos = VALUES(oc_pesos_kilos),
                        product_state = VALUES(product_state),
                        units_x_product = VALUES(units_x_product),
                        avg_weight = VALUES(avg_weight),
                        start_sellin = VALUES(start_sellin),
                        end_sellin = VALUES(end_sellin),
                        start_sellout = VALUES(start_sellout),
                        end_sellout = VALUES(end_sellout),
                        promotionalstate_id = VALUES(promotionalstate_id),
                        promotionalstate_phase = VALUES(promotionalstate_phase),
                        tooltip_strategic_pxu = VALUES(tooltip_strategic_pxu),
                        tooltip_strategic_sp = VALUES(tooltip_strategic_sp),
                        tooltip_optimization_pxu = VALUES(tooltip_optimization_pxu),
                        tooltip_optimization_sp = VALUES(tooltip_optimization_sp),
                        tooltip_current_pxu = VALUES(tooltip_current_pxu),
                        tooltip_current_sp = VALUES(tooltip_current_sp),
                        -- tooltip_base_pxu = VALUES(tooltip_base_pxu),
                        -- tooltip_base_sp = VALUES(tooltip_base_sp),
                        short_brand = VALUES(short_brand),
                        brand = VALUES(brand),
                        family = VALUES(family),
                        subfamily = VALUES(subfamily),
                        strategy_name = VALUES(strategy_name),
                        elasticity = VALUES(elasticity),
                        strategic_volume_kg = VALUES(strategic_volume_kg),
                        distributor_name = VALUES(distributor_name),
                        active = VALUES(active);"""
        err=query_execute_w_err(query)
        print(query)
        # print(query)
        if err:
            print(f"error while inserting products {product_codes} to the modern offer", "\n", err)
            return False
        return True
    print("No new products to insert...")
    return False

def pull_offer_family_samemonth_products(offer_id):
    query = f"""SELECT pl.current_volume AS curr_vol,
                    pl.optimization_volume AS opt_vol,
                    pl.strategic_volume AS strat_vol,
                    
                    pl.base_price,
                    pl.current_price AS curr_price,
                    pl.optimization_price AS opt_price,
                    pl.strategic_price AS strat_price,
                    pl.critical_price,
                    
                    pl.customer_margin,
                    pl.promotional_variables_json,
                    
                    p.promotion_name,
                    p.id AS promotion_id,
                    pl.product_code,
                    pl.product_description,
                    pl.units_x_product,
                    pl.avg_weight,
                    pl.brand_code,
                    pl.short_brand,
                    pl.brand,
                    pl.family,
                    pl.product_state,
                    pl.subfamily,

                    pl.oc_adim,
                    pl.oc_adim_sale,
                    pl.oc_pesos,
                    pl.oc_pesos_kilos,
                    pl.direct_cost,
                    
                    pl.tooltip_base_pxu,
                    pl.tooltip_base_sp,
                    pl.tooltip_current_pxu,
                    pl.tooltip_current_sp,
                    pl.tooltip_optimization_pxu,
                    pl.tooltip_optimization_sp,
                    pl.tooltip_strategic_pxu,
                    pl.tooltip_strategic_sp,

                    pl.start_sellin,
                    pl.end_sellin,
                    pl.start_sellout,
                    pl.end_sellout,

                    pl.elasticity,
                    pl.strategy_name,
                    pl.model
                FROM pb_promotion_product pl
                JOIN pb_promotion p ON pl.promotion_id=p.id
                WHERE p.year_month01_str=(SELECT year_month01_str FROM pb_promotion WHERE id={offer_id}) 
                    AND p.id<>{offer_id} AND p.promotion_type_id=2 AND p.channel_id=2;"""
    df = pull_dataframe_from_sql(query)
    df["type"] = "offer"
    return df
    
def pull_offer_family_products(offer_id):
    query = f"""SELECT pl.id, pl.current_volume AS curr_vol,
                    pl.optimization_volume AS opt_vol,
                    pl.strategic_volume AS strat_vol,
                    
                    pl.base_price,
                    pl.current_price AS curr_price,
                    pl.optimization_price AS opt_price,
                    pl.strategic_price AS strat_price,
                    pl.critical_price,
                    
                    pl.customer_margin,
                    pl.promotional_variables_json,
                    
                    p.promotion_name,
                    p.id AS promotion_id,
                    pl.product_code,
                    pl.product_description,
                    pl.units_x_product,
                    pl.avg_weight,
                    pl.brand_code,
                    pl.short_brand,
                    pl.brand,
                    pl.family,
                    pl.product_state,
                    pl.subfamily,

                    pl.oc_adim,
                    pl.oc_adim_sale,
                    pl.oc_pesos,
                    pl.oc_pesos_kilos,
                    pl.direct_cost,
                    
                    pl.tooltip_base_pxu,
                    pl.tooltip_base_sp,
                    pl.tooltip_current_pxu,
                    pl.tooltip_current_sp,
                    pl.tooltip_optimization_pxu,
                    pl.tooltip_optimization_sp,
                    pl.tooltip_strategic_pxu,
                    pl.tooltip_strategic_sp,

                    pl.start_sellin,
                    pl.end_sellin,
                    pl.start_sellout,
                    pl.end_sellout,

                    pl.elasticity,
                    pl.strategy_name,
                    pl.model,

                    pl.active,
                    pl.distributor_name,

                    pl.strategic_volume_kg AS tooltip_strat_vol_kg
                FROM pb_promotion_product pl
                JOIN pb_promotion p ON pl.promotion_id=p.id
                WHERE pl.promotion_id={offer_id};"""
    df = pull_dataframe_from_sql(query)
    df["type"] = "offer"
    return df

def pull_strat_ro_pct(row):
    strategic_price_kg = umd_to_kg_price('__strat_price', row['brand_code'], row)
    strategic_ro = (strategic_price_kg - row['direct_cost'] - row['oc_pesos_kilos'] - (row['oc_adim_sale']*strategic_price_kg)) / strategic_price_kg
    row['strat_ro_pct'] = strategic_ro#format_number(strategic_ro*100, 1)
    # row['strat_ro_pct'] = format_number(strategic_ro*100, 1)
    return row['strat_ro_pct']

def pull_ro_strat_price(row):
    #RO$Estr = volumen_estrategico * (precio_estrategico - costo_directo - oc_pesos_kilos - (oc_adim_venta*precio_estrategico))
    strategic_price_kg = umd_to_kg_price('strat_price', row['brand_code'], row)
    direct_cost_kg = umd_to_kg_price('direct_cost', row['brand_code'], row)
    strategic_volume_kg = umd_to_kg_price('__strat_vol', row['brand_code'], row)
    strategic_ro = strategic_volume_kg*(strategic_price_kg - direct_cost_kg - row['oc_pesos_kilos'] - (row['oc_adim_sale']*strategic_price_kg))
    # row['strat_ro_price']=format_number(strategic_ro, 0)
    # return row['strat_ro_price']
    return strategic_ro

def pull_ro_opt_price(row):
    #RO$Actual = volumen_actual * (precio_actual - costo_directo - oc_pesos_kilos - (oc_adim_venta*precio_actual))
    opt_price_kg = umd_to_kg_price('opt_price', row['brand_code'], row)
    direct_cost_kg = umd_to_kg_price('direct_cost', row['brand_code'], row)
    current_volume_kg = umd_to_kg_price('__opt_vol', row['brand_code'], row)
    opt_ro = current_volume_kg*(opt_price_kg - direct_cost_kg - row['oc_pesos_kilos'] - (row['oc_adim_sale']*opt_price_kg))
    # row['curr_ro_price']=format_number(current_ro, 0)
    # return row['curr_ro_price']
    return opt_ro

def pull_ro_curr_price(row):
    #RO$Actual = volumen_actual * (precio_actual - costo_directo - oc_pesos_kilos - (oc_adim_venta*precio_actual))
    current_price_kg = umd_to_kg_price('curr_price', row['brand_code'], row)
    direct_cost_kg = umd_to_kg_price('direct_cost', row['brand_code'], row)
    current_volume_kg = umd_to_kg_price('__curr_vol', row['brand_code'], row)
    current_ro = current_volume_kg*(current_price_kg - direct_cost_kg - row['oc_pesos_kilos'] - (row['oc_adim_sale']*current_price_kg))
    # row['curr_ro_price']=format_number(current_ro, 0)
    # return row['curr_ro_price']
    return current_ro

def pull_family_products_header(df):
    df_data_header=df[df["type"] == "offer"]
    index = df_data_header['brand_code'].first_valid_index()
    brand_code = df_data_header.loc[index, 'brand_code']
    # format_number(x['curr_vol'], 1, x['brand_code'])
    df_header=pd.DataFrame({
        
        'curr_vol': format_number(df_data_header['__curr_vol'].sum(), 1, brand_code),
        'opt_vol': format_number(df_data_header['__opt_vol'].sum(), 1, brand_code),
        'strat_vol': format_number(df_data_header['__strat_vol'].sum(), 1, brand_code),
        'curr_price': f"${format_number((df_data_header['__curr_price'] * df_data_header['__curr_vol']).sum() / df_data_header['__curr_vol'].sum(), 0)}",
        'opt_price': f"${format_number((df_data_header['__opt_price'] * df_data_header['__opt_vol']).sum() / df_data_header['__opt_vol'].sum(), 0)}",
        'strat_price': f"${format_number((df_data_header['__strat_price'] * df_data_header['__strat_vol']).sum() / df_data_header['__strat_vol'].sum(), 0)}",
        'opt_ro_mm': f"${format_number(df_data_header['__opt_ro_price'].sum()/10**6, 0)}",
        'curr_ro_mm': f"${format_number(df_data_header['__curr_ro_price'].sum()/10**6, 0)}",
        'strat_ro_mm': f"${format_number(df_data_header['__strat_ro_price'].sum()/10**6, 0)}",
        'strat_act_benefit': f"${format_number(df_data_header['__strat_ro_price'].sum() - df_data_header['__curr_ro_price'].sum(), 0)}",
        'opt_act_benefit': f"${format_number(df_data_header['__opt_ro_price'].sum() - df_data_header['__curr_ro_price'].sum(), 0)}",
        'pvp_margin': format_number(df_data_header['__pvp_margin'].mean()*100, 1)
    }, index=[0])
    print(df_header.head())
    json_header = df_header.to_json(orient='records')
    del df_header
    return loads(json_header)

def pull_family_products_summary(df_products):
    strat_op_res = df_products['__strat_ro_price'].sum()
    curr_op_res = df_products['__curr_ro_price'].sum()
    strat_ben = strat_op_res-curr_op_res

    opt_op_res = df_products['__opt_ro_price'].sum()
    opt_ben = opt_op_res-curr_op_res

    df_summary=pd.DataFrame({
        'strat_op_res': f"${format_number(strat_op_res, 0)}",
        'strat_ben': f"${format_number(strat_ben, 0)}",
        'opt_op_res': f"${format_number(opt_op_res, 0)}",
        'opt_ben': f"${format_number(opt_ben, 0)}",

    }, index=[0])

    json_summary = df_summary.to_json(orient='records')
    del df_summary
    return loads(json_summary)


def pull_strat_ro(row):
    if row['brand_code'] > 6:
        return (row['__strat_price'] * row['units_x_product'] / row['avg_weight'] - row['__critical_price']) * (
                row['__strat_vol'] * row['avg_weight'] / row['units_x_product'])
    else:
        return (row['__strat_price'] - row['__critical_price']) * row['__strat_vol']

def pull_opt_ro(row):
    if row['brand_code'] > 6:
        return (row['__opt_price'] * row['units_x_product'] / row['avg_weight'] - row['__critical_price']) * (
                row['__opt_vol'] * row['avg_weight'] / row['units_x_product'])
    else:
        return (row['__opt_price'] - row['__critical_price']) * row['__opt_vol']

def pull_curr_ro(row):
    if row['brand_code'] > 6:
        return (row['__curr_price'] * row['units_x_product'] / row['avg_weight'] - row['__critical_price']) * (
                row['__curr_vol'] * row['avg_weight'] / row['units_x_product'])
    else:
        return (row['__curr_price'] - row['__critical_price']) * row['__curr_vol']

def pull_grouped_strat_ro_pct(row, brand_code):
    row['umd_to_kg_price']=row.apply(lambda x: umd_to_kg_price('__strat_price', brand_code, x), axis=1)

    result = row.groupby(['product_code']).apply(lambda group: pd.Series({
        #row['strat_ro_pct'] = ((row['umd_to_kg_price'] - row['direct_cost'] - row['oc_pesos_kilos'] - (row['oc_adim_sale']*row['umd_to_kg_price'])) / row['umd_to_kg_price']).mean()
        'strat_ro_pct': format_number(((group['umd_to_kg_price'] - group['direct_cost'] - group['oc_pesos_kilos'] - (group['oc_adim_sale']*group['umd_to_kg_price'])) / group['umd_to_kg_price']).mean()*100, 1),
    }))
    return result['strat_ro_pct']

def pull_grouped_rows(df):
    index = df['brand_code'].first_valid_index()
    brand_code = df.loc[index, 'brand_code']

    result = df.groupby(['product_code', 'product_description']).apply(lambda group: pd.Series({
        'model': group['model'].iloc[0],
        'strategy_name': group['strategy_name'].iloc[0],
        'subfamily': group['subfamily'].iloc[0],
        'short_brand': group['short_brand'].iloc[0],
        'product_description': group['product_description'].iloc[0],
        'product_code': group['product_code'].iloc[0],
        'strat_price_modified': 0,

        'curr_vol': format_number(group['__curr_vol'].sum(), 1, brand_code),
        'opt_vol': format_number(group['__opt_vol'].sum(), 1, brand_code),
        'strat_vol': format_number(group['__strat_vol'].sum(), 1, brand_code),
        'var': format_number(group['__strat_vol'].sum() / group['__strat_vol'].sum() - 1, 1) + '%',
        'base_price': format_number((group['__curr_vol'] * group['__base_price']).sum() / group['__curr_vol'].sum(), 0),
        'curr_price': format_number((group['__curr_vol'] * group['__curr_price']).sum() / group['__curr_vol'].sum(), 0),
        'opt_price': format_number((group['__opt_vol'] * group['__opt_price']).sum() / group['__opt_vol'].sum(), 0),
        'strat_price': format_number((group['__strat_vol'] * group['__strat_price']).sum() / group['__strat_vol'].sum(), 0),
        'discount': format_number((1-((group['__strat_vol'] * group['__strat_price']).sum() / group['__strat_vol'].sum()) / ((group['__curr_vol'] * group['__base_price']).sum() / group['__curr_vol'].sum()))*100, 1),  # 1-strat_price_pond/base_price_pond
        # 'opt_ro': pull_opt_ro(group),
        # 'curr_ro': pull_curr_ro(group),
        # 'strat_ro': pull_strat_ro(group),
        'var_eb': format_number((group['__strat_vol'] * group['__strat_price']).sum() / group['__strat_vol'].sum()/group['__base_price'].mean()-1, 1) + '%',
        'var_ob': format_number((group['__opt_vol'] * group['__opt_price']).sum() / group['__opt_vol'].sum()/group['__base_price'].mean()-1, 1) + '%',
        
        'strat_ro_pct': pull_grouped_strat_ro_pct(group, brand_code).iloc[0], #format_number(group['__strat_ro_pct'].mean(), 0),
        'incr_ro': format_number(group['__incr_ro'].mean(), 0),
        'elasticity': format_number(group['__elasticity'].mean()*100, 1)  + '%',

        'customer_margin': format_number(group['__customer_margin'].mean()*100, 1),
        'recommended_pvp': format_number(1.19*(group['__strat_vol'] * group['__strat_price']).sum() / group['__strat_vol'].sum() / (1-group['__customer_margin'].mean()), 0),
        # 'weighted_avg_volume2': (group['volume2'] * group['price2']).sum() / group['price2'].sum(),
        # 'weighted_avg_volume3': (group['volume3'] * group['price3']).sum() / group['price3'].sum(),
    }))
    json_data = result.to_json(orient='records')
    return json_data

def pull_data_rows_v2(products_dataframe):
    
    grouped_products_json=loads(products_dataframe[products_dataframe["current_offer"]==1].drop_duplicates(subset=["product_code"]).to_json(orient='records'))
    products_dataframe=products_dataframe[products_dataframe["current_offer"] == 1]

    for product_json in grouped_products_json:
        product_code = product_json['product_code']
        product_dataframe = products_dataframe[products_dataframe['product_code'] == product_code].to_json(orient='records')

        product_json['products']=len(products_dataframe)
        del product_json["distributor_name"]
        product_json['product_rows'] = loads(product_dataframe)

    return grouped_products_json

def pull_data_rows(grouped_products_json, products_dataframe):
    for product_json in grouped_products_json:
        product_code = product_json['product_code']
        product_dataframe = products_dataframe[products_dataframe['product_code'] == product_code].to_json(orient='records')
        product_json['products']=len(products_dataframe)
        product_json['product_rows'] = loads(product_dataframe)
    return grouped_products_json
    products_json = df_result.to_json(orient='records')
    products_json = loads(products_json)

def update_tooltip_price(row, price_name):
    factor=row['units_x_product']/row['avg_weight']
    if row['brand_code']>6:
        row["tooltip_base_pxu"]=row['tooltip_base_pxu']*factor
        row["tooltip_base_sp"]=row["tooltip_base_pxu"]*row['units_x_product']
        row["tooltip_current_pxu"]=row["tooltip_current_pxu"]*factor
        row["tooltip_current_sp"]=row["tooltip_current_pxu"]*row['units_x_product']
        row["tooltip_optimization_pxu"]=row["tooltip_optimization_pxu"]*factor
        row["tooltip_optimization_sp"]=row["tooltip_optimization_pxu"]*row['units_x_product']
        row["tooltip_strategic_pxu"]=row["tooltip_strategic_pxu"]*factor
        row["tooltip_strategic_sp"]=row["tooltip_strategic_pxu"]*row['units_x_product']
    elif row['brand_code']<=6 and row['product_state']=='UN':
        row["tooltip_base_pxu"]=row['tooltip_base_pxu']*factor**-1
        row["tooltip_base_sp"]=row["tooltip_base_sp"]*row['units_x_product']
        row["tooltip_current_pxu"]=row["tooltip_current_pxu"]*factor**-1
        row["tooltip_current_sp"]=row["tooltip_current_sp"]*row['units_x_product']
        row["tooltip_optimization_pxu"]=row["tooltip_optimization_pxu"]*factor**-1
        row["tooltip_optimization_sp"]=row["tooltip_optimization_sp"]*row['units_x_product']
        row["tooltip_strategic_pxu"]=row["tooltip_strategic_pxu"]*factor**-1
        row["tooltip_strategic_sp"]=row["tooltip_strategic_sp"]*row['units_x_product']



    return row

def pull_pvp(line_code, product_state, strat_price, avg_weight, units_x_product, mg_pvp):
    try:
        if line_code <=6 and product_state == "UN":
            pvp_sug = round((int(strat_price)*(avg_weight/units_x_product))*1.19*(1+mg_pvp), 1)
        elif line_code in [1,4] and product_state == "UK":
            pvp_sug = round(int(strat_price)*1.19*(1+mg_pvp) * 0.25, 1)
        else:
            pvp_sug = round(int(strat_price)*1.19*(1+mg_pvp), 1)
        return ceil(pvp_sug / 10) * 10
    except Exception as e:
        print("error on pull_pvp function:\n", e)
        print("---------------------------")
        print("avg_weight:", avg_weight)
        print("units_x_product:", units_x_product)
        print("---------------------------")
        return 0


def pull_data_summary(products_dataframe):
    result = products_dataframe.groupby().apply(lambda group: pd.Series({
        'curr_vol': format_number(group['__curr_vol'].sum(), 1),
        'opt_vol': format_number(group['__opt_vol'].sum(), 1),
        'strat_vol': format_number(group['__strat_vol'].sum(), 1),
        'curr_price': format_number((group['__curr_vol'] * group['__curr_price']).sum() / group['__curr_vol'].sum(), 0),
        'opt_price': format_number((group['__opt_vol'] * group['__opt_price']).sum() / group['__opt_vol'].sum(), 0),
        'strat_price': (group['__strat_vol'] * group['__strat_price']).sum() / group['__strat_vol'].sum(),
        'opt_ro_mm': format_number(group['__opt_ro_price'].sum()/10**6, 0),
        'curr_ro_mm': format_number(group['__curr_ro_price'].sum()/10**6, 0),
        'strat_ro_mm': format_number(group['__strat_ro_price'].sum()/10**6, 0),
        'strat_act_benefit': format_number(group['__strat_ro_price'].sum() - group['__curr_ro_price'].sum(), 0),
        'opt_act_benefit': format_number(group['__opt_ro_price'].sum() - group['__curr_ro_price'].sum(), 0),
        'customer_margin': format_number(group['__customer_margin'].mean()*100, 1),

    }))

def pull_offer_data(offer_id):
    query = f"""SELECT p.promotion_name, DATE_FORMAT(p.start_sellin, '%d/%m/%y') AS start_sellin,
                    DATE_FORMAT(p.end_sellin, '%d/%m/%y') AS end_sellin, 
                    DATE_FORMAT(p.start_sellout, '%d/%m/%y') AS start_sellout, 
                    DATE_FORMAT(p.end_sellout, '%d/%m/%y') AS end_sellout, 
                    p.distributors_name AS customer, p.month_str AS month,
                    c.description_channel AS channel, p.id AS offer_id,
                    p.distributors_mask
                FROM pb_promotion p JOIN `channel` c ON p.channel_id=c.id WHERE p.id={offer_id};"""
    print(query)
    outcome = pull_outcome_query(query)
    for row in outcome:
        customers_array = row['customer'].split(",") if row['customer'] else []
        customers=len(customers_array)
        group = row['distributors_mask'] or ""
        data = {
            'offer_id': row['offer_id'],
            'promotion_name': row['promotion_name'],
            'start_sellin': row['start_sellin'],
            'end_sellin': row['end_sellin'],
            'start_sellout': row['start_sellout'],
            'end_sellout': row['end_sellout'],
            'channel': row['channel'],
            'month': row['month']
        }

        data['offices'] = f"{group} ({customers})" if len(group) > 2 else ", ".join([c.capitalize() for c in customers_array])
        return data
    return {}

def apply_price_format(df_result):
    inst=(int, float)
    df_result['curr_price']=df_result['__curr_price'].apply(lambda x: format_number(x, 0) if (isinstance(x, inst) and x is not None) else None)
    df_result['curr_price']=df_result['curr_price'].apply(lambda x: f"${x}" if x else "")
    df_result['opt_price']=df_result['__opt_price'].apply(lambda x: format_number(x, 0) if (isinstance(x, inst) and x is not None) else None)
    df_result['opt_price']=df_result['opt_price'].apply(lambda x: f"${x}" if x else "")
    df_result['strat_price']=df_result['__strat_price'].apply(lambda x: format_number(x, 0) if (isinstance(x, inst) and x is not None) else None)
    df_result['strat_price']=df_result['strat_price'].apply(lambda x: f"${x}" if x else "")
    df_result['critical_price']=df_result['__critical_price'].apply(lambda x: format_number(x, 0) if (isinstance(x, inst) and x is not None) else None)
    df_result['critical_price']=df_result['critical_price'].apply(lambda x: f"${x}" if x else "")
    df_result['base_price']=df_result['__base_price'].apply(lambda x: format_number(x, 0) if (isinstance(x, inst) and x is not None) else None)
    df_result['base_price']=df_result['base_price'].apply(lambda x: f"${x}" if x else "")
    return df_result

def apply_pvp_format(df_result):
    df_result['__pvp_margin']=df_result['__pvp_margin'].apply(lambda x: float(back_to_format(x))/100.0 if isinstance(x, str) else x)
    df_result['__recommended_pvp'] = df_result.apply(lambda row: pull_pvp(row['brand_code'], row['product_state'],
                                                                      row['__strat_price'], row['avg_weight'],
                                                                      row['units_x_product'], row['__pvp_margin']), axis=1) #1.19*df_result['__strat_price']/(1-df_result['__pvp_margin'])
    df_result['recommended_pvp']=df_result['__recommended_pvp'].apply(lambda x: format_number(x, 0) if (isinstance(x, (int, float)) and x is not None) else None)
    df_result['recommended_pvp']=df_result['recommended_pvp'].apply(lambda x: f"${x}" if x else "")
    df_result['pvp_margin']=df_result['__pvp_margin'].apply(lambda x: format_number(x*100, 1))
    return df_result

def apply_tooltip_format(df_result, sim=False):
    if not sim:
        df_result["tooltip_sellin"] = pd.to_datetime(df_result["start_sellin"]).dt.strftime("%d/%b/%Y") + " - " + pd.to_datetime(df_result["end_sellin"]).dt.strftime("%d/%b/%Y")
        df_result["tooltip_sellout"] = pd.to_datetime(df_result["start_sellout"]).dt.strftime("%d/%b/%Y") + " - " + pd.to_datetime(df_result["end_sellout"]).dt.strftime("%d/%b/%Y")
        df_result["tooltip_base_pxu"] = df_result['tooltip_base_pxu'].apply(lambda x: format_number(x, 0))
        df_result["tooltip_base_sp"] = df_result['tooltip_base_sp'].apply(lambda x: format_number(x, 0))
        df_result["tooltip_current_pxu"] = df_result['tooltip_current_pxu'].apply(lambda x: format_number(x, 0))
        df_result["tooltip_current_sp"] = df_result['tooltip_current_sp'].apply(lambda x: format_number(x, 0))
        df_result["tooltip_optimization_pxu"] = df_result['tooltip_optimization_pxu'].apply(lambda x: format_number(x, 0))
        df_result["tooltip_optimization_sp"] = df_result['tooltip_optimization_sp'].apply(lambda x: format_number(x, 0))
    df_result["tooltip_strat_vol_kg"] = df_result['__tooltip_strat_vol_kg'].apply(lambda x: format_number(x, 1) if x else '-')
    df_result["tooltip_strategic_pxu"] = df_result['__tooltip_strategic_pxu'].apply(lambda x: format_number(x, 0))
    df_result["tooltip_strategic_sp"] = df_result['__tooltip_strategic_sp'].apply(lambda x: format_number(x, 0))
    return df_result

def apply_strat_based_values(df_result):
    df_result["strat_vol"]=df_result.apply(lambda x: format_number(x['__strat_vol'], 1, x['brand_code']), axis=1)

    df_result['var']=df_result["__strat_vol"]/df_result["__curr_vol"]-1
    df_result['var']=df_result['var'].apply(lambda x: format_number(x*100, 1))
    df_result['var']=df_result['var'].apply(lambda x: f"{x}%" if x else "")

    df_result['discount']=1-df_result['__strat_price']/df_result['__base_price']
    df_result['discount']=df_result['discount'].apply(lambda x: format_number(x*100, 1))
    df_result['discount']=df_result['discount'].apply(lambda x: f"{x}%" if x else "")

    df_result['__var_eb']=df_result['__strat_price']/df_result['__base_price']-1
    df_result['var_eb']=df_result['__var_eb'].apply(lambda x: format_number(x*100, 1))
    df_result['var_eb']=df_result['var_eb'].apply(lambda x: f"{x}%" if x else "")
    return df_result

def format_grouped_rows(grouped_df):
    grouped_df["curr_vol"]=grouped_df.apply(lambda x: format_number(x['__curr_vol'], 1, x['brand_code']), axis=1)
    grouped_df["opt_vol"]=grouped_df.apply(lambda x: format_number(x['__opt_vol'], 1, x['brand_code']), axis=1)
    grouped_df["strat_vol"]=grouped_df.apply(lambda x: format_number(x['__strat_vol'], 1, x['brand_code']), axis=1)
    grouped_df["strat_ro_pct"]=grouped_df["__strat_ro_pct"].apply(lambda x: format_number(x*100, 1) if x else None)
    grouped_df["strat_ro_pct"]=grouped_df["strat_ro_pct"].apply(lambda x: f"{x}%" if x else "")
    grouped_df["incr_ro"]=grouped_df["__incr_ro"].apply(lambda x: format_number(x, 0) if x else None)
    grouped_df["incr_ro"]=grouped_df["incr_ro"].apply(lambda x: f"${x}" if x else "")
    grouped_df["elasticity"]=grouped_df["__elasticity"].apply(lambda x: format_number(x*100, 1) if x else None)
    grouped_df["elasticity"]=grouped_df["elasticity"].apply(lambda x: f"{x}%" if x else "")
    return grouped_df

def pull_family_products_grouped_rows(df):
    family_grouped_df = df.groupby(['brand', 'family', 'brand_code']).agg({
        '__curr_vol': 'sum',
        '__opt_vol': 'sum',
        '__strat_vol': 'sum',
        '__strat_ro_pct': 'mean',
        '__incr_ro': 'mean',
        '__elasticity': 'mean',
        # 'brand_': 'brand',
        # 'family_': 'family',
    }).reset_index()
    family_grouped_df=format_grouped_rows(family_grouped_df)

    return_fields=["brand", "family", "brand_code", "curr_vol", "opt_vol", "strat_vol", "strat_ro_pct", "incr_ro", "elasticity"]
    grouped_summary = family_grouped_df[return_fields].groupby('brand').apply(lambda group: group.to_dict(orient='records')).to_dict()

    brand_grouped_df = df.groupby(['brand', 'brand_code']).agg({
        '__curr_vol': 'sum',
        '__opt_vol': 'sum',
        '__strat_vol': 'sum',
        '__strat_ro_pct': 'mean',
        '__incr_ro': 'mean',
        '__elasticity': 'mean',
        # 'brand_': 'brand',
        # 'family_': 'family',
    }).reset_index()
    brand_grouped_df=format_grouped_rows(brand_grouped_df)

    return_fields=["brand", "brand_code", "curr_vol", "opt_vol", "strat_vol", "strat_ro_pct", "incr_ro", "elasticity"]
    brand_grouped_summary = brand_grouped_df[return_fields].groupby('brand').apply(lambda group: group.to_dict(orient='records')).to_dict()
    summary=[{key: {"families": {item["family"]: item for item in value}, "brand": brand_grouped_summary[key][0]}} for key, value in grouped_summary.items()]

    return summary

    
# def pull_data_rows(df):

def pull_product_view(offer_id):
    # df_catalog = pull_catalog_offer_products_coincidence(offer_id)
    # df_catalog["current_offer"]=0
    # df_catalog["id"]=-1

    df_offer = pull_offer_family_products(offer_id)
    df_offer["current_offer"]=1

    # df_offer_same_month = pull_offer_family_samemonth_products(offer_id)
    # df_offer_same_month["current_offer"]=0
    # df_offer_same_month["id"]=-1

    # df_result = pd.concat([df_offer, df_catalog, df_offer_same_month], ignore_index=True).reset_index(drop=True)
    df_result = pd.concat([df_offer], ignore_index=True).reset_index(drop=True)
    #df_result = df_result.sort_values(by='product_code')

    df_result['direct_cost'].fillna(1e-8, inplace=True)
    df_result['oc_pesos_kilos'].fillna(1e-8, inplace=True)
    df_result['oc_adim_sale'].fillna(1e-8, inplace=True)

    df_result["__curr_vol"]=df_result["curr_vol"]
    df_result["__opt_vol"]=df_result["opt_vol"]
    df_result["__strat_vol"]=df_result["strat_vol"]
    df_result["__base_price"]=df_result["base_price"]
    df_result["__curr_price"]=df_result["curr_price"]
    df_result["__opt_price"]=df_result["opt_price"]
    df_result["__strat_price"]=df_result["strat_price"]
    df_result["__critical_price"]=df_result["critical_price"]
    df_result["__pvp_margin"]=df_result["customer_margin"]
    df_result.rename(columns={'customer_margin': 'pvp_margin'}, inplace=True)

    df_result.rename(columns={'active': 'active_office'}, inplace=True)

    df_result["__elasticity"]=df_result["elasticity"]
    df_result["__tooltip_strategic_pxu"]=df_result['tooltip_strategic_pxu']
    df_result["__tooltip_strategic_sp"]=df_result['tooltip_strategic_sp']
    df_result["__tooltip_strat_vol_kg"]=df_result['tooltip_strat_vol_kg']

    df_result["elasticity"]=df_result['elasticity'].apply(lambda x: format_number(x*100, 1) if isnan(x) else '-')
    df_result['elasticity']=df_result['elasticity'].apply(lambda x: f"{x}%" if x else "")

    df_result["curr_vol"]=df_result.apply(lambda x: format_number(x['curr_vol'], 1, x['brand_code']), axis=1)
    df_result["opt_vol"]=df_result.apply(lambda x: format_number(x['opt_vol'], 1, x['brand_code']), axis=1)

    df_result=apply_strat_based_values(df_result)

    df_result['__var_ob']=df_result['__opt_price']/df_result['__base_price']-1
    df_result['var_ob']=df_result['__var_ob'].apply(lambda x: format_number(x*100, 1))
    df_result['var_ob']=df_result['var_ob'].apply(lambda x: f"{x}%" if x else "")

    # indicators to validate
    df_result['__strat_ro_pct']=df_result.apply(pull_strat_ro_pct, axis=1)
    df_result['strat_ro_pct']=df_result['__strat_ro_pct'].apply(lambda x: f"{format_number(x*100, 1)}%" if x else "")

    df_result['__strat_ro_price']=df_result.apply(pull_ro_strat_price, axis=1)
    df_result['strat_ro_price']=df_result['__strat_ro_price'].apply(lambda x: f"${format_number(x, 0)}" if x else "")

    df_result['__opt_ro_price']=df_result.apply(pull_ro_opt_price, axis=1)
    df_result['__curr_ro_price']=df_result.apply(pull_ro_curr_price, axis=1)
    df_result['curr_ro_price']=df_result['__curr_ro_price'].apply(lambda x: f"${format_number(x, 0)}" if x else "")

    df_result['__incr_ro']=df_result['__strat_ro_price']-df_result['__curr_ro_price']
    df_result['incr_ro']=df_result['__incr_ro'].apply(lambda x: f"${format_number(x, 0)}" if x else "")
    ########################

    df_result=apply_price_format(df_result)
    df_result=apply_pvp_format(df_result)
    df_result=apply_tooltip_format(df_result)

    df_result["promotional_variables_json"] = df_result["promotional_variables_json"].apply(lambda x: loads(x))

    df_result.drop(columns=["start_sellin", "end_sellin", "start_sellout", "end_sellout"], inplace=True)
    df_result['strat_price_modified']=0
    df_result['ever_modified']=0

    print(df_result[['product_code', "distributor_name"]].head())
    

    # df_data_rows=pull_data_rows(df_result)


    header = pull_family_products_header(df_result)
    summary = pull_family_products_summary(df_result)
    grouped_rows = pull_family_products_grouped_rows(df_result)

    #grouped_products_json = loads(pull_grouped_rows(df_result))

    products_json = df_result.to_json(orient='records')
    products_json = loads(products_json)

    # data_rows = pull_data_rows(grouped_products_json, df_result)
    data_rows = pull_data_rows_v2(df_result)
    
    #data_summary = pull_data_summary(df_result)

    del df_result
    del df_offer
    # del df_catalog

    return {
        "offer_data": pull_offer_data(offer_id),
        "header": header,
        "data_rows": data_rows,
        #"grouped_data": loads(df_result_grouped),
        "summary": summary,
        "grouped_rows": grouped_rows,
    }   


def pull_product_optimization_view(product_code, customer, offer_id):
    query = f"""SELECT d.*, p.brand_code
                FROM detalles_optimizacion_sellout d
                JOIN pb_product_list p ON d.codigo_producto=p.code_product
                WHERE d.codigo_producto="{product_code}" AND d.cadena="{customer}";"""
    outcome = pull_outcome_query(query)
    response= {}
    brand_code=None
    for r in outcome:
        brand_code=r["brand_code"]
        elasticidad_lineal = r[2]
        elasticidad_logaritmica = r[3],
        elasticidad_log_log = r[4]
        elasticidad_logaritmo_cuadratico = r[5]
        ind_lineal = r[6]
        ind_log = r[7]
        ind_log_log = r[8]
        ind_log_log_cuadratico = r[9]
        mape_lineal = r[10]
        mape_log = r[11]
        mape_log_log = r[12]
        mape_logcuad = r[13]
        precio_actual = r[14]
        precio_optimizado = r[15]
        volumen_actual = r[16]
        volumen_optimizado = r[17]

        if ind_lineal==1:
            mape=mape_lineal
            mape_name="Elast. Lineal"
            elasticidad=elasticidad_lineal
            other_mape_names=["Log-Cuad", "Log-Log", "Log"]
            other_mape_values=[to_number_format(mape_logcuad,1), to_number_format(mape_log_log,1), to_number_format(mape_log,1)]
        elif ind_log==1:
            mape=mape_log
            mape_name="Log"
            elasticidad=elasticidad_logaritmica
            other_mape_names=["Log-Cuad", "Log-Log", "Elast. Lineal"]
            other_mape_values=[to_number_format(mape_logcuad,1), to_number_format(mape_log_log,1), to_number_format(mape_lineal,1)]
        elif ind_log_log==1:
            mape=mape_log_log
            mape_name="Log-Log"
            elasticidad=elasticidad_log_log
            other_mape_names=["Log-Cuad", "Log", "Elast. Lineal"]
            other_mape_values=[to_number_format(mape_logcuad,1), to_number_format(mape_log,1), to_number_format(mape_lineal,1)]
        elif ind_log_log_cuadratico==1:
            mape=mape_logcuad
            mape_name="Log-Cuad"
            elasticidad=elasticidad_logaritmo_cuadratico
            other_mape_names=["Log-Log", "Log", "Elast. Lineal"]
            other_mape_values=[to_number_format(mape_log_log,1), to_number_format(mape_log,1), to_number_format(mape_lineal,1)]
        
        response={
            "mape_winner_name": mape_name,
            "mape": to_number_format(mape,1),
            "elasticity": to_number_format(elasticidad,1),
            "base_price": to_number_format(round(precio_actual), 0),
            "base_volume": format_number(volumen_actual, 1, brand_code),
            "opt_price": to_number_format(round(precio_optimizado), 0),
            "opt_volume": format_number(volumen_optimizado, 1, brand_code),
            "other_mape_names": other_mape_names,
            "other_mape_values": other_mape_values
        }

    query = f"""SELECT p.CODIGO_MARCA, p.PESO_PROMEDIO,
                    p.PESO_PROMEDIO/p.UNIDAD_X_PRODUCTO,
                    p.UNIDAD_X_PRODUCTO, p.ESTADO_PRODUCTO,
                    m.NOMBRE_LINEA, m.NOMBRE_FAM_PRODUCCION,
                    -- ROUND((p.PESO_PROMEDIO/p.UNIDAD_X_PRODUCTO)*1000,1) as peso
                    (p.PESO_PROMEDIO/p.UNIDAD_X_PRODUCTO)*1000 AS peso_gramos,
                    v.COSTO_DIRECTO_UMP, 
                    v.COSTO_PROYECTADO,
                    v.SALAS, 
                    v.CATALOGACION,
                    CASE WHEN v.CREC_INTERANUAL IS NOT NULL THEN v.CREC_INTERANUAL ELSE '0' END AS CREC_INTERANUAL,
                    v.DIAS_INVENTARIO
                FROM pf_producto p  
                LEFT JOIN  maestra_productos m ON p.CODIGO_PRODUCTO=m.CODIGO_PRODUCTO
                LEFT JOIN vista_producto_moderno v ON v.CODIGO_PRODUCTO=m.CODIGO_PRODUCTO
                WHERE p.CODIGO_PRODUCTO="{product_code}" AND v.CADENA="{customer}";"""

    outcome = pull_outcome_query(query)
    # print(query)
    for r in outcome:
        costo_directo, costo_proyectado, salas, catalogacion, crec_interanual, dias_inventario=r[8:]

        response["brand"]=r[0]
        response["peso_caja"]=to_number_format(r[1], 1)
        response["peso_unidad"]=to_number_format(round(r[7]), 0)#round(r[2],1)
        response["unidades"]=round(r[3])
        response["formato_venta"]=r[4]
        response["linea"]=r[5]
        response["familia"]=r[6]
        response["costo_directo"]=to_number_format(round(float(costo_directo)), 0) if costo_directo else "-"
        response["costo_proyectado"]=to_number_format(round(float(costo_proyectado)), 0) if costo_proyectado else "-"
        response["salas"]=salas
        response["catalogacion"]=to_number_format(catalogacion*100, 1)
        response["crec_interanual"]=to_number_format((float(crec_interanual)-1)*100,1)+"%" if crec_interanual is not None else "-"
        response["dias_inventario"]=f"{dias_inventario} dias"

    query = f"""select codigo_producto, cadena,
                SUM(CASE WHEN tipo_estrategia = 1 then precio_estrategico else 0 end) as precio_est_1 , SUM(CASE WHEN tipo_estrategia = 1 then volumen_estrategico else 0 end) as volumen_est_1 ,
                SUM(CASE WHEN tipo_estrategia = 2 then precio_estrategico else 0 end) as precio_est_2 , SUM(CASE WHEN tipo_estrategia = 2 then volumen_estrategico else 0 end) as volumen_est_2 ,
                SUM(CASE WHEN tipo_estrategia = 3 then precio_estrategico else 0 end) as precio_est_3, SUM(CASE WHEN tipo_estrategia = 3 then volumen_estrategico else 0 end) as volumen_est_3,
                nombre_estrategia, tipo_estrategia
                from oferta_moderno_base
                WHERE codigo_producto='{product_code}' AND cadena='{customer}'
                group by codigo_producto, cadena;"""
    outcome = pull_outcome_query(query)
    for r in outcome:
        response["precio_est_1"]=to_number_format(round(r[2]), 0)
        response["volumen_est_1"]=format_number(r[3], 1, brand_code)
        response["precio_est_2"]=to_number_format(round(r[4]), 0)
        response["volumen_est_2"]=format_number(r[5], 1, brand_code)
        response["precio_est_3"]=to_number_format(round(r[6]), 0)
        response["volumen_est_3"]=format_number(r[7], 1, brand_code)
        response["nombre_estrategia"]=r["nombre_estrategia"]
        response["tipo_estrategia"]=r["tipo_estrategia"]


    query = f"""SELECT p.name_promotion AS promotion_name, ppl.code_product AS product_code, 
                    'Catalog' AS `type`, p.start_sellin, p.end_sellin, p.start_sellout, p.end_sellout,
                    pl.proposed_price AS price, (1-(pl.proposed_price/pl.base_price))*100 AS discount,
                    pl.current_volume_proposed AS volume, pl.recommend_pvp,
                    case when ppl.brand_code<=6 then (pl.proposed_price/pl.critical_price-1)*100
                    ELSE (pl.proposed_price/pl.critical_price*(ppl.avg_weight/ppl.units_x_product)-1)*100 END AS ro_prcnt
                FROM promotion p
                JOIN customer c ON p.id_customer=c.id
                JOIN promotion_line pl ON p.id=pl.id_promotion
                JOIN pb_product_list ppl ON pl.id_product=ppl.id
                WHERE p.id_customer = (SELECT distributors_id FROM pb_promotion WHERE id={offer_id}) AND 
                p.year_month_01_str = (SELECT year_month01_str FROM pb_promotion WHERE id={offer_id}) 
                    AND ppl.code_product='{product_code}' AND 1-(pl.proposed_price/pl.base_price)>0
                    AND p.id_promotional_state in (14,18,19)
                UNION ALL
                SELECT p.promotion_name, pl.product_code, 'Offer' AS `type`, 
                    p.start_sellin, p.end_sellin, p.start_sellout, p.end_sellout,
                    pl.strategic_price AS price, (1-(pl.strategic_price/pl.base_price))*100 AS discount,
                    pl.strategic_volume AS volume, 1.19*pl.strategic_price/(1-pl.customer_margin) AS recommended_pvp,
                    case when pl.brand_code<=6 then (pl.strategic_price/pl.critical_price-1)*100
                    ELSE (pl.strategic_price/pl.critical_price*(pl.avg_weight/pl.units_x_product)-1)*100 END AS ro_prcnt	
                FROM pb_promotion_product pl
                JOIN pb_promotion p ON pl.promotion_id=p.id
                WHERE p.year_month01_str=(SELECT year_month01_str FROM pb_promotion WHERE id={offer_id}) 
                    AND p.id<>{offer_id} AND 1-(pl.strategic_price/pl.base_price)>0 AND pl.product_code='{product_code}'
                    AND p.promotional_state_id in (14,18,19);"""
    outcome = pull_outcome_query(query)
    activations=[]
    date_format="%d/%m/%y"
    for r in outcome:
        activations.append({
            "name": r["name"],
            "type": r["type"],
            "sellout": r["start_sellout"].strftime(date_format) + " - " + r["end_sellout"].strftime(date_format),
            "sellin": r["start_sellin"].strftime(date_format) + " - " + r["end_sellin"].strftime(date_format),
            "price": to_number_format(round(r["price"]), 0),
            "discount": to_number_format(r["discount"], 1),
            "volume": format_number(r[9], 1, brand_code),
            "recommended_pvp": to_number_format(round(r["recommended_pvp"]), 0),
            "ro_prcnt": to_number_format(r["ro_prcnt"], 1)
        })
    response["activations"]=activations
    #offer_id to add activaciones

    return response

def parse_save_dict(d):
    product={
        'strategic_price': d["__strat_price"],
        'customer_margin': d["__customer_margin"],
        'tooltip_strategic_pxu': d["__tooltip_strategic_pxu"],
        'tooltip_strategic_sp': d["__tooltip_strategic_sp"],
        'strategic_volume_kg': d["__tooltip_strat_vol_kg"],
        'strategic_volume': d["__strat_vol"],
        'promotional_variables_json': dumps(d['promotional_variables_json']),
        'eb_variation': d["__var_eb"],
        'ob_variation': d["__var_ob"],
        'recommended_pvp': d["__recommended_pvp"],
        'id': d["id"],
    }
    #to_update=(d["__strat_price"], d["__customer_margin"], d["__tooltip_strategic_pxu"], d["__tooltip_strategic_sp"], f"'{dumps(d['promotional_variables_json'])}'")
    #return to_update
    #d.pop('product_rows')
    return product


def save_offer(offer_id, data_rows):
    offer_products = [parse_save_dict(prod) for prod in data_rows]
    metadata = MetaData(bind=db.engine)
    promotion_product_table = Table('pb_promotion_product', metadata, autoload_with=db.engine)
    with db.engine.begin() as connection:
        for data in offer_products:
            update_stmt = (
                update(promotion_product_table)
                .where(promotion_product_table.c.id == data['id'])
                .values({
                    'strategic_price': data['strategic_price'],
                    'customer_margin': data['customer_margin'],
                    'tooltip_strategic_pxu': data["tooltip_strategic_pxu"],
                    'tooltip_strategic_sp': data["tooltip_strategic_sp"],
                    'strategic_volume_kg': data["strategic_volume_kg"],
                    'strategic_volume': data["strategic_volume"],
                    'promotional_variables_json': data['promotional_variables_json'],
                })
            )

            try:
                connection.execute(update_stmt)
                msg=f"offer id {offer_id} successfully updated"
                upd=True
            except exc.SQLAlchemyError as e:
                msg=f"error on offer update: {e}"
                upd=False
                # raise
    #update offer updated_at
    tmp=pull_timestamp()
    query = f"UPDATE pb_promotion SET updated_at='{tmp}' WHERE id={offer_id};"
    e=query_execute_w_err(query)
    print("update updated_At:", e)
    
    return msg, upd

def simulator_lambda(row, customer):
    if row["strat_price_modified"]==1:
        sim_vars=offer_simulator(row["product_code"], customer, row["__strat_price"])
        row["__strat_vol"]=sim_vars["strat_volume"]
        row["__tooltip_strategic_pxu"]=sim_vars["strat_up"]
        row["__tooltip_strategic_sp"]=sim_vars["strat_sp"]
        row["__tooltip_strat_vol_kg"]=sim_vars["strat_volume_kg"]

        row["__strat_ro_price"]=sim_vars["strat_ro"]
        row['strat_ro_price']=f"{format_number(row['__strat_ro_price'], 0)}" if row['__strat_ro_price'] else "-"
        #df_result['__strat_ro_price']=df_result.apply(pull_ro_strat_price, axis=1)
        #df_result['strat_ro_price']=df_result['__strat_ro_price'].apply(lambda x: f"${format_number(x, 0)}" if x else "")
    return row

def flatten_data_rows(data_rows):
    flat_data_rows=[]
    for data_row in data_rows:
        product_rows = data_row.pop("product_rows")
        flat_data_rows.append(data_row)
        flat_data_rows.extend(product_rows)
    return flat_data_rows


def simulator_handler(offer_id, data_rows, offer_data):
    data_rows = [simulator_lambda(data, offer_data["customer"]) for data in data_rows]
    flatten_data=flatten_data_rows(data_rows)

    df_result=pd.DataFrame(flatten_data)
    df_result=apply_strat_based_values(df_result)
    df_result=apply_price_format(df_result)
    df_result=apply_pvp_format(df_result)
    df_result=apply_tooltip_format(df_result, True)
    df_result.loc[df_result['strat_price_modified'] == 1, 'ever_modified'] = 1
    df_result["strat_price_modified"]=0

    header = pull_family_products_header(df_result)
    summary = pull_family_products_summary(df_result)
    grouped_rows = pull_family_products_grouped_rows(df_result)
    products_json = df_result.to_json(orient='records')
    products_json = loads(products_json)
    data_rows = pull_data_rows_v2(df_result)

    del df_result

    return {
        "offer_data": offer_data,
        "header": header,
        "data_rows": data_rows,
        "summary": summary,
        "grouped_rows": grouped_rows
    } 

def delete_offer_product(offer_product_id):
    query = f"DELETE FROM pb_promotion_product WHERE id={offer_product_id};"
    e=query_execute_w_err(query)
    if e:
        return f"error on delete offer product: {str(e)}", 400
    return "offer product deleted successfully", 200

def clean_brand_tracking_df_active(row):
    if row["base_price"] - row["strategic_price"] > 0:
        return 1
    else:
        return 0

def clean_brand_tracking_df_discount(row):
    return 1-row['strategic_price']/row['base_price']


def clean_brand_tracking_df(df):
    df_cleaned = df[["base_price", "current_price", "strategic_price", "product_code", "current_volume", "strategic_volume"]].copy()
    df_cleaned["active"] = df_cleaned.apply(clean_brand_tracking_df_active, axis=1)
    df_cleaned["discount"] = df_cleaned.apply(clean_brand_tracking_df_discount, axis=1)
    return df_cleaned

def update_brand_tracking_fields(row):
    if row["discount"]>row["discount_col"]:
        row["icon_col"]="check"
    elif row["discount"]>0 and row["discount_col"]==0:
        row["icon_col"]="x"
    else:
        row["icon_col"]="-"

    if row["discount_col"]>row["discount_neg"]:
        row["icon_neg"]="check"
    elif row["discount_neg"]>0 and row["discount_neg"]==0:
        row["icon_neg"]="x"
    else:
        row["icon_neg"]="-"

    if row["discount_neg"]>row["discount_eje"]:
        row["icon_eje"]="check"
    elif row["discount_neg"]>0 and row["discount_eje"]==0:
        row["icon_eje"]="x"
    else:
        row["icon_eje"]="-"
    
    return row

def pull_brand_tracking_header(df):
    active_ini = df["active"].sum()
    active_col = df["active_col"].sum()
    active_neg = df["active_neg"].sum()
    active_eje = df["active_eje"].sum()
    
    header = {
        "collaborative": f"{format_number(active_col*100/active_ini, 1)}% ({active_col}/{active_ini})",
        "negotiation": f"{format_number(active_neg*100/active_col, 1)}% ({active_neg}/{active_col})",
        "execution": f"{format_number(active_eje*100/active_neg, 1)}% ({active_eje}/{active_neg})",
    }
    return header


        # products_line_tracking.append({
        #     "product_id": id_product,
        #     "product_code": code_product,
        #     "short_brand": short_brand,
        #     "subfamily": subfamily,
        #     "opt_price":  to_number_format(round(opt_price)),
        #     "base_price":  to_number_format(round(base_price)),
        #     "act_price": to_number_format(round(inicial_curr_price)),
        #     "product_description": current_description_product,
        #     "model": model,
        #     "recommendation": recommendation,
        #     "category": category,
        #     "subcategory": subcategory,
        #     "inicial": {
        #         "prop_price": to_number_format(round(inicial_prop_price)) if inicial_prop_price is not None else 0,
        #         "discount": inicial_discount,
        #     },
        #     "colaborativa": {
        #         "prop_price": to_number_format(round(colab_prop_price)) if colab_prop_price is not None else 0,
        #         "discount": colab_discount,
        #         "icon": colab_icon
        #     },
        #     "negociacion": {
        #         "prop_price": to_number_format(round(neg_prop_price)) if neg_prop_price is not None else 0,
        #         "discount": neg_discount,
        #         "icon": neg_icon
        #     },
        #     "ejecucion": {
        #         "prop_price": to_number_format(round(ejec_prop_price)) if ejec_prop_price is not None else 0,
        #         "discount": ejec_discount,
        #         "icon": ejec_icon
        #     },
        # })

def pull_brand_tracking_data_rows(df):
    products_line_tracking = []
    for index, row in df[(df["discount"] > 0) | (df["discount_col"] > 0) | (df["discount_neg"] > 0) | (df["discount_eje"] > 0)].iterrows():
        #id_product = row["id"]

        products_line_tracking.append({
            #"product_id": id_product,
            "product_code": row["product_code"],
            "short_brand": row["short_brand"],
            "subfamily": row["subfamily"],
            "base_price":  format_number(row["base_price"], 0),
            "curr_price": format_number(row["current_price"], 0),
            "opt_price":  format_number(row["optimization_price"], 0),
            "product_description": row["product_description"],
            "model": row["model"],
            "strategy": row["strategy_name"],
            "brand": row["brand"],
            "family": row["family"],
            "initial": {
                "strat_price": f'${format_number(row["strategic_price"], 0)}' if row["strategic_price"] is not None else "$-",
                "discount": f'{format_number(row["discount"]*100, 1)}%',
            },
            "collaborative": {
                "strat_price": f'${format_number(row["strategic_price_col"], 0)}' if row["strategic_price_col"] is not None else "$-",
                "discount": f'{format_number(row["discount_col"]*100, 1)}%',
                "icon": row["icon_col"]
            },
            "negotiation": {
                "strat_price": f'${format_number(row["strategic_price_neg"], 0)}' if row["strategic_price_neg"] is not None else "$-",
                "discount": f'{format_number(row["discount_neg"]*100, 1)}%',
                "icon": row["icon_neg"]
            },
            "execution": {
                "strat_price": f'${format_number(row["strategic_price_eje"], 0)}' if row["strategic_price_eje"] is not None else "$-",
                "discount": f'{format_number(row["discount_eje"]*100, 1)}%',
                "icon": row["icon_eje"]
            },
        })

    return products_line_tracking

def pull_brand_tracking_summary(offer_id):
    query = f"""SELECT pph.*
                FROM pb_promotion_product_historic pph
                JOIN (
                    SELECT
                        promotion_id,
                        product_code,
                        MAX(promotionalstate_id) as promotionalstate_id
                    FROM
                        pb_promotion_product_historic
                    GROUP BY
                        promotion_id, product_code
                ) AS max_fases
                ON
                    pph.promotion_id = max_fases.promotion_id
                    AND pph.product_code = max_fases.product_code
                    AND pph.promotionalstate_id = max_fases.promotionalstate_id
                WHERE pph.promotion_id={offer_id} AND pph.base_price-pph.strategic_price>0;"""
    df=pull_dataframe_from_sql(query)
    df['__strat_vol']=df['strategic_volume']
    df['__opt_vol']=df['optimization_volume']
    df['__curr_vol']=df['current_volume']
    df['__base_price']=df['base_price']
    df['curr_price']=df['current_price']
    df['opt_price']=df['optimization_price']
    df['strat_price']=df['strategic_price']
    df['__strat_ro_price']=df.apply(pull_ro_strat_price, axis=1)
    df['__opt_ro_price']=df.apply(pull_ro_opt_price, axis=1)
    df['__curr_ro_price']=df.apply(pull_ro_curr_price, axis=1)
    summary = pull_family_products_summary(df)
    return summary

def pull_brand_tracking(offer_id):
    query_ini = f'SELECT * FROM pb_promotion_product_historic WHERE promotionalstate_phase="INICIAL" AND promotion_id={offer_id};'
    query_col = f'SELECT * FROM pb_promotion_product_historic WHERE promotionalstate_phase="COLABORATIVA" AND promotion_id={offer_id};'
    query_neg = f'SELECT * FROM pb_promotion_product_historic WHERE promotionalstate_phase="NEGOCIACIÓN" AND promotion_id={offer_id};'
    query_eje = f'SELECT * FROM pb_promotion_product_historic WHERE promotionalstate_phase="EJECUCIÓN" AND promotion_id={offer_id};'
    df_inicial = pull_dataframe_from_sql(query_ini)
    df_colaborativa = pull_dataframe_from_sql(query_col)
    df_negociacion = pull_dataframe_from_sql(query_neg)
    df_ejecucion = pull_dataframe_from_sql(query_eje)

    df_bt=df_inicial[["strategy_name", "model", "product_code", "product_description", "brand", "family", "subfamily", "short_brand", "base_price", "current_price", "optimization_price"]]

    df_inicial = clean_brand_tracking_df(df_inicial)
    df_colaborativa = clean_brand_tracking_df(df_colaborativa)
    df_negociacion = clean_brand_tracking_df(df_negociacion)
    df_ejecucion = clean_brand_tracking_df(df_ejecucion)


    df_bt = pd.merge(df_bt, df_inicial, on='product_code', how='left', suffixes=('', '_ini'))
    df_bt = pd.merge(df_bt, df_colaborativa, on='product_code', how='left', suffixes=('', '_col'))
    df_bt = pd.merge(df_bt, df_negociacion, on='product_code', how='left', suffixes=('', '_neg'))
    df_bt = pd.merge(df_bt, df_ejecucion, on='product_code', how='left', suffixes=('', '_eje'))
    df_bt = df_bt.apply(update_brand_tracking_fields, axis=1)
    
    df_bt_header = pull_brand_tracking_header(df_bt)

    data_rows = pull_brand_tracking_data_rows(df_bt)

    # products_json = df_bt.to_json(orient='records')
    # products_json = loads(products_json)
    del df_bt
    del df_inicial
    del df_colaborativa
    del df_negociacion
    del df_ejecucion

    summary=pull_brand_tracking_summary(offer_id)
    return {
        "offer_data": pull_offer_data(offer_id),
        "data_rows": data_rows,
        "header": df_bt_header,
        "summary": summary
        # "summary": {
        #     'strat_op_res': "$-",
        #     'strat_ben': "$-",
        #     'opt_op_res': "$-",
        #     'opt_ben': "$-",
        # }
        # "header": {
        #     "initial_prop": "",
        #     "collaborative_prop": "",
        #     "negotiation_prop": "",
        #     "execution_prop": "",
        # },
    }
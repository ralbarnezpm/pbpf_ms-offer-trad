from json import loads
from api.controllers.utils import pull_dataframe_from_sql, pull_outcome_query


def pull_offices():
    query  = """SELECT o.id, o.office_name, o.office_code, og.group_name
                FROM office o JOIN office_group og ON o.id_office_group=og.id;"""
    df = pull_dataframe_from_sql(query)
    json_data_pandas = df.to_json(orient='records')
    return loads(json_data_pandas) 

def pull_brands():
    query  = """SELECT distinct category, category_name 
                FROM pb_product_list;"""
    df = pull_dataframe_from_sql(query)
    json_data_pandas = df.to_json(orient='records')
    return loads(json_data_pandas) 

def created_promotion_offices(month, year, brand_id):
    """ Returns the offices that have a promotion created for the given year, month and category"""

    query = f"""SELECT distributors_id
                FROM pb_promotion p WHERE channel_id=2 AND promotion_type_id=2 AND year_str='{year}' AND month_str='{month}' AND brand_id={brand_id};"""
    df = pull_dataframe_from_sql(query)
    print(query)
    dist = df["distributors_id"].tolist()
    dist_set=set()
    [dist_set.update(d.split(",")) for d in dist]
    
    dist_list = list(dist_set) # remove duplicates by converting to set then back to list
    print("dist_list:", dist_list)
    return dist_list

def distributors_name(office_code_list):
    """ Returns the offices that have a promotion created for the given year, month and category"""

    query = f"""SELECT office_name
                FROM office WHERE office_code IN ({office_code_list});"""
    df = pull_dataframe_from_sql(query)
    names = df["office_name"].tolist()
    print("names:", names)
    return ",".join(names)

def check_created_promotion_offices(year, month, category, offices_id):
    """ Returns the offices that have a promotion created for the given year, month and category"""
    offices=created_promotion_offices(year, month, category)
    if len(offices)==0:
        return offices_id
    # offices=offer_offices[0]["distributors_id"].split(",")
    # offices=[int(x) for x in offices]
    # offices_wnames=offer_offices[0]["distributors_name"].split(",")
    # print("created_offices:", offices)
    # print("offices_id:", offices_id)
    if set(offices)==set(offices_id):
        offices_id=[]
    else:
        offices_id=list(set(offices_id)-set(offices))
    print("new offices:", offices_id)
    return offices_id



def pull_catalog_offer_products_coincidence(offer_id):
    query = f"""SELECT DISTINCT pl.current_volume_sold AS curr_vol,
                    pl.current_volume_optimization AS opt_vol,
                    pl.current_volume_proposed AS strat_vol,
                    
                    pl.base_price,
                    pl.current_price AS curr_price,
                    pl.current_optimization_price AS opt_price,
                    pl.proposed_price AS strat_price,
                    pl.critical_price,
                    
                    pl.customer_margin,
                    pl.promotional_variables_json,
                    
                    p.name_promotion AS promotion_name,
                    p.id AS promotion_id,
                    ppl.code_product AS product_code,
                    ppl.description AS product_description,
                    ppl.units_x_product,
                    ppl.avg_weight,
                    ppl.brand_code,
                    ppl.short_brand,
                    ppl.category_name AS brand,
                    ppl.subcategory_name AS family,
                    ppl.product_state,
                    ppl.subfamily,

                    0.001 AS oc_adim,
                    0.001 AS oc_adim_sale,
                    0.001 AS oc_pesos,
                    0.001 AS oc_pesos_kilos,
                    0.001 AS direct_cost,
                    
                    mct.pxu_base AS tooltip_base_pxu,
                    mct.pv_base AS tooltip_base_sp,
                    mct.pxu_actual AS tooltip_current_pxu,
                    mct.pv_actual AS tooltip_current_sp,
                    mct.pxu_optimo AS tooltip_optimization_pxu,
                    mct.pv_optimo AS tooltip_optimization_sp,
                    mct.pxu_propuesto AS tooltip_strategic_pxu,
                    mct.pv_propuesto AS tooltip_strategic_sp,

                    pl.start_sellin,
                    pl.end_sellin,
                    pl.start_sellout,
                    pl.end_sellout,

                    pl.elasticity,
                    '' as strategy_name,
                    case when pl.no_model=0 then 1 else 0 end as model
                FROM promotion p
                JOIN customer c ON p.id_customer=c.id
                JOIN promotion_line pl ON p.id=pl.id_promotion
                JOIN pb_product_list ppl ON pl.id_product=ppl.id
                JOIN tooltips_moderno mct ON mct.codigo_producto=ppl.code_product AND mct.cadena=c.name_customer 
                    AND mct.fecha=(SELECT year_month01_str FROM pb_promotion WHERE id={offer_id})
                WHERE p.id_customer = (SELECT distributors_id FROM pb_promotion WHERE id={offer_id}) AND 
                    p.year_month_01_str = (SELECT year_month01_str FROM pb_promotion WHERE id={offer_id}) AND
                    ppl.code_product IN (SELECT DISTINCT o.codigo_producto FROM pb_promotion_product p 
                        JOIN oferta_moderno_base o ON p.product_id=o.id_pb WHERE p.promotion_id={offer_id}
                );"""
    df = pull_dataframe_from_sql(query)
    print(query)
    df["type"] = "catalog"
    return df




from api.controllers.traditional.simulator import offer_simulator
from api.controllers.system import query_execute_w_err
from api.controllers.utils import pull_dataframe_from_sql, upsert_massive_load
from api.routes.utils import pull_timestamp
import pandas as pd


def pull_product_data(product_codes, offer_id):
    query = f"""SELECT
                    p.id AS promotion_id,
                    o.id_pb AS product_id,
                    o.codigo_producto AS product_code,
                    pl.description AS product_description,
                    'Moderno' AS promotion_channel,
                    t.type_promotion AS promotion_type_name,
                    4 AS recommendation_id,
                    'Carga Masiva' AS recommendation_name,
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
                    p.start_sellin,
                    p.end_sellin,
                    p.start_sellout,
                    p.end_sellout,
                    1 AS promotionalstate_id,
                    1 AS promotionalstate_phase,
                    o.pxu_estrategico AS tooltip_strategic_pxu,
                    o.pv_estrategico AS tooltip_strategic_sp,
                    o.pxu_optimo AS tooltip_optimization_pxu,
                    o.pv_optimo AS tooltip_optimization_sp,
                    o.pxu_actual AS tooltip_current_pxu,
                    o.pv_actual AS tooltip_current_sp,
                    o.pxu_base AS tooltip_base_pxu,
                    o.pv_base AS tooltip_base_sp,
                    pl.short_brand,
                    c.category AS brand,
                    s.subcategory AS family,
                    pl.subfamily,
                    o.nombre_estrategia AS strategy_name,
                    o.elasticidad AS elasticity,
                    CASE WHEN ppp.product_id IS NOT NULL THEN 'SI' ELSE 'NO' END AS on_offer
                FROM
                    pb_promotion p
                JOIN
                    oferta_moderno_base o ON p.distributors_name = o.cadena
                JOIN
                    pb_product_list pl ON pl.code_product = o.codigo_producto
                JOIN
                    category c ON pl.category = c.id
                JOIN
                    subcategory s ON pl.subcategory = s.id
                JOIN
                    type_promotion t ON p.promotion_type_id = t.id
                LEFT JOIN
                    pb_promotion_product ppp on ppp.product_id = o.id_pb
                WHERE p.id={offer_id} AND o.codigo_producto IN ({product_codes}) and o.nombre_estrategia = 'Optimizacion';"""
    return pull_dataframe_from_sql(query)
    


def bulk_update_promotion_products(file, user_id, offer_id):
    timestamp_actualizacion = pull_timestamp()
    if file and file.filename.endswith('.xlsx'):
        # Lee el archivo Excel
        try:
            df = pd.read_excel(file)
            df = df.rename(columns={
                'Producto': 'product_code',
                'Cadena': 'customer',
                'Precio': 'prop_price',
            })

            print(df.head())

            product_codes = df['product_code'].tolist()
            product_codes_str = ", ".join(f"'{code}'" for code in product_codes)
            customer_products = pull_product_data(product_codes_str, offer_id)


            df = df.merge(customer_products, on='product_code', how='left', suffixes=('_massive', ''))
            print(df.info())

            df['price_error']=df['prop_price'].apply(lambda x: 1 if (x < 0 or not isinstance(x, (int, float)) or x is None) else 0)

            df3=df.apply(lambda x: pd.Series(offer_simulator(x['product_code'], x['customer'], x['prop_price'])), axis=1)

            response_json = {'message': 'Archivo procesado correctamente'}
            response_json['data_rows'] = {
                'products_validated': len(df),
                'products_on_offer': len(df.loc[df['on_offer'] == 'SI']),
                'products_not_on_offer': len(df.loc[df['on_offer'] == 'NO']),
                'products_with_price_error': len(df.loc[df['price_error'] == 1]),
            }


            df3.columns = df3.columns + '_simulation'
            df = df.merge(df3, left_on='product_code', right_on='product_code_simulation',how='left', suffixes=('', '_simulated'))

            df.rename(columns={
                'recommend_pvp_simulation': 'recommend_pvp', #pvp recomendado
                'roskg_simulation': 'ro_price_kg', #ro precio propuesto
                'ros_simulation': 'ro_price', #ro precio propuesto
                }, inplace=True)

            df['strategic_volume']=df['strat_volume_simulation']
            df['strategic_volume_kg']=df['strat_volume_kg_simulation']
            df['tooltip_strategic_sp']=df['strat_sp_simulation']
            df['tooltip_strategic_pxu']=df['strat_up_simulation']
            df['strat_prop_ro']=df['strat_ro_simulation']

            df.drop(columns=[
                'product_code_simulation', 
                'strat_volume_simulation', 
                'strat_volume_kg_simulation',
                'strat_sp_simulation', 
                'strat_up_simulation', 
                'strat_ro_simulation',
                'strat_base_variation_simulation',
            ], inplace=True)
            df['strategy_name']='Carga Masiva'

            print(df.head())

            df_mltosql=df.copy()
            # df_mltosql['offer_id'] = offer_id
            df_mltosql['user_id'] = user_id
            df_mltosql['timestamp_actualizacion'] = timestamp_actualizacion
            upsert_massive_load(df_mltosql, 'moffer_massive_load')

            return response_json, df[['product_code', 'on_offer', 'price_error']], True
        except Exception as e:
            return {'message': f'Error al procesar el archivo Excel: {str(e)}'}, pd.DataFrame(), False
    else:
        print('El archivo no es un archivo Excel (.xlsx)')


def insert_massive_load_to_promotion_products(user_id, offer_id):

    try:
        query = f"""INSERT INTO pb_promotion_product (product_code, promotion_id, product_id, product_description, promotion_channel, promotion_type_name, recommendation_id, recommendation_name, current_volume, optimization_volume, strategic_volume, base_price, current_price, optimization_price, strategic_price, critical_price, oc_adim, oc_adim_sale, oc_pesos, oc_pesos_kilos, product_state, brand_code, units_x_product, avg_weight, start_sellin, end_sellin, start_sellout, end_sellout, promotionalstate_id, promotionalstate_phase, tooltip_strategic_pxu, tooltip_strategic_sp, tooltip_optimization_pxu, tooltip_optimization_sp, tooltip_current_pxu, tooltip_current_sp, tooltip_base_pxu, tooltip_base_sp, short_brand, brand, family, subfamily, strategy_name, elasticity, recommended_pvp, ro_price_kg, strat_prop_ro, strategic_volume_kg)
                    SELECT
                        product_code,
                        promotion_id,
                        product_id,
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
                        tooltip_base_pxu,
                        tooltip_base_sp,
                        short_brand,
                        brand,
                        family,
                        subfamily,
                        strategy_name,
                        elasticity,
                        recommend_pvp,
                        ro_price_kg,
                        strat_prop_ro,
                        strategic_volume_kg
                    FROM
                        moffer_massive_load
                    WHERE
                        user_id = {user_id} AND promotion_id = {offer_id}
                    ON DUPLICATE KEY UPDATE
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
                        avg_weight = VALUES(avg_weight),
                        tooltip_strategic_pxu = VALUES(tooltip_strategic_pxu),
                        tooltip_strategic_sp = VALUES(tooltip_strategic_sp),
                        tooltip_optimization_pxu = VALUES(tooltip_optimization_pxu),
                        tooltip_optimization_sp = VALUES(tooltip_optimization_sp),
                        tooltip_current_pxu = VALUES(tooltip_current_pxu),
                        tooltip_current_sp = VALUES(tooltip_current_sp),
                        tooltip_base_pxu = VALUES(tooltip_base_pxu),
                        tooltip_base_sp = VALUES(tooltip_base_sp),
                        short_brand = VALUES(short_brand),
                        strategy_name = VALUES(strategy_name),
                        elasticity = VALUES(elasticity),
                        recommended_pvp = VALUES(recommended_pvp),
                        ro_price_kg = VALUES(ro_price_kg),
                        ro_price_kg = VALUES(ro_price_kg),
                        strategic_volume_kg = VALUES(strategic_volume_kg);"""
        # print(query)
        err=query_execute_w_err(query)
        if err:
            return {'message': f'Error al insertar la carga masiva a la tabla pb_promotion_product: {str(err)}', "allowed": True}, False
        else:
            return {'message': 'Carga masiva insertada correctamente', "allowed": True}, True
    except Exception as e:
        return {'message': f'Error al procesar el archivo Excel: {str(e)}', "allowed": True}, False


from api.controllers.utils import query_execute_w_err


def pull_last_phase_id(promotional_state_id):
    if promotional_state_id<=3:
        last_phase_id=3
    elif promotional_state_id>3 and promotional_state_id <=9:
        last_phase_id=9
    elif promotional_state_id>9 and promotional_state_id <=13:
        last_phase_id=13
    elif promotional_state_id>13 and promotional_state_id <=14:
        last_phase_id=14
    return last_phase_id

def promotion_line_screenshot(promotion_id, promotionalstate_id):
    query = f"""INSERT INTO `pb_promotion_product_historic` (`id`, `promotion_id`, `product_id`, `product_code`, `product_description`, `promotion_channel`, `promotion_type_name`, `recommendation_id`, `recommendation_name`, `current_volume`, `optimization_volume`, `strategic_volume`, `proposed_volume`, `variation_volume`, `base_price`, `current_price`, `optimization_price`, `proposed_price`, `strategic_price`, `variation_current_opt_price`, `variation_base_proposed_price`, `variation_proposed_current_price`, `discount`, `proposed_ro`, `customer_margin`, `recommended_pvp`, `critical_price`, `oc_adim`, `oc_adim_sale`, `oc_pesos`, `oc_pesos_kilos`, `direct_cost`, `product_state`, `brand_code`, `units_x_product`, `avg_weight`, `start_sellin`, `end_sellin`, `start_sellout`, `end_sellout`, `promotional_variables_json`, `promotionalstate_id`, `promotionalstate_phase`, `promotionalstate_state`, `active`, `model`, `on_edit`, `brand`, `short_brand`, `family`, `subfamily`, `strategy_name`, `eb_variation`, `ob_variation`, `elasticity`, `tooltip_base_pxu`, `tooltip_base_sp`, `tooltip_current_pxu`, `tooltip_current_sp`, `tooltip_optimization_pxu`, `tooltip_optimization_sp`, `tooltip_strategic_pxu`, `tooltip_strategic_sp`)
                SELECT `id`, `promotion_id`, `product_id`, `product_code`, 
                    `product_description`, `promotion_channel`, `promotion_type_name`, 
                    `recommendation_id`, `recommendation_name`, `current_volume`, 
                    `optimization_volume`, `strategic_volume`, `proposed_volume`, 
                    `variation_volume`, `base_price`, `current_price`, `optimization_price`, 
                    `proposed_price`, `strategic_price`, `variation_current_opt_price`, 
                    `variation_base_proposed_price`, `variation_proposed_current_price`, 
                    `discount`, `proposed_ro`, `customer_margin`, `recommended_pvp`, 
                    `critical_price`, `oc_adim`, `oc_adim_sale`, `oc_pesos`, `oc_pesos_kilos`, 
                    `direct_cost`, `product_state`, `brand_code`, `units_x_product`, 
                    `avg_weight`, `start_sellin`, `end_sellin`, `start_sellout`, `end_sellout`, 
                    `promotional_variables_json`, {promotionalstate_id} as `promotionalstate_id`, `promotionalstate_phase`, 
                    `promotionalstate_state`, `active`, `model`, `on_edit`, `brand`, `short_brand`, 
                    `family`, `subfamily`, `strategy_name`, `eb_variation`, `ob_variation`, 
                    `elasticity`, `tooltip_base_pxu`, `tooltip_base_sp`, `tooltip_current_pxu`, 
                    `tooltip_current_sp`, `tooltip_optimization_pxu`, `tooltip_optimization_sp`, 
                    `tooltip_strategic_pxu`, `tooltip_strategic_sp` 
                FROM pb_promotion_product WHERE promotion_id={promotion_id}
                ON DUPLICATE KEY UPDATE
                promotion_id=VALUES(promotion_id),
                product_id=VALUES(product_id),
                product_code=VALUES(product_code),
                strategic_volume=VALUES(strategic_volume),
                proposed_volume=VALUES(proposed_volume),
                variation_volume=VALUES(variation_volume),
                proposed_price=VALUES(proposed_price),
                strategic_price=VALUES(strategic_price),
                variation_current_opt_price=VALUES(variation_current_opt_price),
                variation_base_proposed_price=VALUES(variation_base_proposed_price),
                variation_proposed_current_price=VALUES(variation_proposed_current_price),
                discount=VALUES(discount),
                proposed_ro=VALUES(proposed_ro),
                customer_margin=VALUES(customer_margin),
                recommended_pvp=VALUES(recommended_pvp),
                critical_price=VALUES(critical_price),
                start_sellin=VALUES(start_sellin),
                end_sellin=VALUES(end_sellin),
                start_sellout=VALUES(start_sellout),
                end_sellout=VALUES(end_sellout),
                promotional_variables_json=VALUES(promotional_variables_json),
                promotionalstate_id=VALUES(promotionalstate_id),
                promotionalstate_phase=VALUES(promotionalstate_phase),
                promotionalstate_state=VALUES(promotionalstate_state),
                active=VALUES(active),
                model=VALUES(model),
                on_edit=VALUES(on_edit),
                brand=VALUES(brand),
                short_brand=VALUES(short_brand),
                family=VALUES(family),
                subfamily=VALUES(subfamily),
                strategy_name=VALUES(strategy_name),
                eb_variation=VALUES(eb_variation),
                ob_variation=VALUES(ob_variation),
                elasticity=VALUES(elasticity),
                tooltip_strategic_pxu=VALUES(tooltip_strategic_pxu),
                tooltip_strategic_sp=VALUES(tooltip_strategic_sp);"""
    e = query_execute_w_err(query)
    return e




# def update_screenshot(promotion, promotional_state_id, promotion_catalog_id):
def update_screenshot(promotion_id, promotional_state_id):
    try:
        #last_phase_id=pull_last_phase_id(promotional_state_id)
        promotion_line_screenshot(promotion_id, promotional_state_id)

    except Exception as e:
        print("error creating the screenshot:\n", e)
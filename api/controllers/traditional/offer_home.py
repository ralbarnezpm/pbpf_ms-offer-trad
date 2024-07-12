

from api.controllers.comment import create_promotion_comment
from api.controllers.traditional.comment import pull_comment_comment, pull_comment_state_update
from api.controllers.traditional.notification import create_notification
from api.controllers.traditional.promotion import pull_promotion_by_id
from api.controllers.traditional.screenshot import update_screenshot
from api.controllers.utils import pull_outcome_query
from api.models.pb_promotion_product import PBPromotionProduct
from api.routes.utils import pull_timestamp
from api.utils import format_number
from api.extensions import db


def listall_offer():
    query="""SELECT DISTINCT p.id as offer_id, p.promotion_name, p.start_sellin, p.end_sellin,
                p.start_sellout, p.end_sellout, p.month_str AS promotion_month,
                p.year_str AS promotion_year, p.distributors_name AS customer,
                tpm.type_promotion AS promotion_type, p.created_at, p.updated_at,
                ps.id, ps.phase_str AS phase, ps.state_phase_str AS state,
                CASE WHEN ps.id IN (2,6,8,11,16,17) AND DATEDIFF(CURDATE(),p.updated_at) > 60 THEN 0
                WHEN ps.id IN (18) THEN 0 ELSE 1 END AS show_promo,
                prod.*
            FROM pb_promotion p 
            JOIN pb_promotion_product pl ON p.id=pl.promotion_id
            JOIN promotional_state ps ON p.promotional_state_id=ps.id
            JOIN type_promotion tpm ON tpm.id=p.promotion_type_id
            JOIN `channel` c ON c.id=p.channel_id
            LEFT JOIN (

            SELECT  -- SUM(vol_act) AS sum_vol_act, SUM(vol_opt) AS sum_vol_opt, SUM(vol_prop) AS sum_vol_prop, 
            ROUND(SUM(ro_opt),1) AS ro_opt, ROUND(SUM(ro_opt)-SUM(ro_act),1) AS ben_opt, ROUND(SUM(ro_strat)-SUM(ro_act),1) AS ben_strat, 
            ROUND(SUM(ro_strat),1) AS ro_prop, ROUND(SUM(ro_act),1) AS ro_act, promotion_id, 
            CASE 
                WHEN (SUM(n_actives)/COUNT(*))*100 > 0 AND (SUM(n_actives)/COUNT(*))*100 < 4.5 THEN 1 
                WHEN (SUM(n_actives)/COUNT(*))*100 > 4.5 AND (SUM(n_actives)/COUNT(*))*100 < 14.5 THEN 2 
                WHEN (SUM(n_actives)/COUNT(*))*100 > 14.5 AND (SUM(n_actives)/COUNT(*))*100 < 29.5 THEN 3
                WHEN (SUM(n_actives)/COUNT(*))*100 > 29.5 THEN 4
                ELSE 1
            END AS color,
            ROUND((SUM(n_actives)/COUNT(*))*100, 1) AS activation
            -- SUM(n_actives), COUNT(*) AS total_products 
            FROM (
            SELECT pl.promotion_id,
                CASE
                WHEN pl.brand_code>6 THEN (pl.strategic_price*pl.units_x_product/pl.avg_weight-pl.critical_price)*(pl.strategic_volume*pl.avg_weight/pl.units_x_product)
                ELSE (pl.strategic_price-pl.critical_price)*pl.strategic_volume
                END AS ro_strat,
                CASE
                WHEN pl.brand_code>6 THEN (pl.optimization_price*pl.units_x_product/pl.avg_weight-pl.critical_price)*(pl.optimization_volume*pl.avg_weight/pl.units_x_product)
                ELSE (pl.optimization_price-pl.critical_price)*pl.optimization_volume
                END AS ro_opt,
                CASE
                WHEN pl.brand_code>6 THEN (pl.current_price*pl.units_x_product/pl.avg_weight-pl.critical_price)*(pl.current_volume*pl.avg_weight/pl.units_x_product)
                ELSE (pl.current_price-pl.critical_price)*pl.current_volume
                END AS ro_act, -- pl.strategic_volume AS vol_prop, pl.optimization_volume AS vol_opt, pl.current_volume AS vol_act,
                CASE
                WHEN pl.base_price - pl.strategic_price > 0 THEN 1
                ELSE 0
                END AS n_actives
            FROM pb_promotion_product pl
            ) t
            GROUP BY t.promotion_id
            ) prod ON p.id=prod.promotion_id;"""
    outcome = pull_outcome_query(query)
    promotions = []
    for row in outcome:
        row=dict(row)
        if int(row['show_promo']) == 1:
            row["start_sellin"]=row["start_sellin"].strftime("%d/%m/%Y")
            row["end_sellin"]=row["end_sellin"].strftime("%d/%m/%Y")
            row["start_sellout"]=row["start_sellout"].strftime("%d/%m/%Y")
            row["end_sellout"]=row["end_sellout"].strftime("%d/%m/%Y")
            row["created_at"]=row["created_at"].strftime("%d/%m/%Y")
            row["updated_at"]=row["updated_at"].strftime("%d/%m/%Y")
            row["show_promo"]=1 if row["show_promo"]==1 else 0
            row["activation"]=format_number(row["activation"], 1)
            row["ro_act"]=format_number(row["ro_act"], 0)
            row["ro_prop"]=format_number(row["ro_prop"], 0)
            row["ro_opt"]=format_number(row["ro_opt"], 0)
            row["ben_strat"]=format_number(row["ben_strat"], 0)
            row["ben_opt"]=format_number(row["ben_opt"], 0)
            promotions.append(dict(row))
    return promotions

def pull_offer_current_products(offer_id):
    query = f"""SELECT p.product_code, p.product_description, p.strategy_name, p.product_id AS offer_product_id
                FROM pb_promotion_product p WHERE p.promotion_id={offer_id};"""
    outcome = pull_outcome_query(query)
    products = []
    for row in outcome:
        row=dict(row)
        products.append(dict(row))
    return products

def pull_id_phase_phase_str(offer_id):
    query = f"""SELECT ps.id, ps.phase_str
                FROM pb_promotion p
                LEFT JOIN promotional_state ps ON p.id_promotional_state=ps.id
                WHERE p.id={offer_id};"""
    result = pull_outcome_query(query)
    phase_id, phase_str = None, None
    for r in result:
        phase_id, phase_str = r
    return phase_id, phase_str


def pull_state_phase_str(promotional_state_id):
    query = f"""SELECT ps.state_phase_str, ps.phase_str, ps.id
                FROM promotional_state ps
                WHERE ps.id={promotional_state_id};"""
    result = pull_outcome_query(query)
    state_phase_str, phase_str, phase_id = None, None, None
    for r in result:
        state_phase_str, phase_str, phase_id = r
    return state_phase_str, phase_str, phase_id


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

def update_offer_phase(promotional_state_id, offer_id, update_comment, user_id):
    """ Updates promotion phase """

    #old_phase_id, old_phase_str=pull_id_phase_phase_str(offer_id)
    #promotional_state_id = promotional_state_id+1 if promotional_state_id in (3, 9, 13, 18) else promotional_state_id
    state_str, phase_str, phase_id = pull_state_phase_str(promotional_state_id)
    promotion = pull_promotion_by_id(offer_id)

    comment=pull_comment_state_update(user_id, promotion.promotion_name, phase_str, state_str)
    if not comment:
        return {"response": "Error trying to update the promotional state"}, 400
    
    try:
        screenshot_ps_id=pull_last_phase_id(promotional_state_id)

        promotional_state_id = promotional_state_id+1 if promotional_state_id in (3, 9, 13, 18) else promotional_state_id
        db.session.query(PBPromotionProduct).filter_by(promotion_id=offer_id).update(dict(promotionalstate_id=promotional_state_id, promotionalstate_phase=phase_str, promotionalstate_state=state_str))
        promotion.promotional_state_id=promotional_state_id
        promotion.updated_at=pull_timestamp()
        db.session.commit()

        
        update_screenshot(offer_id, screenshot_ps_id)
        
        created, err, new_comment_id=create_promotion_comment(offer_id, user_id, comment, promotion.promotional_state_id)
        if created:
            notification_created, err = create_notification(1, "Movimiento", new_comment_id, user_id)
            print("notification_created:", notification_created)

        else:
            return {"response": "error trying to create the comment notification", "error": str(err)}
        
        if update_comment:
            comment=pull_comment_comment(user_id, promotion.promotion_name, update_comment)
            created, err, new_comment_id=create_promotion_comment(offer_id, user_id, comment, promotion.promotional_state_id)
            if created:
                notification_created, err = create_notification(2, "Comentario", new_comment_id, user_id)
                print("update comment notification_created:", notification_created)
        return "Promotional state successfully updated!", None
    
    except Exception as e:
        db.session.rollback()
        return "Error trying to update the promotional state", str(e)

    # return {"response": "Error trying to update the promotional state"}, 400 
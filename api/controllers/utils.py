from datetime import datetime, timedelta

from pytz import timezone
from ..extensions import db
import pandas as pd

########################################################################
#   SQLAlchemy Utils
########################################################################

def pull_outcome_query(query):  
    outcome=None
    try:
        outcome = db.session.execute(query).all()
        db.session.commit()
    except Exception as e:
        print(e)
        db.session.rollback()
    # finally:
    #     db.session.close()
    return outcome

def query_execute(query):  
    try:
        db.engine.execute(query)
        db.session.commit()
        db.session.flush()
        return True
    except Exception as e:
        print(e)
        db.session.rollback()
    # finally:
    #     db.session.close()
    return None

def query_execute_w_err(query):  
    try:
        db.engine.execute(query)
        db.session.commit()
        return None
    except Exception as e:
        print(e)
        db.session.rollback()
        return e

def manage_session(f):
    def inner(*args, **kwargs):

        # MANUAL PRE PING
        try:
            db.session.execute("SELECT 1;")
            db.session.commit()
        except:
            db.session.rollback()
        finally:
            db.session.close()

        # SESSION COMMIT, ROLLBACK, CLOSE
        try:
            res = f(*args, **kwargs)
            db.session.commit()
            return res
        except Exception as e:
            db.session.rollback()
            raise e
            # OR return traceback.format_exc()
        finally:
            db.session.close()
    return inner

########################################################################
#   Utils
########################################################################


def pull_dataframe_from_sql(query):
    engine = db.engine
    df = pd.read_sql(query, engine)
    return df

def upsert_massive_load(df, table_name):
    engine = db.engine
    columns = ["product_code", "customer", "prop_price", "promotion_id", "product_id", "product_description", "promotion_channel", "promotion_type_name", "recommendation_id", "recommendation_name", "current_volume", "optimization_volume", "strategic_volume", "base_price", "current_price", "optimization_price", "strategic_price", "critical_price", "oc_adim", "oc_adim_sale", "oc_pesos", "oc_pesos_kilos", "product_state", "brand_code", "units_x_product", "avg_weight", "start_sellin", "end_sellin", "start_sellout", "end_sellout", "promotionalstate_id", "promotionalstate_phase", "tooltip_strategic_pxu", "tooltip_strategic_sp", "tooltip_optimization_pxu", "tooltip_optimization_sp", "tooltip_current_pxu", "tooltip_current_sp", "tooltip_base_pxu", "tooltip_base_sp", "short_brand", "brand", "family", "subfamily", "strategy_name", "elasticity", "on_offer", "price_error", "error_simulation", "recommend_pvp", "ro_price_kg", "strat_prop_ro", "user_id", "strategic_volume_kg","timestamp_actualizacion"]

    try:

        base_query = f'INSERT INTO {table_name} (`product_code`, `customer`, `prop_price`, `promotion_id`, `product_id`, `product_description`, `promotion_channel`, `promotion_type_name`, `recommendation_id`, `recommendation_name`, `current_volume`, `optimization_volume`, `strategic_volume`, `base_price`, `current_price`, `optimization_price`, `strategic_price`, `critical_price`, `oc_adim`, `oc_adim_sale`, `oc_pesos`, `oc_pesos_kilos`, `product_state`, `brand_code`, `units_x_product`, `avg_weight`, `start_sellin`, `end_sellin`, `start_sellout`, `end_sellout`, `promotionalstate_id`, `promotionalstate_phase`, `tooltip_strategic_pxu`, `tooltip_strategic_sp`, `tooltip_optimization_pxu`, `tooltip_optimization_sp`, `tooltip_current_pxu`, `tooltip_current_sp`, `tooltip_base_pxu`, `tooltip_base_sp`, `short_brand`, `brand`, `family`, `subfamily`, `strategy_name`, `elasticity`, `on_offer`, `price_error`, `error_simulation`, `recommend_pvp`, `ro_price_kg`, `strat_prop_ro`, `user_id`, `strategic_volume_kg`, `timestamp_actualizacion`) VALUES '

        value_tuples = []
        for _, row in df[columns].iterrows():
            value_tuples.append(tuple(map(str, row.values)))

        values_part = ', '.join([f'({", ".join(map(repr, values))})' for values in value_tuples])


        sql_query = f"""
                {base_query}
                {values_part} 
                ON DUPLICATE KEY UPDATE 
                    `prop_price` = VALUES(`prop_price`), 
                    `product_id` = VALUES(`product_id`), 
                    `strategic_volume` = VALUES(`strategic_volume`),
                    `strategic_volume_kg` = VALUES(`strategic_volume_kg`),
                    `tooltip_strategic_sp` = VALUES(`tooltip_strategic_sp`),
                    `tooltip_strategic_pxu` = VALUES(`tooltip_strategic_pxu`),
                    `strat_prop_ro` = VALUES(`strat_prop_ro`),
                    `recommend_pvp` = VALUES(`recommend_pvp`),
                    `ro_price_kg` = VALUES(`ro_price_kg`),
                    `ro_price` = VALUES(`ro_price`),
                    `timestamp_actualizacion` = VALUES(`timestamp_actualizacion`)
                    """
        engine.execute(sql_query)

    except Exception as e:
        print(f"Error al procesar el DataFrame: {str(e)}")


def get_months(n, include_past=False):
    current_date = datetime.now()
    months = []

    for i in range(-n, n + 1):
        date = current_date + timedelta(days=i * 30)

        if include_past or date >= current_date:
            months.append(date.strftime("%B - %Y"))

    return months

def dataframe_to_sql(df, table_name, engine, if_exists='append'):
    """
    Insert a Pandas DataFrame into a SQL database table using to_sql.

    Parameters:
    - df: Pandas DataFrame
    - table_name: str, nombre de la tabla en la base de datos
    - database_url: str, URL de conexión a la base de datos (ej. 'mysql://user:password@localhost:3306/database')
    - if_exists: {'fail', 'replace', 'append'}, comportamiento si la tabla ya existe

    Returns:
    - None
    """
    try:
        df.to_sql(name=table_name, con=engine, if_exists=if_exists, index=False)
    except Exception as e:
        raise RuntimeError(f"error inserting DataFrame into table '{table_name}': {str(e)}")


def date_format(date):
    try:
        return date.strftime("%d/%m/%Y")
    except:
        return ""
        raise

def datetime_format(datetime):
    try:
        return datetime.strftime("%d/%m/%Y | %H:%M")
    except:
        return ""
        raise

def datetime_now():
    return datetime.now(timezone('Chile/Continental'))

# n = 6
# include_past = False
# result = get_months(n, include_past)
# for month in result:
#     print(month)
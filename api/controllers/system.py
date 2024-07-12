from ..extensions import db

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


def query_brand_filter(brand_filter):
    if brand_filter=="all":
        brand_filter_str=""
    else:
        brand_filter_str=f' AND pr.short_brand in ("{brand_filter}")'
    return brand_filter_str

def query_subfamily_filter(brand_filter):
    if brand_filter=="all":
        brand_filter_str=""
    else:
        brand_filter_str=f' AND pr.subfamily in ("{brand_filter}")'
    return brand_filter_str


def dict_keys_with_commas(dictionary):
    keys = []
    for element in dictionary:
        keys=list(element.keys())
        break
    
    return ",".join(keys)


def dict_values_with_commas(dictionary):
    values = []
    for element in dictionary:
        element_values = list(element.values())
        values.append( "(" + ",".join(map(str, element_values)) + ")" + " \n") 
    
    return ",".join(values)

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
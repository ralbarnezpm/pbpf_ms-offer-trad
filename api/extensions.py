from flask_sqlalchemy import SQLAlchemy
from os import environ
from json import dumps
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv("./.env"))

db = SQLAlchemy()
user = environ.get("DB_USER")
password = environ.get("DB_PASS")
host = environ.get("DB_HOST")
port = environ.get("DB_PORT")
database = environ.get("DB_NAME")

db_uri = f"mariadb+mariadbconnector://{user}:{password}@{host}:{port}/{database}"

admin_users_id=[1,3,5]

def update_promotion_line(session, Object, object_id, promotion_type,**kwargs):
    """promotion_type: 1->moderno, 2->tradicional"""
    if promotion_type==1:
        table_name='promotion_line'
    elif promotion_type==2:
        table_name='traditional_promotion_line'


    print("------------------>")
    promotional_variables_json=kwargs["promotional_variables_json"]
    print("updating traditional promotion line...", object_id)

    del kwargs["promotional_variables_json"]
    keys=list(kwargs.keys())
    values=list(kwargs.values())
    set_str=""
    for i in range(len(keys)):
        set_str += f"{keys[i]} = {values[i]}, "
    set_str += f"promotional_variables_json = '{promotional_variables_json}'"
    query = f"""UPDATE {table_name}
                SET {set_str}
                WHERE id={object_id};"""
    try:
        session.execute(query)
        session.commit()
    except Exception as e:
        print(e)
        session.rollback()
    print("------------------<")


def editObjectPL(session, Object, object_id, **kwargs):
    object = session.query(Object).filter_by(id = object_id).first()
    for attr, value in kwargs.items():
        #print("(attr, value):", "(", attr, ":", value, ")")
        if value:
            if value=="active":
                object.active=value
            else:
                setattr(object, attr, value)

    try:
        session.add(object)
        session.commit()
    except Exception as e:
        print(e)
        session.rollback()

    print(object.active)


def editObject(session, Object, object_id, **kwargs):
    object = session.query(Object).filter_by(id = object_id).first()
    for attr, value in kwargs.items():
        #print("(attr, value):", "(", attr, ":", value, ")")
        if value:
            setattr(object, attr, value)

    try:
        session.add(object)
        session.commit()
    except Exception as e:
        print(e)
        session.rollback()
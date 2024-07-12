from jwt import encode, decode
from jwt import exceptions
from datetime import datetime, timedelta
from os import getenv, environ
from functools import wraps
from flask import request, make_response

def verify_token_middleware(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if request.headers.get('Authorization'):
            token = request.headers['Authorization'].split(" ")[1]
        else:
            token=""
            
        v_token = validate_token(token, True)
        if type(v_token) is dict:
            global payload 
            payload = {"email":v_token["email"], "rol":v_token["rol"], "id":v_token["id"]}
            
            return f(payload,*args, **kwargs)
        else:
            return v_token
        
    return decorated

def expire_date(hours: int):
    date = datetime.now()
    new_date = date + timedelta(hours=hours)
    return new_date

def write_token(data: dict):
    token = encode(payload={**data, "exp": expire_date(12), "iat": datetime.now() + timedelta(hours=0)}, key=environ.get("SECRET_KEY"), algorithm="HS256")
    return token

def validate_token(token, output=False):
    try:
        if output:
            return decode(token, key=environ.get("SECRET_KEY"), algorithms=["HS256"])
        decode(token, key=environ.get("SECRET_KEY"), algorithms=["HS256"])
    except exceptions.DecodeError as e:
        print(e)
        return make_response({"message": "Invalid Token"}, 401, {'WWW-Authenticate': 'Basic realm="Login Required"'})
    except exceptions.ExpiredSignatureError as e:
        print(e)
        return make_response({"message": "Token Expired"}, 401, {'WWW-Authenticate': 'Basic realm="Login Required"'})
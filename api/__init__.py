from flask import Flask, g
from flask_cors import CORS
from dotenv import load_dotenv, find_dotenv
from .routes.utils import pull_timestamp
from .extensions import db, db_uri
from .routes.traditional.offer import offer_bp
from .routes.traditional.product import product_bp
# from .routes.traditional.comment import comment_bp

import locale
from pytz import timezone
from datetime import datetime


def set_locale():
    try:
        locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')
    except locale.Error:
        print("Warning: Unable to set 'es_ES.UTF-8' locale. Using default locale.")

def get_current_time():
    if 'current_time' not in g:
        santiago_timezone = timezone('Chile/Continental')
        current_time = datetime.now(santiago_timezone)
        g.current_time = current_time
    return g.current_time

def create_app(testing_config=None):
    app = Flask(__name__, instance_relative_config=True)

    app.config.from_mapping(
        SQLALCHEMY_DATABASE_URI=db_uri,
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
        debug=True,
        PORT=7875
    )
    CORS(app)

    db.init_app(app)

    # app.register_blueprint(test_bp, url_prefix="/ras")
    app.register_blueprint(offer_bp, url_prefix="/offer")
    app.register_blueprint(product_bp, url_prefix="/product")
    # app.register_blueprint(comment_bp, url_prefix="/comment")


    @app.before_request
    def set_current_time():
        g.current_time = get_current_time()
        print(pull_timestamp())

    set_locale()

    return app
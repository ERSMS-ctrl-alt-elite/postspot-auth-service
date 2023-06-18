import os
import logging
from datetime import datetime
from functools import wraps

from flask import Flask, request, jsonify
from flask_swagger_ui import get_swaggerui_blueprint

from postspot.data_gateway import FirestoreGateway, User
from postspot.config import Config
from postspot.auth import decode_openid_token
from postspot.constants import Environment, AccountStatus

# ---------------------------------------------------------------------------- #
#                                   App init                                   #
# ---------------------------------------------------------------------------- #

env = Environment(os.environ["ENV"]) if "ENV" in os.environ else Environment.PRODUCTION

config = Config(env)

# ----------------------------- Configure logging ---------------------------- #
logging.basicConfig(
    level=config.log_level,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ------------------------------- Create an app ------------------------------ #
logger.info(f"Running application in {env.value} environment")
app = Flask("PostSpot Notification Service")
app.secret_key = "PostSpot123"

# -------------------------- Create database gateway ------------------------- #
data_gateway = FirestoreGateway()

# --------------------------- Configure Swagger UI --------------------------- #
SWAGGER_URL = "/swagger"
API_URL = "/static/swagger.json"
SWAGGERUI_BLUEPRINT = get_swaggerui_blueprint(
    SWAGGER_URL, API_URL, config={"app_name": "Seans-Python-Flask-REST-Boilerplate"}
)
app.register_blueprint(SWAGGERUI_BLUEPRINT, url_prefix=SWAGGER_URL)


# ---------------------------------------------------------------------------- #
#                                   Endpoints                                  #
# ---------------------------------------------------------------------------- #


@app.route("/")
def index():
    return "Hello from PostSpot's notification service"


if __name__ == "__main__":
    debug = env != Environment.PRODUCTION
    app.run(debug=debug, port=8080)

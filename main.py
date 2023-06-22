import os
import requests
import logging
from datetime import datetime
from functools import wraps
from concurrent.futures import ThreadPoolExecutor

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
app = Flask("PostSpot Recommendation Service")
app.secret_key = os.environ["RECOMMENDATION_SERVICE_SECRET_KEY"]

# -------------------------- Create database gateway ------------------------- #
data_gateway = FirestoreGateway()

# --------------------------- Configure Swagger UI --------------------------- #
SWAGGER_URL = "/swagger"
API_URL = "/static/swagger.json"
SWAGGERUI_BLUEPRINT = get_swaggerui_blueprint(
    SWAGGER_URL, API_URL, config={"app_name": "PostSpot Recommendation Service"}
)
app.register_blueprint(SWAGGERUI_BLUEPRINT, url_prefix=SWAGGER_URL)

POST_API_URL = os.environ["POST_API_URL"]

def user_signed_up(function):
    @wraps(function)
    def wrapper(*args, **kwargs):
        token = None

        if "Authorization" in request.headers:
            bearer = request.headers.get("X-Forwarded-Authorization")
            token = bearer.split()[1]

        if not token:
            return jsonify({"message": "Token not provided"}), 401

        try:
            (
                google_id,
                name,
                email,
                token_issued_t,
                token_expired_t,
            ) = decode_openid_token(token)

            token_issued_at_datetime = datetime.fromtimestamp(token_issued_t)
            token_exp_datetime = datetime.fromtimestamp(token_expired_t)
            logger.debug(
                f"Token issued at {token_issued_at_datetime} ({token_issued_t})"
            )
            logger.debug(f"Token expires at {token_exp_datetime} ({token_expired_t})")

            try:
                current_user = data_gateway.read_user(google_id)
            except Exception as e:
                logger.error(f"User not signed up: {e}")
                return jsonify({"message": "Invalid token or user not signed up"}), 401
        except Exception as e:
            logger.error(f"Invalid token: {e}")
            return jsonify({"message": "Invalid token or user not signed up"}), 401

        return function(current_user, *args, **kwargs)

    return wrapper


# ---------------------------------------------------------------------------- #
#                                   Endpoints                                  #
# ---------------------------------------------------------------------------- #

@app.route("/v1/recommendations/<user_google_id>", methods=["GET"])
# @user_signed_up
def get_recommendations(user_google_id):
    logging.debug(f"fetching: {POST_API_URL}/v1/users/{user_google_id}/followees")
    r = requests.get(f"{POST_API_URL}/v1/users/{user_google_id}/followees")
    folowees = r.json()["user"]
    logging.debug(f"User follows {len(folowees)} users")
    def get_posts_by_author(author):
        author = author["google_id"]
        logging.debug(f"fetching: {POST_API_URL}/v1/posts?author={author}")
        return requests.get(f"{POST_API_URL}/v1/posts", params={"author": author}).json()

    with ThreadPoolExecutor() as executor:
        posts = executor.map(get_posts_by_author, folowees) 
    return [post for author_posts in posts for post in author_posts], 200

if __name__ == "__main__":
    debug = env != Environment.PRODUCTION
    app.run(debug=debug, port=8082)

import os
import logging
import requests

from google.oauth2 import id_token
import google.auth.transport.requests
from pip._vendor import cachecontrol


logger = logging.getLogger(__name__)


def decode_openid_token(token) -> tuple:
    request_session = requests.session()
    cached_session = cachecontrol.CacheControl(request_session)
    token_request = google.auth.transport.requests.Request(session=cached_session)

    logger.debug(f"{request_session=} {cached_session=} {token_request=}")

    id_info = id_token.verify_oauth2_token(
        id_token=token, request=token_request, audience=os.environ["CLIENT_ID"]
    )

    logger.debug(f"{id_info=}")

    google_id = id_info.get("sub")
    name = id_info.get("name")
    email = id_info.get("email")
    token_issue_t = id_info.get("iat")
    token_expired_t = id_info.get("exp")

    return (google_id, name, email, token_issue_t, token_expired_t)

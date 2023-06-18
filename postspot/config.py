import configparser
import json

from google.cloud import secretmanager

from postspot.constants import Environment


PROJECT_ID = "mystic-stack-382412"

# ---------------------------------------------------------------------------- #
#                                 SecretManager                                #
# ---------------------------------------------------------------------------- #


def access_secret_version(secret_id, version_id="latest"):
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version.
    response = client.access_secret_version(name=name)

    # Return the decoded payload.
    return response.payload.data.decode("UTF-8")


# ---------------------------------------------------------------------------- #
#                                    Config                                    #
# ---------------------------------------------------------------------------- #


class Config:
    def __init__(self, env: Environment):
        self._env = env
        self._config = configparser.ConfigParser()
        self._config.read("config.ini")

    @property
    def log_level(self) -> str:
        return self._config["DEFAULT"]["log_level"]

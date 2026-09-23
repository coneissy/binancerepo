import os
from pathlib import Path

from hummingbot.client.config.config_crypt import ETHKeyFileSecretManger, store_password_verification
from hummingbot.client.config.config_helpers import ClientConfigAdapter, save_to_yml
from hummingbot.client.config.security import Security
from hummingbot.client.settings import AllConnectorSettings, CONNECTORS_CONF_DIR_PATH


def main() -> None:
    password = os.environ["HBOT_PASSWORD"]
    api_key = os.environ["BINANCE_API_KEY"]
    api_secret = os.environ["BINANCE_API_SECRET"]

    Security.secrets_manager = ETHKeyFileSecretManger(password)

    verification_path = Path("/home/hummingbot/conf/.password_verification")
    verification_path.parent.mkdir(parents=True, exist_ok=True)
    if not verification_path.exists():
        store_password_verification(Security.secrets_manager)

    config_model = AllConnectorSettings.get_connector_config_keys("binance_perpetual").__class__(
        connector="binance_perpetual",
        binance_perpetual_api_key=api_key,
        binance_perpetual_api_secret=api_secret,
    )
    config = ClientConfigAdapter(config_model)

    connector_path = Path(CONNECTORS_CONF_DIR_PATH) / "binance_perpetual.yml"
    connector_path.parent.mkdir(parents=True, exist_ok=True)
    save_to_yml(connector_path, config)


if __name__ == "__main__":
    main()

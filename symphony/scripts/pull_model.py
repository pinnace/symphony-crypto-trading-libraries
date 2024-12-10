import os
from symphony.config import AWS_ACCESS_KEY_ID, AWS_REGION, AWS_SECRET_ACCESS_KEY, ML_LOCAL_PATH
import pathlib

version_folder = "latest"

if __name__ == "__main__":
    base_path = pathlib.Path(ML_LOCAL_PATH)
    for strategy in ["DemarkBuySetup", "DemarkBuyCountdown"]:
        strategy_path = base_path / f"{strategy}/"

        cmd = f"AWS_ACCESS_KEY_ID={AWS_ACCESS_KEY_ID} AWS_REGION={AWS_REGION} AWS_SECRET_ACCESS_KEY={AWS_SECRET_ACCESS_KEY} aws s3 sync s3://symphony-trading-ml/{strategy}/ {str(strategy_path)}"
        os.system(cmd)

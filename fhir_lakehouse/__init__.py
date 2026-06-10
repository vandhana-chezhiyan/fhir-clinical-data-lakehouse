from dagster import Definitions, load_assets_from_modules
from . import assets

# Safely scan assets.py and load every function decorated with @asset
all_pipeline_assets = load_assets_from_modules([assets])

# Expose these assets to the Dagster orchestration tool
defs = Definitions(
    assets=all_pipeline_assets,
)
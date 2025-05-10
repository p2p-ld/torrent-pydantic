from pydantic import BaseModel, ConfigDict


class ConfiguredBase(BaseModel):
    """Configured base model"""

    model_config = ConfigDict(populate_by_name=True, extra="allow")

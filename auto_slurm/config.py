from typing import Dict
from pydantic import BaseModel, ConfigDict
from pydantic import PositiveInt
from pydantic import model_validator
from typing_extensions import Self
from typing import Optional


class GeneralConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    hostname_config_mappings: Dict[str, str]
    global_fillers: Dict[str, str]


class Config(BaseModel):
    model_config = ConfigDict(extra="forbid")

    template: str
    default_fillers: Dict[str, str]

    NO_gpus: Optional[PositiveInt]
    max_tasks: Optional[PositiveInt]

    gpus_per_task: Optional[PositiveInt]

    @model_validator(mode="after")
    def check(self) -> Self:
        if self.NO_gpus is not None:
            assert (
                self.max_tasks is None
            ), "NO_gpus and max_tasks cannot be set at the same time"
            assert (
                self.gpus_per_task is not None
            ), "NO_gpus and gpus_per_task should be set at the same time"

        if self.max_tasks is not None:
            assert (
                self.NO_gpus is None
            ), "NO_gpus and max_tasks cannot be set at the same time"
            assert (
                self.gpus_per_task is None
            ), "gpus_per_task and max_tasks cannot be set at the same time"

from dataclasses import asdict, dataclass
from typing import List, Dict

@dataclass
class JobStates:
    env: str 
    trigger_states: List[str] 
    error_states: List[str]
    completed_states: List[str]

    def __init__(
        self, 
        env: str, 
        trigger_states: List[str], 
        error_states: List[str], 
        completed_states: List[str]
    ) -> None:
        self._env = env
        self._trigger_states = trigger_states
        self._error_states = error_states
        self._completed_states = completed_states

    # Property for 'env' with validation
    @property
    def env(self) -> str:
        return self._env

    @env.setter
    def env(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("Environment must be a string")
        self._env = value

    # Property for 'trigger_states' with validation
    @property
    def trigger_states(self) -> List[str]:
        return self._trigger_states

    @trigger_states.setter
    def trigger_states(self, states: List[str]) -> None:
        if not isinstance(states, list):
            raise ValueError("Trigger states must be a list of strings")
        self._trigger_states = states

    # Property for 'error_states' with validation
    @property
    def error_states(self) -> List[str]:
        return self._error_states

    @error_states.setter
    def error_states(self, states: List[str]) -> None:
        if not isinstance(states, list):
            raise ValueError("Error states must be a list of strings")
        self._error_states = states

    # Property for 'completed_states' with validation
    @property
    def completed_states(self) -> List[str]:
        return self._completed_states

    @completed_states.setter
    def completed_states(self, states: List[str]) -> None:
        if not isinstance(states, list):
            raise ValueError("Completed states must be a list of strings")
        self._completed_states = states

    def __repr__(self) -> str:
        return (f"JobStates(env={self.env}, trigger_states={self.trigger_states}, "
                f"error_states={self.error_states}, completed_states={self.completed_states})")
    
    def to_dict(self):
         return asdict(self)
    
    @classmethod
    def from_json(cls, data: Dict[str, any]) -> "JobStates":
        return cls(
            env=data.get("env", ""),
            trigger_states=data.get("trigger_states", []),
            error_states=data.get("error_states", []),
            completed_states=data.get("completed_states", [])
        )


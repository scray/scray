class JobStates:
    def __init__(self, trigger_states=None, error_states=None, completed_states=None):
        self.trigger_states = trigger_states if trigger_states else []
        self.error_states = error_states if error_states else []
        self.completed_states = completed_states if completed_states else []

    def add_trigger_state(self, env, state):
        self.trigger_states.append((env, state))

    def add_error_state(self, env, state):
        self.error_states.append((env, state))

    def add_completed_state(self, env, state):
        self.completed_states.append((env, state))

    def get_trigger_states(self):
        return self.trigger_states

    def get_error_states(self):
        return self.error_states

    def get_completed_states(self):
        return self.completed_states

    def clear_states(self):
        self.trigger_states.clear()
        self.error_states.clear()
        self.completed_states.clear()

    def display_states(self):
        print("Trigger States:", self.trigger_states)
        print("Error States:", self.error_states)
        print("Completed States:", self.completed_states)

    def has_error(self, env=None):
        if env:
            return any(e == env for e, _ in self.error_states)
        return len(self.error_states) > 0

    def is_completed(self, env=None):
        if env:
            return any(e == env for e, _ in self.completed_states)
        return len(self.completed_states) > 0

    def __str__(self):
        """String representation of the StateManager."""
        return (f"Trigger States: {self.trigger_states}\n"
                f"Error States: {self.error_states}\n"
                f"Completed States: {self.completed_states}")

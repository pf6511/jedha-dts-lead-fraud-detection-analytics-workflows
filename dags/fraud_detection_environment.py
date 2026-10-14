import os


class Environment:
    def env_var(self, env_var_name: str) -> str:
        try:
            return os.environ[env_var_name]
        except KeyError:
            raise RuntimeError(
                f"Required environment variable "
                f"{env_var_name!r} is not set"
            ) from None
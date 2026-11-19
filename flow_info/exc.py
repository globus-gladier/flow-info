class ConfigException(Exception):
    """Configured incorrectly"""

    pass


class MigrationRequired(ConfigException):
    """A month is still stored as JSON, and needs `flow-info migrate` before it can be updated."""

    def __init__(self, year_months: list):
        self.year_months = sorted(year_months)
        months = " ".join(self.year_months)
        super().__init__(
            f"These months are still stored as JSON and must be migrated before they "
            f"can be updated: {months}\nRun: flow-info migrate {months}"
        )

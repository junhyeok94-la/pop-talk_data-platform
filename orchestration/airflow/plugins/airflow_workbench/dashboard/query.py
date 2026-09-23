"""Retired SQLite query API. Kept only as a clear compatibility error."""


class QueryError(ValueError):
    pass


def execute_sql(*args, **kwargs):
    raise QueryError(
        "Legacy SQLite execution was removed. Use Studio API builders or registered PostgreSQL sources."
    )

from sqlalchemy.sql.compiler import IdentifierPreparer

from timeplus_connect.driver.binding import escape_str


class TpIdentifierPreparer(IdentifierPreparer):

    def quote_identifier(self, value: str) -> str:
        """Quote an identifier."""
        first_char = value[0]
        if first_char in ('`', '"') and value[-1] == first_char:
            # Identifier is already quoted, assume that it's valid
            return value
        return f'"{escape_str(value)}"'

    def _requires_quotes(self, _value):
        return True

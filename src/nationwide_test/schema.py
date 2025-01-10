from ipaddress import AddressValueError, IPv4Address

import pandera.polars as pa

from nationwide_test.utils import VALID_CREDIT_CARD_PREFIXES_MAPPING


# mypy and pandera don't play too nicely together yet unfortunately :(
class TRANSACTION_SCHEMA(pa.DataFrameModel):  # type: ignore
    """
    A schema definition for validating transaction data using pandera.

    This schema ensures that transaction data adheres to specified constraints
    and includes additional validation checks for fields like IPv4 addresses.

    Attributes:
        credit_card_number (int):
            A positive integer representing the credit card number.
            Values must be greater than 0 and will be coerced into integers.
        ipv4 (str):
            A string representing a valid IPv4 address.
            Additional validation is applied to check its format.
        state (str):
            A string representing the two-character state abbreviation.
            This field must have a length of exactly 2 characters.
        vendor (str):
            A non-null string indicating the vendor name.
            Values must belong to a predefined set of valid vendors.
        is_fraudulent (bool):
            A boolean indicating whether the transaction is flagged as fraudulent.
        fraudulent_state (str):
            Some of the transactions in the fraud dataset had states and some didn't.
            This column just keeps track of those,
            although I'm not sure they are important.

    Methods:
        check_valid_ipv4(cls, ipv4: str) -> bool:
            Validates whether the given string is a properly formatted IPv4 address.

    """

    credit_card_number: int = pa.Field(gt=0, coerce=True)
    ipv4: str
    state: str = pa.Field(str_length=2)
    vendor: str = pa.Field(
        nullable=False, isin=list(VALID_CREDIT_CARD_PREFIXES_MAPPING.keys())
    )
    is_fraudulent: bool
    fraudulent_state: str = pa.Field(nullable=True)

    @pa.check("ipv4", name="check_valid_ipv4", element_wise=True)  # type: ignore
    def check_valid_ipv4(cls, ipv4: str) -> bool:
        try:
            IPv4Address(ipv4)
            return True
        except AddressValueError:
            print(f"invalid ipv4: {ipv4}")
            return False

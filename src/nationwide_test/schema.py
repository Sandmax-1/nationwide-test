from ipaddress import AddressValueError, IPv4Address
from itertools import chain

import pandera.polars as pa

VALID_CREDIT_CARD_PREFIXES_MAPPING = {
    "maestro": ["5018", "5020", "5038", "56##"],
    "mastercard": ["51", "52", "54", "55", "222%"],
    "visa": ["4"],
    "amex": ["34", "37"],
    "discover": ["6011", "65"],
    "diners": ["300", "301", "304", "305", "36", "38"],
    "jcb16": ["35"],
    "jcb15": ["2131", "1800"],
}


VALID_CREDIT_CARD_PREFIXES = chain(*VALID_CREDIT_CARD_PREFIXES_MAPPING.values())


class TrieNode:
    """
    Represents a single node in the Trie data structure.

    Attributes:
        children (dict[str, TrieNode]): Dictionary mapping characters
                                        to child TrieNode objects.
        is_end (bool): Indicates whether this node marks the end of a prefix.
        vendor (str): Vendor associated with the prefix that ends at this node.
    """

    def __init__(self) -> None:
        self.children: dict[str, "TrieNode"] = {}
        self.is_end: bool = False
        self.vendor: str = ""


class Trie:
    """
    Trie data structure for storing and searching prefixes.

    Attributes:
        root (TrieNode): The root node of the Trie.
    """

    def __init__(self) -> None:
        self.root = TrieNode()

    def add(self, word: str, vendor: str) -> None:
        """Adds a word and its associated vendor to the Trie.

        Args:
            word (str): The word (or prefix) to be added to the Trie.
            vendor (str): The vendor associated with the word.
        """
        node = self.root
        for char in word:
            if char not in node.children:
                new_node = TrieNode()
                node.children[char] = new_node

            node = node.children[char]
        node.is_end = True
        node.vendor = vendor

    def search(self, word: str) -> str:
        """
        Searches for a word in the Trie and returns its associated vendor.

        Args:
            word (str): The word (or prefix) to search for.

        Returns:
            str: The associated vendor if the word is found; otherwise, an empty string.
        """
        node = self.root
        for char in word:
            if char in node.children:
                node = node.children[char]
                if node.is_end:
                    return node.vendor
            else:
                return ""
        return ""


PREFIX_TRIE = Trie()

PREFIX_TO_VENDOR_MAPPING: dict[str, str] = {}

for vendor, prefixes in VALID_CREDIT_CARD_PREFIXES_MAPPING.items():
    for prefix in prefixes:
        PREFIX_TO_VENDOR_MAPPING[prefix] = vendor
        PREFIX_TRIE.add(prefix, vendor)


# mypy and pandera don't play too nicely together yet unfortunately :(
class FRAUD_SCHEMA(pa.DataFrameModel):  # type: ignore
    """
    Schema for fraud-related data validation using pandera.

    Attributes:
        credit_card_number (int): A positive integer representing
                                  the credit card number.
        ipv4 (str): A string representing a valid IPv4 address.
        state (str): A two-character string representing a state (nullable).
    """

    credit_card_number: int = pa.Field(gt=0, coerce=True)
    ipv4: str
    state: str = pa.Field(str_length=2, nullable=True)

    @pa.check("ipv4", name="check_valid_ipv4", element_wise=True)  # type: ignore
    def check_valid_ipv4(cls, ipv4: str) -> bool:
        try:
            IPv4Address(ipv4)
            return True
        except AddressValueError:
            print(f"invalid ipv4: {ipv4}")
            return False


class TRANSACTION_SCHEMA(FRAUD_SCHEMA):
    """
    Schema for transaction data validation using pandera.

    Attributes:
        vendor (str): A non-null string representing the vendor.
                      Must be one of the valid vendor keys.
    """

    vendor: str = pa.Field(
        nullable=False, isin=list(VALID_CREDIT_CARD_PREFIXES_MAPPING.keys())
    )

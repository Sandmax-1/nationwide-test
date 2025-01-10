from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZipFile

import polars as pl

from nationwide_test.config import ZIPPED_DATA_PATH
from nationwide_test.schema import FRAUD_SCHEMA, PREFIX_TRIE, TRANSACTION_SCHEMA, Trie


def process_transactions(
    file_path: Path, prefix_trie: Trie = PREFIX_TRIE
) -> pl.DataFrame:
    """
    Processes a transaction file by adding vendor information
    and filtering invalid entries.

    Args:
        file_path (Path): The path to the transaction CSV file.
        prefix_trie (Trie, optional): A Trie used to map credit card prefixes
                                      to vendors.Defaults to PREFIX_TRIE.

    Returns:
        pl.DataFrame: The processed DataFrame with a 'vendor' column and filtered rows.
    """
    return (
        pl.read_csv(
            file_path,
            has_header=True,
            schema_overrides={"credit_card_number": str},
        )
        .with_columns(
            vendor=pl.col("credit_card_number")
            .str.slice(0, 4)
            .map_elements(prefix_trie.search, return_dtype=pl.String)
        )
        .filter(pl.col("vendor") != "")
    )


def preprocess_fraud_csv(directory_path: Path) -> None:
    """
    Preprocesses the raw fraud CSV file
    by fixing the shcema at the top of the file (state isn't included).
    Then adding in the missing commas at the ends of the lines to accomodate
    the state column when the value is missing.

    Args:
        directory_path (Path): The directory containing the raw fraud CSV file.
    """
    with (
        open(Path(directory_path) / "fraud" / "fraud", "r") as raw,
        open(Path(directory_path) / "fraud" / "preprocessed.csv", "a") as preprocessed,
    ):
        line_iterator = raw.readlines()
        header = line_iterator[0]

        header = header.strip() + ",state\n"
        preprocessed.write(header)

        for line in line_iterator[1:]:
            line = line.strip()

            # Need to add in comma to fill in missing column with null
            if line[-1].isdigit():
                line += ","

            line += "\n"

            preprocessed.write(line)


def extract_zip_files(
    data_path: Path = ZIPPED_DATA_PATH,
) -> tuple[pl.DataFrame, pl.DataFrame,]:
    """
    Extracts and processes fraud and transaction data from ZIP files.

    Args:
        data_path (Path): The directory containing the ZIP files.
                          Defaults to ZIPPED_DATA_PATH.

    Returns:
        tuple[pl.DataFrame, pl.DataFrame]: A tuple containing:
            - A validated DataFrame for fraud data.
            - A validated DataFrame for transaction data.
    """
    with TemporaryDirectory() as tmp:
        tmp_folder_path = Path(tmp)

        fraud = ZipFile(data_path / "fraud.zip")
        transactions_1 = ZipFile(data_path / "transaction-001.zip")
        transactions_2 = ZipFile(data_path / "transaction-002.zip")

        fraud.extractall(tmp_folder_path / "fraud")
        transactions_1.extractall(tmp_folder_path / "t1")
        transactions_2.extractall(tmp_folder_path / "t2")

        preprocess_fraud_csv(tmp_folder_path)

        fraud_df = pl.read_csv(
            tmp_folder_path / "fraud" / "preprocessed.csv",
            has_header=True,
            schema_overrides={"credit_card_number": str},
        )
        transactions_1_df = process_transactions(
            tmp_folder_path / "t1" / "transaction-001"
        )

        transactions_2_df = process_transactions(
            tmp_folder_path / "t2" / "transaction-002"
        )

        transactions_df = pl.concat([transactions_1_df, transactions_2_df])

        # pandera still janky with types annoyingly, so formally cast here
        validated_fraud_df: pl.DataFrame = FRAUD_SCHEMA.validate(fraud_df)
        validated_transactions_df: pl.DataFrame = TRANSACTION_SCHEMA.validate(
            transactions_df
        )

        fraudulent_transactions = validated_transactions_df.join(
            validated_fraud_df, on=["credit_card_number", "ipv4"]
        )
        # print(fraudulent_transactions.head())

    return (fraudulent_transactions, validated_transactions_df)


if __name__ == "__main__":
    fraudulent_transactions, all_transactions = extract_zip_files()
    print(fraudulent_transactions.shape, all_transactions.shape)
    # fraudulent_transactions.write_csv(Path.cwd() / "fraudulent_transactions.csv")

from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZipFile

import polars as pl

from nationwide_test.config import ZIPPED_DATA_PATH
from nationwide_test.schema import FRAUD_SCHEMA, PREFIX_TRIE, TRANSACTION_SCHEMA, Trie


def process_transactions(
    file_path: Path, prefix_trie: Trie = PREFIX_TRIE
) -> pl.DataFrame:
    return (
        pl.read_csv(
            file_path,
            has_header=True,
            schema_overrides={"credit_card_number": str},
        )
        .with_columns(
            vendor=pl.col("credit_card_number")
            .str.slice(0, 3)
            .map_elements(prefix_trie.search, return_dtype=pl.String)
        )
        .filter(pl.col("vendor") != "")
    )


def extract_zip_files(
    data_path: Path = ZIPPED_DATA_PATH,
) -> tuple[TRANSACTION_SCHEMA, TRANSACTION_SCHEMA, TRANSACTION_SCHEMA]:
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

    return (
        FRAUD_SCHEMA.validate(fraud_df),
        TRANSACTION_SCHEMA.validate(transactions_1_df),
        TRANSACTION_SCHEMA.validate(transactions_2_df),
    )


def preprocess_fraud_csv(directory_path: Path) -> None:
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


if __name__ == "__main__":
    fraud_df, transactions_1_df, transactions_2_df = extract_zip_files()

    print(fraud_df.head())
    print(transactions_1_df.head())
    print(transactions_2_df.head())

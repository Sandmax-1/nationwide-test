from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZipFile

from polars import DataFrame, read_csv

from nationwide_test.config import ZIPPED_DATA_PATH


def extract_zip_files() -> tuple[DataFrame, DataFrame, DataFrame]:
    with TemporaryDirectory() as tmp:
        tmp_folder_path = Path(tmp)

        fraud = ZipFile(ZIPPED_DATA_PATH / "fraud.zip")
        transactions_1 = ZipFile(ZIPPED_DATA_PATH / "transaction-001.zip")
        transactions_2 = ZipFile(ZIPPED_DATA_PATH / "transaction-002.zip")

        fraud.extractall(tmp_folder_path / "fraud")
        transactions_1.extractall(tmp_folder_path / "t1")
        transactions_2.extractall(tmp_folder_path / "t2")

        preprocess_fraud_csv(tmp_folder_path)

        fraud_df = read_csv(
            tmp_folder_path / "fraud" / "preprocessed.csv",
            has_header=True,
        )
        transactions_1_df = read_csv(
            tmp_folder_path / "t1" / "transaction-001",
            has_header=True,
        )
        transactions_2_df = read_csv(
            tmp_folder_path / "t2" / "transaction-002",
            has_header=True,
        )

    return fraud_df, transactions_1_df, transactions_2_df


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
    extract_zip_files()

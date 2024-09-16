import json
import logging
import re
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# I use a simple dict here as I have a few variables, but we can store them as ENV vars, for exp.
CONFIG = {
    "api_url": "https://fakerapi.it/api/v2/persons",
    "db_path": "persons.sqlite",
    "chunk_size": 1000,
    "total_quantity": 30000,
}


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DataProcessor:
    """
    A class is used to fetch, validate, clean, and process data from an API, and store it in a local database.

    Attributes:
        api_url (str): The URL of the API to fetch data from.
        db_path (str): The path to the local database.
        total_quantity (int): The total number of records to fetch (default is 30000).
        chunk_size (int): The number of records to fetch per API call (default is 1000).
    """

    def __init__(
        self,
        api_url: str,
        db_path: str,
        total_quantity: int = 30000,
        chunk_size: int = 1000,
    ):
        self.api_url = api_url
        self.db_path = db_path
        self.total_quantity = total_quantity
        self.chunk_size = chunk_size

    def create_retry_session(
        self,
        retries: int = 3,
        backoff_factor: float = 0.3,
        status_forcelist: Tuple[int, ...] = (500, 502, 503, 504),
    ) -> requests.Session:
        """
        Creates a session with automatic retries for failed requests.

        :param retries: Number of retry attempts for failed requests.
        :param backoff_factor: A factor that applies a delay between retries.
        :param status_forcelist: A tuple of HTTP status codes that trigger a retry.
        :return: A session object with retry capabilities.
        """

        session = requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def fetch_data_chunk(self, params: Dict) -> List[Dict]:
        try:
            session = self.create_retry_session()
            response = session.get(self.api_url, params=params)
            response.raise_for_status()
            return response.json()["data"]
        except requests.RequestException as e:
            logger.error(f"Error fetching data chunk: {e}")
            return []

    def fetch_data(self) -> List[Dict]:
        """
        Fetch data from the API in chunks.

        :param total_quantity: Total number of records to fetch
        :param chunk_size: Number of records to fetch in each API call
        :return: List of fetched data records
        """
        all_data = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_params = {
                executor.submit(
                    self.fetch_data_chunk,
                    {
                        "_quantity": min(
                            self.chunk_size, self.total_quantity - i * self.chunk_size
                        )
                    },
                ): i
                for i in range(
                    (self.total_quantity + self.chunk_size - 1) // self.chunk_size
                )
            }

            for future in as_completed(future_to_params):
                try:
                    data = future.result()
                    all_data.extend(data)
                except Exception as exc:
                    logger.error(f"Generated an exception: {exc}")

        logger.info(f"Fetched {len(all_data)} records")
        return all_data

    # we also can use attrs library here
    def validate_data(self, data: List[Dict]) -> List[Dict]:
        """
        Validate the fetched data.

        :param data: List of data records to validate
        :return: List of valid data records
        """
        valid_data = []
        for item in data:
            # I think the best approach is to validate data on the frontend side
            try:
                if all(
                    field in item
                    for field in [
                        "firstname",
                        "lastname",
                        "email",
                        "birthday",
                        "address",
                    ]
                ):
                    if (
                        self.validate_email(item["email"])
                        and self.validate_date(item["birthday"])
                        and self.validate_address(item["address"])
                    ):
                        valid_data.append(item)
            except Exception as e:
                logger.error(f"Error validating item: {e}")

        logger.info(f"Validated {len(valid_data)} records out of {len(data)}")
        return valid_data

    @staticmethod
    def validate_email(email: str) -> bool:
        email_regex = re.compile(r"[^@]+@[^@]+\.[^@]+")
        return bool(email_regex.match(email))

    @staticmethod
    def validate_date(date_string: str) -> bool:
        try:
            datetime.strptime(date_string, "%Y-%m-%d")
            return True
        except ValueError:
            return False

    @staticmethod
    def validate_address(address: Dict) -> bool:
        required_fields = ["street", "city", "country"]
        return all(field in address for field in required_fields)

    def clean_data(self, data: List[Dict]) -> List[Dict]:
        """
        Clean the data by standardizing values and formatting fields.

        :param data: A list of data records to clean.
        :return: A list of cleaned data records.
        """
        cleaned_data = []
        for item in data:
            try:
                cleaned_item = item.copy()
                cleaned_item["email"] = cleaned_item["email"].lower()
                cleaned_item["firstname"] = cleaned_item["firstname"].capitalize()
                cleaned_item["lastname"] = cleaned_item["lastname"].capitalize()
                if "phone" in cleaned_item:
                    cleaned_item["phone"] = re.sub(r"\D", "", cleaned_item["phone"])
                cleaned_data.append(cleaned_item)
            except Exception as e:
                logger.error(f"Error cleaning item: {e}")

        logger.info(f"Cleaned {len(cleaned_data)} records")
        return cleaned_data

    @staticmethod
    def detect_duplicates(data: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """
        Detect duplicate records in the dataset.

        :param data: A list of data records to check for duplicates.
        :return: A tuple containing unique data records and duplicate records.
        """
        seen = set()
        duplicates = []
        unique_data = []
        for item in data:
            try:
                item_tuple = tuple(
                    sorted(
                        (k, str(v))
                        for k, v in item.items()
                        if k not in ["id", "address"]
                    )
                )
                address_tuple = tuple(
                    sorted((k, str(v)) for k, v in item["address"].items())
                )
                row_repr = item_tuple + address_tuple

                if row_repr in seen:
                    duplicates.append(item)
                else:
                    seen.add(row_repr)
                    unique_data.append(item)
            except Exception as e:
                logger.error(f"Error detecting duplicate: {e}")

        logger.info(
            f"Found {len(duplicates)} duplicates. {len(unique_data)} unique records remaining"
        )
        return unique_data, duplicates

    def anonymize_data(self, data: List[Dict]) -> List[Dict]:
        """
        Anonymize the data by masking personal information.

        :param data: List of data records to anonymize
        :return: List of anonymized data records
        """
        anonymized = []
        for person in data:
            try:
                age_group = self.calculate_age_group(person["birthday"])
                email_parts = person["email"].split("@")
                anonymized.append(
                    {
                        "firstname": "****",
                        "lastname": "****",
                        "email": f"****@{email_parts[1]}",
                        "phone": "****",
                        "birthday": "****",
                        "gender": "****",
                        "street": "****",
                        "streetName": "****",
                        "buildingNumber": "****",
                        "city": person["address"]["city"],
                        "zipcode": "****",
                        "country": person["address"]["country"],
                        "county_code": "****",
                        "latitude": "****",
                        "longitude": "****",
                        "website": "****",
                        "image": "****",
                        "age_group": age_group,
                        "email_provider": email_parts[1],
                    }
                )
            except Exception as e:
                logger.error(f"Error anonymizing person: {e}")

        logger.info(f"Anonymized {len(anonymized)} records")
        return anonymized

    @staticmethod
    def calculate_age_group(birthday: str) -> str:
        birth_date = datetime.strptime(birthday, "%Y-%m-%d")
        age = datetime.now().year - birth_date.year
        return f"[{age // 10 * 10}-{(age // 10 * 10) + 9}]"

    def store_data(self, data: List[Dict]):
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute(
                """
            CREATE TABLE IF NOT EXISTS persons (
                id INTEGER PRIMARY KEY autoincrement,
                firstname TEXT,
                lastname TEXT,
                email TEXT,
                phone TEXT,
                birthday TEXT,
                gender TEXT,
                street TEXT,
                streetName TEXT,
                buildingNumber TEXT,
                city TEXT,
                zipcode TEXT,
                country TEXT,
                county_code TEXT,
                latitude TEXT,
                longitude TEXT,
                website TEXT,
                image TEXT,
                age_group TEXT,
                email_provider TEXT
            )
            """
            )

            cursor.execute("DELETE FROM persons")
            conn.commit()

            df = pd.DataFrame(data)

            # I process in batches in order to spare memory that can be critical in production and/or in case of large datasets
            for i in range(0, len(df), self.chunk_size):
                chunk = df.iloc[i : i + self.chunk_size]
                chunk.to_sql("persons", conn, if_exists="append", index=False)
                conn.commit()

            conn.close()
            logger.info(
                f"Data upload complete. {len(data)} records stored in the database."
            )
        except Exception as e:
            logger.error(f"Error storing data: {e}")

    def profile_data(self, data: List[Dict]) -> Dict:
        try:
            df = pd.DataFrame(data)
            profile = {
                "total_records": len(df),
                "unique_values": {
                    "city": df["city"].nunique(),
                    "country": df["country"].nunique(),
                    "age_group": df["age_group"].nunique(),
                    "email_provider": df["email_provider"].nunique(),
                },
                "most_common_countries": df["country"].value_counts().head(5).to_dict(),
                "age_group_distribution": df["age_group"].value_counts().to_dict(),
                "top_email_providers": df["email_provider"]
                .value_counts()
                .head(5)
                .to_dict(),
            }
            logger.info("Data profiling complete")
            return profile
        except Exception as e:
            logger.error(f"Error profiling data: {e}")
            return {}

    def process_pipeline(self):
        try:
            raw_data = self.fetch_data()
            validated_data = self.validate_data(raw_data)
            cleaned_data = self.clean_data(validated_data)
            unique_data, duplicates = self.detect_duplicates(cleaned_data)
            anonymized_data = self.anonymize_data(unique_data)
            self.store_data(anonymized_data)
            data_profile = self.profile_data(anonymized_data)

            logger.info("Data Profile: %s", json.dumps(data_profile, indent=2))
            logger.info("Number of duplicates removed: %d", len(duplicates))
        except Exception as e:
            logger.error(f"Error in processing pipeline: {e}")


def main():
    processor = DataProcessor(**CONFIG)
    processor.process_pipeline()


if __name__ == "__main__":
    main()

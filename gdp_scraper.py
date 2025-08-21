import requests
import pandas as pd
import json
from datetime import datetime
import psycopg2
from sqlalchemy import create_engine, text
import numpy as np

connection_string = "postgresql://postgres.iwdgjfmeaxqabnobmsfl:2580@aws-1-eu-north-1.pooler.supabase.com:6543/postgres"


class GDPDataCollector:
    def __init__(self):
        self.base_url = "https://api.worldbank.org/v2"
        self.gdp_indicator = "NY.GDP.MKTP.CD"  # GDP (current US$)
        self.gdp_per_capita_indicator = "NY.GDP.PCAP.CD"  # GDP per capita

    def fetch_gdp_data(self, start_year=2019, end_year=2023):
        gdp_url = f"{self.base_url}/country/all/indicator/{self.gdp_indicator}"
        gdp_params = {
            "date": f"{start_year}:{end_year}",
            "format": "json",
            "per_page": 1000,
            "page": 1,
        }

        gdp_pc_url = (
            f"{self.base_url}/country/all/indicator/{self.gdp_per_capita_indicator}"
        )
        gdp_pc_params = gdp_params.copy()

        print("Fetching GDP data...")
        gdp_response = requests.get(gdp_url, params=gdp_params)
        gdp_pc_response = requests.get(gdp_pc_url, params=gdp_pc_params)

        if gdp_response.status_code == 200 and gdp_pc_response.status_code == 200:
            gdp_data = gdp_response.json()[1] if len(gdp_response.json()) > 1 else []
            gdp_pc_data = (
                gdp_pc_response.json()[1] if len(gdp_pc_response.json()) > 1 else []
            )

            return self.process_gdp_data(gdp_data, gdp_pc_data)
        else:
            raise Exception(f"Failed to fetch data: {gdp_response.status_code}")

    def process_gdp_data(self, gdp_data, gdp_pc_data):
        gdp_records = []
        for record in gdp_data:
            if record["value"] is not None:
                gdp_records.append(
                    {
                        "country_code": record["country"]["id"],
                        "country_name": record["country"]["value"],
                        "year": int(record["date"]),
                        "gdp_current_usd": float(record["value"]),
                    }
                )

        gdp_pc_records = []
        for record in gdp_pc_data:
            if record["value"] is not None:
                gdp_pc_records.append(
                    {
                        "country_code": record["country"]["id"],
                        "country_name": record["country"]["value"],
                        "year": int(record["date"]),
                        "gdp_per_capita_usd": float(record["value"]),
                    }
                )

        gdp_df = pd.DataFrame(gdp_records)
        gdp_pc_df = pd.DataFrame(gdp_pc_records)

        combined_df = pd.merge(
            gdp_df, gdp_pc_df, on=["country_code", "country_name", "year"], how="outer"
        )

        combined_df["gdp_category"] = pd.cut(
            combined_df["gdp_per_capita_usd"],
            bins=[0, 1045, 4125, 12735, float("inf")],
            labels=[
                "Low income",
                "Lower middle income",
                "Upper middle income",
                "High income",
            ],
            include_lowest=True,
        )

        return combined_df

    def create_country_mapping(self):
        country_mapping = {
            "United States": "USA",
            "United Kingdom": "GBR",
            "Russia": "RUS",
            "Iran": "IRN",
            "South Korea": "KOR",
            "Czech Republic": "CZE",
            "Slovakia": "SVK",
            "Venezuela": "VEN",
            "Bolivia": "BOL",
            "Moldova": "MDA",
            "North Macedonia": "MKD",
            "Taiwan": "TWN",
        }
        return country_mapping

    def save_to_supabase(self, df, connection_string):
        engine = create_engine(connection_string)

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS gdp_data (
            id SERIAL PRIMARY KEY,
            country_code VARCHAR(3),
            country_name VARCHAR(100),
            year INTEGER,
            gdp_current_usd BIGINT,
            gdp_per_capita_usd DECIMAL(10,2),
            gdp_category VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(country_code, year)
        );
        """

        with engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()

        df.to_sql("gdp_data", engine, if_exists="append", index=False, method="multi")
        print(f"Inserted {len(df)} GDP records into database")


def main():
    collector = GDPDataCollector()

    gdp_df = collector.fetch_gdp_data(2019, 2023)

    print("\nSample GDP Data:")
    print(gdp_df.head())
    print(f"\nTotal records: {len(gdp_df)}")
    print(f"Countries covered: {gdp_df['country_name'].nunique()}")

    collector.save_to_supabase(gdp_df, connection_string)
    return gdp_df


if __name__ == "__main__":
    gdp_data = main()

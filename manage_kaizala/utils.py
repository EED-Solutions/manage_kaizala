import polars as pl
import pandas as pd
import os
import pathlib
from loguru import logger
import requests
import re


def import_and_clean_messages(path_to_csv):

    # Importiere csv als dataframe
    df = pl.read_csv(path_to_csv)
    logger.debug(f"Anzahl der Zeilen {df.height}")

    # bereinige ungültige Daten anhand timestamp pattern
    ts_pattern = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z"  # 2023-02-16T07:31:59Z
    df = df.select(
        "*",
        pl.col("Timestamp").str.contains(ts_pattern).alias("is_invalid_data"),
    )
    df_clean = df.filter(pl.col("is_invalid_data"))
    df_error = df.filter(~pl.col("is_invalid_data"))
    logger.debug(f"Anzahl der Zeilen nach Bereinigung {len(df_clean)}")
    logger.debug(f"Anzahl der fehlerhaften Zeilen: {len(df_error)}")

    # datentypen definieren
    # timestamp umwandeln
    dateformat = "%Y-%m-%dT%H:%M:%SZ"
    df_clean = df_clean.with_columns(pl.col("Timestamp").str.to_datetime(dateformat))
    return {"df_clean": df_clean, "df_error": df_error}


def export_messages(df, path_to_xlsx):
    writer = pd.ExcelWriter(
        path_to_xlsx,
        engine="xlsxwriter",
        engine_kwargs={"options": {"strings_to_urls": False}},
    )
    df.to_pandas().to_excel(writer)
    writer.close()


def download_attachments(df, path_out):
    """Downloads attachemnts."""

    def get_ext(url):
        try:
            pattern_ext = r"https://\w+\.[\w-]+\.\w+\.\w+\.\w+/\w+/\w+\.(\w+)\?"
            pattern_ext = r"https://\w+\.[\w\-\./]+\.(\w+)\?"
            pattern_ext = r"^https://[\w\-\./]+\.(\w+)\?"
            ext = re.search(pattern_ext, url).group(1)
        except Exception:  # wenn extension am Ende steht ohne folgendes Fragezeichen
            pattern_ext = r"https://\w+\.[\w\-\./]+\.(\w+)"
            pattern_ext = r"^https://[\w\-\./]+\.(\w+)$"
            ext = re.search(pattern_ext, url).group(1)
        return ext

    pattern = r"https://"
    df_clean = df.filter(pl.col("Attachement stored at:").str.starts_with(pattern))
    I = 0
    standard_msg_types = [
        "Video",
        "Attachment",
        "Audio",
        "Document",
    ]
    for row in df_clean.rows(named=True):
        I += 1
        logger.debug(f'Anhang {I} von {len(df_clean)}')
        logger.debug(row)
        ts = row["Timestamp"].strftime("%Y%m%d_%H%M%S")
        ext = get_ext(row["Attachement stored at:"])
        filename_base = os.path.join(
            path_out, row["Group Name"], f'{ts}_{row["MessageID"]}_{row["Sender"]}_{row["Message Type"]}'
        )

        file_name = f"{filename_base}.{ext}"
        if row["Message Type"] in standard_msg_types:  # images
            if not os.path.exists(file_name):
                data = requests.get(row["Attachement stored at:"]).content
                with open(file_name, "wb") as handler:
                    handler.write(data)
        elif row["Message Type"] == "Album":  # unbekannt
            attachments_stored_at = row["Attachement stored at:"].split(' : ')
            J = 0
            for url in attachments_stored_at:
                if url != '':
                    ext = get_ext(url)
                    J += 1
                    file_name = f"{filename_base}_{J}.{ext}"
                    if not os.path.exists(file_name):
                        data = requests.get(url).content
                        with open(file_name, "wb") as handler:
                            handler.write(data)

    return True

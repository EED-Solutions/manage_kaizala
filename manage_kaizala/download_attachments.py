import pathlib
import os
from utils import export_messages, import_and_clean_messages, download_attachments


def import_csv_and_download_attachments(path):
    """Downloads Attachments according to MicrosoftKaizala_TenantMessages.csv."""
    path_csv = os.path.join(
        path, "Datenexporte", "20220220", "MicrosoftKaizala_TenantMessages.csv"
    )
    path_xlsx_clean = os.path.join(
        path, "Datenexporte", "20220220", "MicrosoftKaizala_TenantMessages.xlsx"
    )
    path_xlsx_error = os.path.join(
        path, "Datenexporte", "20220220", "MicrosoftKaizala_TenantMessages_error.xlsx"
    )
    path_attachements = path
    # path_attachements = os.path.join(path, "Datenexporte", "20220220", "attachments")
    pathlib.Path(path_attachements).mkdir(parents=False, exist_ok=True)
    dict_df = import_and_clean_messages(path_csv)

    path_attachements = path
    # path_attachements = os.path.join(path, "Datenexporte", "20220220", "attachments")
    pathlib.Path(path_attachements).mkdir(parents=False, exist_ok=True)
    dict_df = import_and_clean_messages(path_csv)
    export_messages(dict_df["df_clean"], path_to_xlsx=path_xlsx_clean)
    export_messages(dict_df["df_error"], path_to_xlsx=path_xlsx_error)
    download_attachments(dict_df["df_clean"], path_out=path_attachements)


if __name__ == "__main__":
    pass

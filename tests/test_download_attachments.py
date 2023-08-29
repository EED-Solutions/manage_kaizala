from icecream import ic


def test_installation():
    """Dummy test."""
    import manage_kaizala
    ic(manage_kaizala.__path__)
    from manage_kaizala import import_csv_and_download_attachments

    # import_csv_and_download_attachments(r"C:\Users\ericb\OneDrive - EED-Solutions by Eric Brahmann\Other Documents - Family\Technik\2302 Kaizala Daten sichern FAM-1088")


    # manage_kaizala.import_csv_and_download_attachments()
    assert True

def utf_8_string_to_string(utf_8_string: str) -> str:  # pragma: no cover
    return (
        utf_8_string.encode("latin1")  # To bytes, required by 'unicode-escape'
        .decode("unicode-escape")  # Perform the actual octal-escaping decode
        .encode("latin1")  # 1:1 mapping back to bytes
        .decode("utf-8")  # Decode original encoding
    )


def parse_utf_8_byte_string(  # pragma: no cover
    string: str,
) -> str:  # pragma: no cover
    return utf_8_string_to_string(string.replace("b'", "").replace("'", ""))

# replacement strings
WINDOWS_LINE_ENDING = b'\r\n'
UNIX_LINE_ENDING = b'\n'


def convert_to_linux_eol(path_in, path_out):
    """path_in can be absolute or relative, same for path_out.
    path_out can be the same as path_in, i.e. replacing in place."""
    with open(path_in, 'rb') as open_file:
        content = open_file.read()

    content = content.replace(WINDOWS_LINE_ENDING, UNIX_LINE_ENDING)

    with open(path_out, 'wb') as open_file:
        open_file.write(content)
    return True

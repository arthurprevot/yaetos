from yaetos.logger import setup_logging
logger = setup_logging('Job')

# replacement strings
WINDOWS_LINE_ENDING = b'\r\n'
UNIX_LINE_ENDING = b'\n'

def convert_to_linux_eol(path_in, path_out):
    """
    path_in can be absolute or relative, same for path_out.
    path_out can be the same as path_in, i.e. replacing in place."""
    # # relative or absolute file path, e.g.:
    # file_path = in_path

    with open(file_path, 'rb') as open_file:
        content = open_file.read()

    content = content.replace(WINDOWS_LINE_ENDING, UNIX_LINE_ENDING)

    with open(file_path, 'wb') as open_file:
        open_file.write(content)
    return True

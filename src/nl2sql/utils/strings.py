def read_file_to_str(filename: str, encoding: str = 'utf-8'):
    with open(filename, 'r', encoding=encoding) as file:
        return file.read()

import os


def get_resource_path(filename):
    """Returns the fully qualified path for a file in the test resources directory

    :param filename: the file's name as a string
    :return: the fully qualified path for the file as a string
    """
    root = os.path.dirname(__file__)
    return os.path.join(root, "resources", filename)


def get_resource(filename):
    """Returns the contents of a file in the test resources directory

    :param filename: the file's name as a string
    :return: the contents of the file as a string
    """
    return open(get_resource_path(filename)).read()

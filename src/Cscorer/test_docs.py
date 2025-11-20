

def test(kind=None):
    """
    Return a list of random ingredients as strings.

    :param kind: Optional "kind" of .
    :type kind: list[str] or None
    :raise lumache.InvalidKindError: If the kind is invalid.
    :return: The ingredients list.
    :rtype: list[str]

    """
    return ["shells", "gorgonzola", "parsley"]
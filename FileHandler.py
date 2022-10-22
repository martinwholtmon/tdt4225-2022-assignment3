"""Containing all the methods to read the files in the dataset
"""


def read_labeled_users_file(path) -> list:
    """Will read the labeled_ids.txt that includes all the users that have labels

    Args:
        path (str): path to file

    Returns:
        list: list of ids
    """
    open_file = open(path, "r", encoding="utf-8")
    list_of_lists = [(line.strip()) for line in open_file]
    open_file.close()
    return list_of_lists


def read_user_labels_file(path) -> dict:
    """Read the labels provided in the users directory
    Returns it as a list with a key matching the datapoint filename.
    E.g.: {
        "20070626113229": ["2007/06/26", "11:32:29", "2007/06/26", "11:40:29" ,"bus"],
        ...
    }

    Args:
        path (str): path to file

    Returns:
        dict: labels with key as start_date_time
    """
    data = read_data_file(path)[1:]  # skip header
    labels = {}
    for activity in data:
        # Create key as filename, e.g.: 20070626113229
        key = str(activity[0]).replace("/", "") + str(activity[1]).replace(":", "")
        labels[key] = activity
    return labels


def read_data_file(path) -> "list[list]":
    """Will read a datafile

    Args:
        path (str): path to file

    Returns:
        list[list]: a list containing every line provided as a list
    """
    n_file = open(path, "r", encoding="utf-8")
    list_of_lists = [(line.strip()).replace(",", " ").split() for line in n_file]
    n_file.close()
    return list_of_lists

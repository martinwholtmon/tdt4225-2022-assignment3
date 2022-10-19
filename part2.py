import pandas as pd
from tabulate import tabulate
from DbHandler import DbHandler


def task_1(db: DbHandler):
    """Find out the total amount of docs in collection: User, Activity and TrackPoint"""
    tables = {}
    tables["User"] = db.get_nr_documents("User")
    tables["Activity"] = db.get_nr_documents("Activity")
    tables["TrackPoint"] = db.get_nr_documents("TrackPoint")

    # Print
    print("\nTask 1")
    print(
        f"Total amount of rows in tables: \n{tabulate_dict(tables, ['Table', 'Rows'])}"
    )


def task_2(db: DbHandler):
    """Find the average number of activities per user (including users with 0 activities)"""
    average = db.get_nr_documents("Activity") / db.get_nr_documents("User")

    # Print
    print("\nTask 2")
    print(f"Average number of activities per user is {average}")


def task_3(db: DbHandler):
    """Find the top 20 users with the highest number of activities."""
    return NotImplementedError


def task_4(db: DbHandler):
    """Find all users who have taken a taxi."""
    return NotImplementedError


def task_5(db: DbHandler):
    """Find all types of transportation modes and count how many activities that are tagged with these transportation mode labels.
    Do not count the rows where the mode is null.
    """
    return NotImplementedError


def task_6(db: DbHandler):
    """
    a) Find the year with the most activities.
    b) Is this also the year with most recorded hours?
    """
    return NotImplementedError


def task_7(db: DbHandler):
    """Find the total distance (in km) walked in 2008, by user with id=112."""
    return NotImplementedError


def task_8(db: DbHandler):
    """Find the top 20 users who have gained the most altitude meters"""
    return NotImplementedError


def task_9(db: DbHandler):
    """Find all users who have invalid activities, and the number of invalid activities per user
    An invalid activity is defined as an activity with consecutive
    trackpoints where the timestamps deviate with at least 5 minutes.
    """
    return NotImplementedError


def task_10(db: DbHandler):
    """Find the users who have tracked an activity in the Forbidden City of Beijing.
    the Forbidden City: lat 39.916, lon 116.397
    """
    return NotImplementedError


def task_11(db: DbHandler):
    """Find all users who have registered transportation_mode and their most used transportation_mode."""
    return NotImplementedError


def tabulate_dict(data, headers) -> str:
    """Will tabulate a dict that has the format of key:value

    Args:
        dict (dict): a key:value pair store
        headers (list): list of the header names

    Returns:
        str: tabulated data
    """
    df = pd.DataFrame(data, index=[0]).transpose()
    return tabulate(df, headers=headers, floatfmt=".0f")


def main():
    db = None
    try:
        db = DbHandler()

        # Execute the tasks:
        task_1(db)
        task_2(db)
        task_3(db)
        task_4(db)
        task_5(db)
        task_6(db)
        task_7(db)
        task_8(db)
        task_9(db)
        task_10(db)
        task_11(db)

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if db:
            db.connection.close_connection()


if __name__ == "__main__":
    main()

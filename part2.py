import pandas as pd
import pprint as pp
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
    pipeline = []

    # Get user with nr_activities
    pipeline.append({"$project": {"_id": 1, "nr_activities": {"$size": "$activities"}}})

    # Sort
    pipeline.append({"$sort": {"nr_activities": -1}})

    # Get top 20
    pipeline.append({"$limit": 20})

    # Query
    ret = db.aggregate("User", pipeline)

    # Print
    print("\nTask 3")
    pp.pprint(list(ret))


def task_4(db: DbHandler):
    """Find all users who have taken a taxi."""
    collection = "User"
    query = {"activities.transportation_mode": "taxi"}
    fields = {"_id": 1}
    ret = db.find_documents(collection, query, fields)

    # Print
    print("\nTask 4")
    pp.pprint(list(ret))


def task_5(db: DbHandler):
    """Find all types of transportation modes and count how many activities that are tagged with these transportation mode labels.
    Do not count the rows where the mode is null.
    """
    pipeline = []

    # Remove null
    pipeline.append({"$match": {"transportation_mode": {"$exists": True, "$ne": None}}})

    # Group by transportation mode and count instances
    pipeline.append({"$group": {"_id": "$transportation_mode", "count": {"$sum": 1}}})

    # Query
    ret = db.aggregate("Activity", pipeline)

    # Print
    print("\nTask 5")
    pp.pprint(list(ret))


def task_6(db: DbHandler):
    """
    a) Find the year with the most activities.
    b) Is this also the year with most recorded hours?
    """
    # Get year with most activities
    pipeline = []

    # Convert start_date_time to year
    pipeline.append({"$project": {"_id": 1, "year": {"$year": "$start_date_time"}}})
    pipeline.append({"$group": {"_id": "$year", "count": {"$sum": 1}}})  # Group by year
    pipeline.append({"$sort": {"count": -1}})  # Sort
    pipeline.append({"$limit": 1})  # Get top 1

    # Query
    ret = db.aggregate("Activity", pipeline)
    most_activities_year = list(ret)[0]["_id"]

    # Get year with most recorded hours
    pipeline = []
    pipeline.append(
        {
            "$project": {
                "_id": 0,
                "year": {"$year": "$start_date_time"},
                "start_date_time": 1,
                "end_date_time": 1,
            }
        }
    )
    # Query
    ret = db.aggregate("Activity", pipeline)

    # Calculate
    recorded_hours = {}
    for tp in ret:
        year = tp["year"]
        start = tp["start_date_time"]
        finish = tp["end_date_time"]

        # Update
        recorded_hours[year] = (
            recorded_hours[year] + (finish - start)
            if recorded_hours.get(year) is not None  # Update if exist in dict
            else (finish - start)  # First time? Insert value
        )

    # Convert to hours
    # source: https://stackoverflow.com/a/47207182
    for key, val in recorded_hours.items():
        recorded_hours[key] = divmod(val.seconds, 3600)[0]

    # Most hours
    most_recorded_hours_year = max(recorded_hours, key=recorded_hours.get)

    # Print
    print("\nTask 6")
    print(f"Year with most activities: {most_activities_year}")
    print(f"Year with most recorded hours: {most_recorded_hours_year}")


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
        # task_1(db)
        # task_2(db)
        # task_3(db)
        # task_4(db)
        # task_5(db)
        # task_6(db)
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

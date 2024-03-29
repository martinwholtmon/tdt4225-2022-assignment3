import itertools
import time
import pandas as pd
import pprint as pp
from tabulate import tabulate
from haversine import haversine, Unit
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
    pipeline.append({"$sort": {"nr_activities": -1}})
    pipeline.append({"$limit": 20})  # Get top 20

    # Query
    ret = db.aggregate("User", pipeline)

    # Print
    print("\nTask 3")
    print("Top 20 users with the highest number of activities: ")
    pp.pprint(list(ret))


def task_4(db: DbHandler):
    """Find all users who have taken a taxi."""
    collection = "User"
    query = {"activities.transportation_mode": "taxi"}
    fields = {"_id": 1}
    ret = db.find_documents(collection_name=collection, query=query, fields=fields)

    # Print
    print("\nTask 4")
    print("All users who have taken a taxi:")
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
    print("Number of activities for the different transportation modes")
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
    pipeline.append({"$sort": {"count": -1}})
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
    # Get year with most activities
    pipeline = []

    # get year
    pipeline.append(
        {
            "$addFields": {
                "year": {"$year": "$start_date_time"},
            }
        }
    )

    # Match
    pipeline.append(
        {
            "$match": {
                "user_id": "112",
                "transportation_mode": "walk",
                "year": 2008,
            }
        }
    )

    # Get lat/lon for each activity
    pipeline.append(
        {
            "$lookup": {
                "from": "TrackPoint",
                "localField": "_id",
                "foreignField": "activity_id",
                "pipeline": [
                    {
                        "$project": {
                            "_id": 0,
                            "lat": 1,
                            "lon": 1,
                        }
                    },
                ],
                "as": "trackpoints",
            }
        }
    )

    # only keep trackpoints
    pipeline.append({"$project": {"trackpoints": 1}})

    # Query
    ret = db.aggregate("Activity", pipeline)

    # Calculate
    distance = 0.0
    for activity in ret:
        old_track_point = None
        for track_point in activity["trackpoints"]:
            # Calculate the distance between the trackpoints (coordinates)
            # And add it to the total distance
            if old_track_point is not None:
                distance += haversine(
                    (old_track_point["lat"], old_track_point["lon"]),
                    (track_point["lat"], track_point["lon"]),
                    unit=Unit.KILOMETERS,
                )
            old_track_point = track_point

    # Print
    print("\nTask 7")
    print(f"User 112 walked {round(distance, 3)} km in 2008")


def task_8(db: DbHandler):
    """Find the top 20 users who have gained the most altitude meters"""
    fields = {"_id": 0, "user_id": 1, "activity_id": 1, "altitude": 1}
    ret = db.find_documents(collection_name="TrackPoint", fields=fields)

    # Calculate
    altitude = {}
    current_aid = -1
    old_alt = -1
    for tp in ret:
        uid = tp["user_id"]
        aid = tp["activity_id"]
        alt = tp["altitude"]

        # Same activity
        if aid == current_aid:
            # Not invalid + new alt is higher
            if old_alt < alt and alt != -777 and old_alt != -777:
                diff = alt - old_alt
                # add altitude
                altitude[uid] = (
                    altitude[uid] + diff if altitude.get(uid) is not None else diff
                )
        else:
            # New activity
            current_aid = aid
        old_alt = alt

    # Sort dict
    # source: https://stackoverflow.com/a/2258273
    altitude = dict(sorted(altitude.items(), key=lambda x: x[1], reverse=True))
    top_users = dict(itertools.islice(altitude.items(), 20))

    # Print
    print("\nTask 8")
    print(
        f"The 20 users who gained the most altitude meters is: \n{tabulate_dict(top_users, ['User', 'Gained Altitude (m)'])}"
    )


def task_9(db: DbHandler):
    """Find all users who have invalid activities, and the number of invalid activities per user
    An invalid activity is defined as an activity with consecutive
    trackpoints where the timestamps deviate with at least 5 minutes.
    """
    fields = {"_id": 0, "user_id": 1, "activity_id": 1, "date_time": 1}
    ret = db.find_documents(collection_name="TrackPoint", fields=fields)

    # Find users
    users = {}
    curr_aid = -1
    old_dt = None
    for tp in ret:
        uid, aid, dt = tp["user_id"], tp["activity_id"], tp["date_time"]
        # If same activity
        if aid == curr_aid:
            # Calulate the time between the trackpoints in minutes
            diff = divmod((dt - old_dt).total_seconds(), 60)[0]
            if diff >= 5:
                users[uid] = users[uid] + 1 if users.get(uid) is not None else 1
        else:
            curr_aid = aid
        old_dt = dt

    # Print
    print("\nTask 9")
    print(
        f"Users with invalid activities: \n{tabulate_dict(users, ['User', 'Invalid Activities'])}"
    )


def task_10(db: DbHandler):
    """Find the users who have tracked an activity in the Forbidden City of Beijing.
    the Forbidden City: lat 39.916, lon 116.397
    """
    pipeline = []

    # Round lat/lon values
    pipeline.append(
        {
            "$project": {
                "user_id": 1,
                "lat": {"$round": ["$lat", 3]},
                "lon": {"$round": ["$lon", 3]},
            }
        }
    )

    # match on the forbidden city
    pipeline.append({"$match": {"lat": 39.916, "lon": 116.397}})
    pipeline.append({"$group": {"_id": "$user_id"}})  # find users

    # Query
    res = db.aggregate("TrackPoint", pipeline)

    # Print
    print("\nTask 10")
    print("Users that have visited 'the Forbidden City':")
    pp.pprint(list(res))


def task_11(db: DbHandler):
    """Find all users who have registered transportation_mode and their most used transportation_mode."""
    pipeline = []

    # Get users with labels and undwind the activities
    pipeline.append({"$match": {"has_label": True}})
    pipeline.append({"$unwind": "$activities"})

    # Remove null values
    pipeline.append(
        {"$match": {"activities.transportation_mode": {"$exists": True, "$ne": None}}}
    )

    # only keep user_id and transportation mode
    pipeline.append({"$project": {"_id": 1, "activities.transportation_mode": 1}})

    # group on user_id and transportation mode, and count instances
    pipeline.append(
        {
            "$group": {
                "_id": {
                    "user_id": "$_id",
                    "transportation_mode": "$activities.transportation_mode",
                },
                "count": {"$sum": 1},
            }
        }
    )

    # Sort
    pipeline.append({"$sort": {"_id.user_id": 1, "count": -1}})

    # Group results per user
    pipeline.append(
        {
            "$group": {
                "_id": "$_id.user_id",
                "transportation": {
                    "$push": {
                        "transportation_mode": "$_id.transportation_mode",
                        "count": "$count",
                    }
                },
            }
        }
    )

    # Get top element in each group (most used activity) per user_id
    pipeline.append(
        {
            "$project": {
                "transportation": {"$first": "$transportation"},
            }
        }
    )

    # only get transportation mode, ignore count
    pipeline.append(
        {
            "$project": {
                "transportation": "$transportation.transportation_mode",
            }
        }
    )

    # sort by user_id
    pipeline.append({"$sort": {"_id": 1}})

    # Query
    res = db.aggregate("User", pipeline)

    # Print
    print("\nTask 11")
    print(
        "All users who have registered transportation_mode and their most used transportation_mode:"
    )
    pp.pprint(list(res))


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
        start = time.time()

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

        end = time.time()
        print(f"Time used: {end - start}")

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if db:
            db.connection.close_connection()


if __name__ == "__main__":
    main()

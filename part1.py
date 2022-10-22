"""This file solves the part 1 of assignment 3:
Cleaning and inserting of the dataset into a mongodb database

Raises:
    ValueError: Activity was not inserted
"""
import os
import time
from datetime import datetime
from DbHandler import DbHandler
from FileHandler import read_data_file, read_labeled_users_file, read_user_labels_file
from structs import User, Activity, TrackPoint


def parse_and_insert_dataset(db: DbHandler, stop_at_user=""):
    """Will parse the dataset and insert the users,
    the activities and all the trackpoints for each activity.

    Args:
        program (DbHandler): the database
    """
    path_to_dataset = os.path.join("./dataset")

    labeled_ids = read_labeled_users_file(
        os.path.join(path_to_dataset, "labeled_ids.txt")
    )
    user = ""
    labels = {}
    has_labels = False
    for root, dirs, files in os.walk(os.path.join(path_to_dataset, "Data")):
        # New user?
        # Directory has a "Trajectory" folder,
        # meaning we are in a new users directory
        if len(dirs) > 0 and dirs[0] == "Trajectory":
            user = os.path.normpath(root).split(os.path.sep)[-1]

            # Partial insert, 0..stop_at_user-1
            if user == stop_at_user:
                return

            # Get labels
            if user in labeled_ids and files[0] == "labels.txt":
                labels = read_user_labels_file(os.path.join(root, files[0]))
                has_labels = True
            else:
                has_labels = False

            # insert user into db
            print(f"Inserting user {user}")
            user_objectid = db.insert_documents(
                "User", [User(user, has_labels, []).__dict__]
            )[0]

        # Insert activities with Trajectory data
        # In "Trajectory" directory
        if os.path.normpath(root).split(os.path.sep)[-1] == "Trajectory":
            activities = []
            for file in files:
                activity_with_transportation_mode = insert_trajectory(
                    db,
                    user_objectid,
                    root,
                    file,
                    has_labels,
                    labels,
                )
                if activity_with_transportation_mode is not None:
                    activities.append(activity_with_transportation_mode)

            # Update user with activities
            data_to_update = {"activities": activities}
            db.update_document("User", user_objectid, data_to_update)


def insert_trajectory(db: DbHandler, user_id, root, file, has_labels, labels):
    """Insert activities with trackpoint data

    Args:
        db (DbHandler): The database
        user_id (str): Id of the user
        root (str): Path to directory
        file (str): Name of current file (activity)
        has_labels (bool): User has labeled activities
        labels (dict): Labeled activities

    Raises:
        ValueError: If the insertion of activity failed
    """
    path = os.path.join(root, file)
    data = read_data_file(path)[6:]

    # Check file size
    if len(data) > 2500:
        return None

    # Insert Activity
    activity_id, transportation_mode = insert_activity(
        db, user_id, file, data, has_labels, labels
    )
    if len(activity_id) == 0:
        raise ValueError(f"Activity {path} was not inserted!")
    else:
        activity_id = activity_id[0]

    # Prepare Trackpoints
    trackpoints = []
    for trackpoint in data:
        lat = float(trackpoint[0])
        lon = float(trackpoint[1])
        altitude = int(round(float(trackpoint[3])))
        date_days = float(trackpoint[4])
        date_time = get_datetime_format(trackpoint[5], trackpoint[6])

        # Append trackpoint
        trackpoints.append(
            TrackPoint(
                user_id, activity_id, lat, lon, altitude, date_days, date_time
            ).__dict__
        )

    # Insert Trackpoints
    _ = db.insert_documents("TrackPoint", trackpoints)

    # return activity with transportation_mode
    return {"_id": activity_id, "transportation_mode": transportation_mode}


def insert_activity(db: DbHandler, user_id, file, data, has_labels, labels):
    """Insert an activity into the database

    Args:
        db (DbHandler): The database
        user_id (str): The id of the user
        file (str): Filename of the activity
        data (list[list]): All the trackpoints for the activity
        has_labels (bool): User has labeled activities
        labels (dict): Labeled activities

    Returns:
        list: ObjectID of inserted data. In this case, only one element
        str | None: Transportation mode
    """
    # Prepare activity
    start_date_time = get_datetime_format(data[0][5], data[0][6])
    end_date_time = get_datetime_format(data[-1][5], data[-1][6])

    # Match Transportation mode
    transportation_mode = None
    if has_labels:
        # Get activity from dict
        activity = labels.get(os.path.splitext(file)[0])  # Match start time on filename
        if activity is not None:
            # Match end time
            if get_datetime_format(activity[2], activity[3]) == end_date_time:
                transportation_mode = activity[4]

    # Insert
    activity = Activity(user_id, transportation_mode, start_date_time, end_date_time)

    ids = db.insert_documents("Activity", [activity.__dict__])
    return ids, transportation_mode


def get_datetime_format(date, the_time) -> datetime:
    """Convert the date and time to datetime format

    Args:
        date (str): the date
        time (str): time

    Returns:
        datetime: the date and time
    """
    return datetime.strptime(
        str(date).replace("/", "-") + " " + str(the_time), "%Y-%m-%d %H:%M:%S"
    )


def main():
    db = None
    try:
        db = DbHandler()

        # Clear DB
        db.drop_all_coll()

        # Create collections
        db.create_coll("User")
        db.create_coll("Activity")
        db.create_coll("TrackPoint")
        print(db.get_coll())  # Print collections

        # Insert data
        start = time.time()
        parse_and_insert_dataset(db)
        end = time.time()
        print(f"Time used: {end - start}")

        # Fetch documents
        # print(db.fetch_documents("User"))
        # print(db.fetch_documents("Activity"))
        # print(db.fetch_documents("TrackPoint"))

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if db:
            db.connection.close_connection()


if __name__ == "__main__":
    main()

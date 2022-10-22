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

    # Iterate over the dataset
    for root, dirs, files in os.walk(os.path.join(path_to_dataset, "Data")):
        # New user
        if len(dirs) > 0 and dirs[0] == "Trajectory":
            user, labels = get_new_user(root, labeled_ids, files)

            # Partial insert, 0..stop_at_user-1
            if user == stop_at_user:
                return

            # See if it has labels
            if labels is None:
                has_labels = False
            else:
                has_labels = True

            # insert user into db
            print(f"Inserting user {user}")
            user_objectid = db.insert_documents(
                "User", [User(user, has_labels, []).__dict__]
            )[0]

        # Insert activities with Trajectory data for the user
        if os.path.normpath(root).split(os.path.sep)[-1] == "Trajectory":
            activities = []
            for file in files:
                activity_with_transportation_mode = insert_trajectory(
                    db,
                    user_objectid,
                    root,
                    file,
                    labels,
                )
                if activity_with_transportation_mode is not None:
                    activities.append(activity_with_transportation_mode)

            # Update user with activities
            data_to_update = {"activities": activities}
            db.update_document("User", user_objectid, data_to_update)


def get_new_user(root, labeled_ids, files):
    """Find the new user_id, and their labeled activities if there is any.

    Args:
        root (str): path to users directory
        labeled_ids (list): all users that have labeled their activities
        files (list[str]): all the files in current directory

    Returns:
        str: id of the user
        dict | None: labeled activities
    """
    # Find user
    user = os.path.normpath(root).split(os.path.sep)[-1]

    # Get labels
    labels = None
    if user in labeled_ids and files[0] == "labels.txt":
        labels = read_user_labels_file(os.path.join(root, files[0]))
    return user, labels


def insert_trajectory(db: DbHandler, user_id, root, file, labels):
    """Insert activities with trackpoint data

    Args:
        db (DbHandler): The database
        user_id (str): Id of the user
        root (str): Path to directory
        file (str): Name of current file (activity)
        labels (dict): Labeled activities

    Raises:
        ValueError: If the insertion of activity failed

    Returns:
        dict: id of activity with transportation mode
    """
    path = os.path.join(root, file)
    data = read_data_file(path)[6:]

    # Check file size
    if len(data) > 2500:
        return None

    # Insert Activity
    activity_id, transportation_mode = insert_activity(db, user_id, file, data, labels)
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


def insert_activity(db: DbHandler, user_id, file, data, labels):
    """Insert an activity into the database

    Args:
        db (DbHandler): The database
        user_id (str): The id of the user
        file (str): Filename of the activity
        data (list[list]): All the trackpoints for the activity
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
    if labels is not None:
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

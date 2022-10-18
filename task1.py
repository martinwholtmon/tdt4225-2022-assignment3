from DbHandler import DbHandler
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

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if db:
            db.connection.close_connection()


if __name__ == "__main__":
    main()

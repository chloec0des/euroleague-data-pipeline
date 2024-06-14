import os

def create_directories():
    HOME = os.path.expanduser('~')
    DATALAKE_ROOT_FOLDER = os.path.join(HOME, "datalake")

    directories = [
        os.path.join(DATALAKE_ROOT_FOLDER, "raw/euroleague"),
        os.path.join(DATALAKE_ROOT_FOLDER, "formatted/euroleague")
    ]

    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
        print(f"Directory created: {directory}")

create_directories()
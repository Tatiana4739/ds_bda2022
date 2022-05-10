import csv
import random
import string

HEADER_ATTENDANCE = ["university", "id", "timestamp", "event_type", "year"]
HEADER_PUBLICATIONS = ["university", "id", "publication", "year"]
MAX_ROWS = 1_000

UNIVERSITIES = ["MePhi", "MSU", "MIPT", "HSE", "MISIS", "RUDN", "BMSTU"]
START_YEAR = 2008
END_YEAR = 2022
ENTRANCE_EVENT = 0
EXIT_EVENT = 1

if __name__ == '__main__':

    user_info = []
    with open("attendance.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(HEADER_ATTENDANCE)

        for i in range(MAX_ROWS):
            university = random.choice(UNIVERSITIES)
            user_id = random.randint(0, 50_000)
            user_info.append((user_id, university))

            entrances_by_years = random.randint(1, 10)
            for _ in range(entrances_by_years):
                timestamp = random.randint(10_000, 1_000_000)
                event_type = ENTRANCE_EVENT
                year = random.randint(START_YEAR, END_YEAR)

                writer.writerow([university, user_id, timestamp, event_type, year])

                timestamp = random.randint(timestamp, 1_000_000)
                event_type = EXIT_EVENT

                writer.writerow([university, user_id, timestamp, event_type, year])

    with open("publications.csv", "w") as f1:
        writer = csv.writer(f1)
        writer.writerow(HEADER_PUBLICATIONS)

        for i in range(MAX_ROWS):
            user_id, university = random.choice(user_info)
            publications_num = random.randint(1, 30)
            for _ in range(publications_num):
                publication_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
                year = random.randint(START_YEAR, END_YEAR)
                writer.writerow([university, user_id, publication_id, year])

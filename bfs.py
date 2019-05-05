from api import parse_user_profile, parse_user_followees, parse_answer_authors, parse_topic_essence, parse_top_questions
import multiprocessing
import requests
import pymongo
import os, time, threading

def produce(e, q, session, seed, visited):
    while not e.is_set():
        cur = seed.get()
        for page in parse_user_followees(session, user=cur):
            for uid in page:
                if uid not in visited:
                    q.put(uid)
                    seed.put(uid)
                    visited.add(uid)


def consume(e, q, session, db):
    while not e.is_set():
        profile = parse_user_profile(session, user=q.get())
        if profile is not None:
            try:
                db.insert_one(profile)
            except:
                pass

if __name__ == '__main__':
    e = multiprocessing.Event()
    q = multiprocessing.Queue()
    seed = multiprocessing.Queue()
    visited = set()

    with pymongo.MongoClient('localhost', 27017) as client:
        collection = client.zhihu.user
        collection.create_index([('userid', pymongo.ASCENDING)], unique=True)
        for it in collection.find():
            seed.put(it['userid'])
            visited.add(it['userid'])

        with requests.Session() as session:
            session.headers.update({
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36',
                'cookie': '_xsrf=34QBKlbYsWOa6pAv9Be9KA5MibA9Tj3I; _zap=85112886-e384-4a1a-b135-011d3faaf031; d_c0="AJAoz-h5Xw-PToxSV6_It8I2WlOwe_1IzaI=|1556885337"; capsion_ticket="2|1:0|10:1556885513|14:capsion_ticket|44:YjJlMTI2OWZmOGFlNDhmNGIxYTJkMWFhYjhjMmRjOWE=|fbc19dd00f446960807b812465a894c68dcc12f377e3d5fd6ab7b503f2241e7d"; z_c0="2|1:0|10:1556885646|4:z_c0|92:Mi4xQ2NPUERRQUFBQUFBa0NqUDZIbGZEeVlBQUFCZ0FsVk5qWDY1WFFBYTF1aTRERUNfcVVYd08xR3RxelZEY0psWGpn|ec7bc86c4cba889b4e598f6bd6028812ebc65723eb12d228c7a17b5bff50f6bf"; unlock_ticket="ALCiAV72qg4mAAAAYAJVTZU3zFwav-qluK06tGZsEiR5olvIIZxOZg=="; tst=r; q_c1=920ae1dd7bcc4bbaa83e98cc8d67f4e3|1556885669000|1556885669000; __gads=ID=0877452f834f2e4e:T=1556885695:S=ALNI_MbvyTk2cHW924u9UfAuD_oPJ9noGA; tgw_l7_route=578107ff0d4b4f191be329db6089ff48; anc_cap_id=262c4088a9d64f0eb5c7b6d509cbc625'
            })

            for _ in range(5):
                threading.Thread(target=produce, args=(e, q, session, seed, visited)).start()
                threading.Thread(target=consume, args=(e, q, session, collection)).start()

            try:
                while True:
                    print(f'q.size() = {q.qsize()}')
                    print(f'seed.size() = {seed.qsize()}')
                    time.sleep(1)
            except KeyboardInterrupt:
                e.set()
                print('ctrl c')
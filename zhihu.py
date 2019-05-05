from api import parse_user_profile, parse_user_followees, parse_answer_authors, parse_topic_essence, parse_top_questions
import multiprocessing
import requests
import pymongo
import os, time, threading

headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36',
    'cookie': '_xsrf=34QBKlbYsWOa6pAv9Be9KA5MibA9Tj3I; _zap=85112886-e384-4a1a-b135-011d3faaf031; d_c0="AJAoz-h5Xw-PToxSV6_It8I2WlOwe_1IzaI=|1556885337"; capsion_ticket="2|1:0|10:1556885513|14:capsion_ticket|44:YjJlMTI2OWZmOGFlNDhmNGIxYTJkMWFhYjhjMmRjOWE=|fbc19dd00f446960807b812465a894c68dcc12f377e3d5fd6ab7b503f2241e7d"; z_c0="2|1:0|10:1556885646|4:z_c0|92:Mi4xQ2NPUERRQUFBQUFBa0NqUDZIbGZEeVlBQUFCZ0FsVk5qWDY1WFFBYTF1aTRERUNfcVVYd08xR3RxelZEY0psWGpn|ec7bc86c4cba889b4e598f6bd6028812ebc65723eb12d228c7a17b5bff50f6bf"; unlock_ticket="ALCiAV72qg4mAAAAYAJVTZU3zFwav-qluK06tGZsEiR5olvIIZxOZg=="; tst=r; q_c1=920ae1dd7bcc4bbaa83e98cc8d67f4e3|1556885669000|1556885669000; __gads=ID=0877452f834f2e4e:T=1556885695:S=ALNI_MbvyTk2cHW924u9UfAuD_oPJ9noGA; tgw_l7_route=578107ff0d4b4f191be329db6089ff48; anc_cap_id=262c4088a9d64f0eb5c7b6d509cbc625'
}

def questions(e, qc, qq, *topics):
    def worker(e, session, topic, parser):
        total = 0
        for page in parser(session, topic=topic):
            total += len(page)
            for qid in page:
                with qc:
                    while qq.full() and not e.is_set():
                        qc.wait()
                    if e.is_set():
                        break
                    qq.put(qid)
                    qc.notify_all()
            if e.is_set():
                break
        print(f'totally mined {total} questions for topic {topic}')
    
    with requests.Session() as session:
        session.headers.update(headers)
        
        threads = []
        for topic in topics:
            t1 = threading.Thread(target=worker, args=(e, session, topic, parse_topic_essence))
            t2 = threading.Thread(target=worker, args=(e, session, topic, parse_top_questions))
            threads.append(t1)
            threads.append(t2)
            t1.start()
            t2.start()
        
        try:
            while any(t.is_alive() for t in threads):
                time.sleep(1)
        except KeyboardInterrupt:
            with qc:
                e.set()
                qc.notify_all()

def authors(e, qc, qq, uc, uq):
    def worker(e, session, question):
        for page in parse_answer_authors(session, question=question):
            for uid in page:
                with uc:
                    while uq.full() and not e.is_set():
                        uc.wait()
                    if e.is_set():
                        break
                    uq.put(uid)
                    uc.notify_all()
            if e.is_set():
                break

    with requests.Session() as session:
        session.headers.update(headers)

        try:
            while not e.is_set():
                with qc:
                    while qq.empty() and not e.is_set():
                        qc.wait()
                    if e.is_set():
                        break
                    threading.Thread(target=worker, args=(e, session, qq.get())).start()
        except KeyboardInterrupt:
            with uc:
                e.set()
                uc.notify_all()

def users(e, uc, uq):
    def worker(collection, session, user):
        profile = parse_user_profile(session, user=user)
        if profile is not None:
            try:
                collection.insert_one(profile)
            except:
                pass

    with requests.Session() as session:
        session.headers.update(headers)

        count = 0
        
        try:
            with pymongo.MongoClient('localhost', 27017) as client:
                collection = client.zhihu.user
                collection.create_index([('userid', pymongo.ASCENDING)], unique=True)
                while not e.is_set():
                    with uc:
                        while uq.empty() and not e.is_set():
                            uc.wait()
                        if e.is_set():
                            break
                        count += 1
                        threading.Thread(target=worker, args=(collection, session, uq.get())).start()
        except KeyboardInterrupt:
            pass

        print(f'totally parsed {count} users')

if __name__ == '__main__':
    e = multiprocessing.Event()
    qc = multiprocessing.Condition()
    uc = multiprocessing.Condition()
    qq = multiprocessing.Queue()
    uq = multiprocessing.Queue()

    jobs = (
        multiprocessing.Process(target=questions, args=(e, qc, qq, 19550517, 19552330, 19552079, 19553510, 19559450, 19567481, 19551769)),
        multiprocessing.Process(target=authors, args=(e, qc, qq, uc, uq)),
        multiprocessing.Process(target=users, args=(e, uc, uq))
    )
    
    for job in jobs:
        job.start()
    
    try:
        for job in jobs:
            job.join()
    except KeyboardInterrupt:
        print(f'{multiprocessing.current_process().name} ctrl c')
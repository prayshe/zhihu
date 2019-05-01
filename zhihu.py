from api import parse_user_profile, parse_user_followees, parse_answer_authors, parse_topic_essence, parse_top_questions
import multiprocessing
import requests
import itertools
import os, time, threading

def questions(e, qc, qq, *topics):
    def worker(e, session, topic, parser):
        for page in parser(session, topic=topic):
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
    
    with requests.Session() as session:
        session.headers.update({
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36',
            'cookie': 'tgw_l7_route=80f350dcd7c650b07bd7b485fcab5bf7; _xsrf=8WgJFvMzvXGII9bERhasbhUo1aAOY4fZ; _zap=237249fe-3997-4288-a8fb-92aefd58f1fc; d_c0="AKBoYAvIXA-PTqMisjsjaKDFOdx6u1SUJNI=|1556704493"; capsion_ticket="2|1:0|10:1556704537|14:capsion_ticket|44:Nzc0NDQyMGY1NjY1NDE3NzgzNGI1MmI1ZjEwMjk3ODg=|9a9f4f37ac5a515f0b70daa078bd1434450ef53100afead185770527a8b60e57"; z_c0="2|1:0|10:1556704546|4:z_c0|92:Mi4xQ2NPUERRQUFBQUFBb0doZ0M4aGNEeVlBQUFCZ0FsVk5JcnUyWFFERmpEbUY2MkY2RWdlT1l1YWJXOWc4WUxFNGVR|221f911275f9b2c6df3c6d29c896dcada42d40a3ff5fb8b8e8c42cab6bc60b16"; anc_cap_id=638f6596fb5a4c5185e9b60f63e940b5; tst=r; q_c1=30cd5839d2944d5eaa6dfa22bd023b29|1556704558000|1556704558000; __utma=51854390.1998127898.1556704575.1556704575.1556704575.1; __utmb=51854390.0.10.1556704575; __utmc=51854390; __utmz=51854390.1556704575.1.1.utmcsr=zhihu.com|utmccn=(referral)|utmcmd=referral|utmcct=/; __utmv=51854390.100--|2=registration_date=20181214=1^3=entry_date=20181214=1; unlock_ticket="ALCiAV72qg4mAAAAYAJVTbd0yVz9x7-4HnC8lYq2zfMNdKKRCaaJoA=="; __gads=ID=428d71cf8eef109d:T=1556704877:S=ALNI_MYe60OBf-K6HH7vocWQl3IuWZcV1g'
        })
        
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
        session.headers.update({
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36',
            'cookie': 'tgw_l7_route=80f350dcd7c650b07bd7b485fcab5bf7; _xsrf=8WgJFvMzvXGII9bERhasbhUo1aAOY4fZ; _zap=237249fe-3997-4288-a8fb-92aefd58f1fc; d_c0="AKBoYAvIXA-PTqMisjsjaKDFOdx6u1SUJNI=|1556704493"; capsion_ticket="2|1:0|10:1556704537|14:capsion_ticket|44:Nzc0NDQyMGY1NjY1NDE3NzgzNGI1MmI1ZjEwMjk3ODg=|9a9f4f37ac5a515f0b70daa078bd1434450ef53100afead185770527a8b60e57"; z_c0="2|1:0|10:1556704546|4:z_c0|92:Mi4xQ2NPUERRQUFBQUFBb0doZ0M4aGNEeVlBQUFCZ0FsVk5JcnUyWFFERmpEbUY2MkY2RWdlT1l1YWJXOWc4WUxFNGVR|221f911275f9b2c6df3c6d29c896dcada42d40a3ff5fb8b8e8c42cab6bc60b16"; anc_cap_id=638f6596fb5a4c5185e9b60f63e940b5; tst=r; q_c1=30cd5839d2944d5eaa6dfa22bd023b29|1556704558000|1556704558000; __utma=51854390.1998127898.1556704575.1556704575.1556704575.1; __utmb=51854390.0.10.1556704575; __utmc=51854390; __utmz=51854390.1556704575.1.1.utmcsr=zhihu.com|utmccn=(referral)|utmcmd=referral|utmcct=/; __utmv=51854390.100--|2=registration_date=20181214=1^3=entry_date=20181214=1; unlock_ticket="ALCiAV72qg4mAAAAYAJVTbd0yVz9x7-4HnC8lYq2zfMNdKKRCaaJoA=="; __gads=ID=428d71cf8eef109d:T=1556704877:S=ALNI_MYe60OBf-K6HH7vocWQl3IuWZcV1g'
        })

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
    def worker(session, user):
        profile = parse_user_profile(session, user=user)
        print(profile)

    with requests.Session() as session:
        session.headers.update({
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36',
            'cookie': 'tgw_l7_route=80f350dcd7c650b07bd7b485fcab5bf7; _xsrf=8WgJFvMzvXGII9bERhasbhUo1aAOY4fZ; _zap=237249fe-3997-4288-a8fb-92aefd58f1fc; d_c0="AKBoYAvIXA-PTqMisjsjaKDFOdx6u1SUJNI=|1556704493"; capsion_ticket="2|1:0|10:1556704537|14:capsion_ticket|44:Nzc0NDQyMGY1NjY1NDE3NzgzNGI1MmI1ZjEwMjk3ODg=|9a9f4f37ac5a515f0b70daa078bd1434450ef53100afead185770527a8b60e57"; z_c0="2|1:0|10:1556704546|4:z_c0|92:Mi4xQ2NPUERRQUFBQUFBb0doZ0M4aGNEeVlBQUFCZ0FsVk5JcnUyWFFERmpEbUY2MkY2RWdlT1l1YWJXOWc4WUxFNGVR|221f911275f9b2c6df3c6d29c896dcada42d40a3ff5fb8b8e8c42cab6bc60b16"; anc_cap_id=638f6596fb5a4c5185e9b60f63e940b5; tst=r; q_c1=30cd5839d2944d5eaa6dfa22bd023b29|1556704558000|1556704558000; __utma=51854390.1998127898.1556704575.1556704575.1556704575.1; __utmb=51854390.0.10.1556704575; __utmc=51854390; __utmz=51854390.1556704575.1.1.utmcsr=zhihu.com|utmccn=(referral)|utmcmd=referral|utmcct=/; __utmv=51854390.100--|2=registration_date=20181214=1^3=entry_date=20181214=1; unlock_ticket="ALCiAV72qg4mAAAAYAJVTbd0yVz9x7-4HnC8lYq2zfMNdKKRCaaJoA=="; __gads=ID=428d71cf8eef109d:T=1556704877:S=ALNI_MYe60OBf-K6HH7vocWQl3IuWZcV1g'
        })
        
        try:
            while not e.is_set():
                with uc:
                    while uq.empty() and not e.is_set():
                        uc.wait()
                    if e.is_set():
                        break
                    threading.Thread(target=worker, args=(session, uq.get())).start()
        except KeyboardInterrupt:
            pass

if __name__ == '__main__':
    e = multiprocessing.Event()
    qc = multiprocessing.Condition()
    uc = multiprocessing.Condition()
    qq = multiprocessing.Queue(10)
    uq = multiprocessing.Queue(10)

    jobs = (
        multiprocessing.Process(target=questions, args=(e, qc, qq, 19565870, 20004712)),
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
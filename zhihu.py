from api import parse_user_profile, parse_user_followees, parse_answer_authors, parse_topic_essence, parse_top_questions
import multiprocessing
import requests
import itertools
import os, time, threading

def questions(qc, qq, *topics):
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
            'cookie': 'tgw_l7_route=116a747939468d99065d12a386ab1c5f; _zap=300a91cb-c2b1-4b92-b412-deabf49706b7; _xsrf=ZRVaCagbnsHzZPdFE8EBOrwlZkwiKCUh; d_c0="AGCnoam7Ww-PTse1DReD5N5OjVnMZZSxCGU=|1556634139"; capsion_ticket="2|1:0|10:1556634219|14:capsion_ticket|44:Zjk3ZDFlYmNmYTViNDAwNjlmNTQ2NmQzNDI3MWJlMzI=|6c17f07a78ef18caceb8e48ba278f2633fe75bc1409c4de643ca10ba6ea8545f"; z_c0="2|1:0|10:1556634275|4:z_c0|92:Mi4xQ2NPUERRQUFBQUFBWUtlaHFidGJEeVlBQUFCZ0FsVk5vNmkxWFFCNTFKRWdXWGpJQVVPSkxOYV9hQW9sNGNmaW1B|60a9c09d6a5c812305bc85eb37f4cddc2d43321a01c0fac851c18c600730d0ea"; unlock_ticket="ALCiAV72qg4mAAAAYAJVTathyFygJOY_CdBWNFynhQ8iVJ-wSBLdqw=="; tst=r; q_c1=2db0e583774b42c297245811e3d4d5d4|1556634297000|1556634297000; __gads=ID=4c59ee54405ec6ec:T=1556634299:S=ALNI_MYJmiozRatuJ0Ji418cK1P-puYGtg'
        })
        
        e, threads = threading.Event(), []
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

def authors(qc, qq, uc, uq):
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
            'cookie': 'tgw_l7_route=116a747939468d99065d12a386ab1c5f; _zap=300a91cb-c2b1-4b92-b412-deabf49706b7; _xsrf=ZRVaCagbnsHzZPdFE8EBOrwlZkwiKCUh; d_c0="AGCnoam7Ww-PTse1DReD5N5OjVnMZZSxCGU=|1556634139"; capsion_ticket="2|1:0|10:1556634219|14:capsion_ticket|44:Zjk3ZDFlYmNmYTViNDAwNjlmNTQ2NmQzNDI3MWJlMzI=|6c17f07a78ef18caceb8e48ba278f2633fe75bc1409c4de643ca10ba6ea8545f"; z_c0="2|1:0|10:1556634275|4:z_c0|92:Mi4xQ2NPUERRQUFBQUFBWUtlaHFidGJEeVlBQUFCZ0FsVk5vNmkxWFFCNTFKRWdXWGpJQVVPSkxOYV9hQW9sNGNmaW1B|60a9c09d6a5c812305bc85eb37f4cddc2d43321a01c0fac851c18c600730d0ea"; unlock_ticket="ALCiAV72qg4mAAAAYAJVTathyFygJOY_CdBWNFynhQ8iVJ-wSBLdqw=="; tst=r; q_c1=2db0e583774b42c297245811e3d4d5d4|1556634297000|1556634297000; __gads=ID=4c59ee54405ec6ec:T=1556634299:S=ALNI_MYJmiozRatuJ0Ji418cK1P-puYGtg'
        })
        
        e = threading.Event()

        try:
            while True:
                with qc:
                    while qq.empty():
                        qc.wait()
                    threading.Thread(target=worker, args=(e, session, qq.get())).start()
        except KeyboardInterrupt:
            with uc:
                e.set()
                uc.notify_all()

def users(uc, uq):
    def worker(session, user):
        profile = parse_user_profile(session, user=user)
        print(profile)

    with requests.Session() as session:
        session.headers.update({
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36',
            'cookie': 'tgw_l7_route=116a747939468d99065d12a386ab1c5f; _zap=300a91cb-c2b1-4b92-b412-deabf49706b7; _xsrf=ZRVaCagbnsHzZPdFE8EBOrwlZkwiKCUh; d_c0="AGCnoam7Ww-PTse1DReD5N5OjVnMZZSxCGU=|1556634139"; capsion_ticket="2|1:0|10:1556634219|14:capsion_ticket|44:Zjk3ZDFlYmNmYTViNDAwNjlmNTQ2NmQzNDI3MWJlMzI=|6c17f07a78ef18caceb8e48ba278f2633fe75bc1409c4de643ca10ba6ea8545f"; z_c0="2|1:0|10:1556634275|4:z_c0|92:Mi4xQ2NPUERRQUFBQUFBWUtlaHFidGJEeVlBQUFCZ0FsVk5vNmkxWFFCNTFKRWdXWGpJQVVPSkxOYV9hQW9sNGNmaW1B|60a9c09d6a5c812305bc85eb37f4cddc2d43321a01c0fac851c18c600730d0ea"; unlock_ticket="ALCiAV72qg4mAAAAYAJVTathyFygJOY_CdBWNFynhQ8iVJ-wSBLdqw=="; tst=r; q_c1=2db0e583774b42c297245811e3d4d5d4|1556634297000|1556634297000; __gads=ID=4c59ee54405ec6ec:T=1556634299:S=ALNI_MYJmiozRatuJ0Ji418cK1P-puYGtg'
        })
        
        try:
            while True:
                with uc:
                    while uq.empty():
                        uc.wait()
                    threading.Thread(target=worker, args=(session, uq.get())).start()
        except KeyboardInterrupt:
            pass

if __name__ == '__main__':
    qc = multiprocessing.Condition()
    uc = multiprocessing.Condition()
    qq = multiprocessing.Queue(10)
    uq = multiprocessing.Queue(10)

    jobs = (
        multiprocessing.Process(target=questions, args=(qc, qq, 19565870, 20004712)),
        multiprocessing.Process(target=authors, args=(qc, qq, uc, uq)),
        multiprocessing.Process(target=users, args=(uc, uq))
    )
    
    for job in jobs:
        job.start()
    
    try:
        for job in jobs:
            job.join()
    except KeyboardInterrupt:
        print(f'{multiprocessing.current_process().name} ctrl c')
import requests
import pymongo

'''
zhihu account
https://www.pdflibr.com/
17124267523
'''

def parse_user_profile(session, user):
    url = 'https://www.zhihu.com/api/v4/members/{user}'.format(user=user)
    params = {
        'include': 'gender, educations, employments'
    }
    response = session.get(url, params=params)
    if response.status_code == 200:
        body = response.json()
        userid = body['url_token']
        gender = body['gender']
        school = [education['school']['name'] for education in body['educations'] if 'school' in education]
        companies = [employment['company']['name'] for employment in body['employments'] if 'company' in employment]
        return {'userid': userid, 'gender': gender, 'school': school, 'companies': companies} 
    return None

def parse_user_followees(session, user, limit=500):
    url = 'https://www.zhihu.com/api/v4/members/{user}/followees'.format(user=user)
    params = {
        'offset': 0,
        'limit': 10
    }
    response = session.get(url, params=params)
    followees = []
    while response.status_code == 200 and len(followees) < limit:
        body = response.json()
        followees.extend([data['url_token'] for data in body['data']])
        if body['paging']['is_end']:
            break
        params['offset'] += params['limit']
        response = session.get(url, params=params)
    return followees

def parse_answer_authors(session, question, limit=500):
    url = 'https://www.zhihu.com/api/v4/questions/{question}/answers'.format(question=question)
    params = {
        'offset': 0,
        'limit': 10
    }
    response = session.get(url, params=params)
    authors = []
    while response.status_code == 200 and len(authors) < limit:
        body = response.json()
        authors.extend([data['author']['url_token'] for data in body['data'] if not data['is_collapsed'] and len(data['author']['url_token']) > 0])
        if body['paging']['is_end']:
            break
        params['offset'] += params['limit']
        response = session.get(url, params=params)
    return authors

def parse_topic_essence(session, topic, limit=500):
    url = 'https://www.zhihu.com/api/v4/topics/{topic}/feeds/essence'.format(topic=topic)
    params = {
        'offset': 0,
        'limit': 10
    }
    response = session.get(url, params=params)
    questions = []
    while response.status_code == 200 and len(questions) < limit:
        body = response.json()
        questions.extend([data['target']['question']['id'] for data in body['data'] if data['target']['type'] == 'answer'])
        if body['paging']['is_end']:
            break
        params['offset'] += params['limit']
        response = session.get(url, params=params)
    return questions

def parse_top_questions(session, topic, limit=500):
    url = 'https://www.zhihu.com/api/v4/topics/{topic}/feeds/top_question'.format(topic=topic)
    params = {
        'offset': 0,
        'limit': 10
    }
    response = session.get(url, params=params)
    questions = []
    while response.status_code == 200 and len(questions) < limit:
        body = response.json()
        questions.extend([data['target']['id'] for data in body['data'] if data['target']['type'] == 'question'])
        if body['paging']['is_end']:
            break
        params['offset'] += params['limit']
        response = session.get(url, params=params)
    return questions

with requests.Session() as session:
    session.headers.update({
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36',
        'cookie': 'tgw_l7_route=4860b599c6644634a0abcd4d10d37251; _zap=a060bbd8-b0da-416c-b8d2-622b0e165a7e; _xsrf=1fb6H75O6evHktKAZiM7s3mNnJrUnSpY; d_c0="AADmqsrYUg-PTgN0wWgbLgegjADaflDtxIA=|1556037795"; capsion_ticket="2|1:0|10:1556037886|14:capsion_ticket|44:ZGUzZDM3M2VhNWYwNDkxZTg2ZmQ4OWMyNTFmNmM1Nzc=|eae75da84214be6c8efdb5fde6d6d0bd18bd5832aff244e8c5b20595c1fd3bbc"; z_c0="2|1:0|10:1556037919|4:z_c0|92:Mi4xX0t6NUF3QUFBQUFBQU9hcXl0aFNEeVlBQUFCZ0FsVk5INC1zWFFBZ2Q0QlA5c1puOG1peTdvVDVpYkdYYmFLdXdR|cd81373a14a43019a7c167cdc962bab1520167917c4ae9e41021c3aeec8c598c"; unlock_ticket="ADACjozBMgsmAAAAYAJVTSdIv1znyGX2m9b-UmVJcvkZwqt1admDaw=="; q_c1=6144bbaf8b4d4248a6201704d29d18e9|1556037941000|1556037941000; __utma=51854390.204247335.1556037959.1556037959.1556037959.1; __utmb=51854390.0.10.1556037959; __utmc=51854390; __utmz=51854390.1556037959.1.1.utmcsr=zhihu.com|utmccn=(referral)|utmcmd=referral|utmcct=/; __utmv=51854390.100--|2=registration_date=20170123=1^3=entry_date=20170123=1; tst=f'
    })

    question_set = set()
    question_set.update(parse_topic_essence(session, 19565870, 10))
    question_set.update(parse_top_questions(session, 19565870, 10))

    print(len(question_set))
    
    user_set = set()
    for question in question_set:
        users = parse_answer_authors(session, question, 10)
        user_set.update(users)

    print(len(user_set))

    with pymongo.MongoClient('mongodb://localhost:27017/') as client:
        db = client['zhihu']
        col = db['user']
        for user in user_set:
            profile = parse_user_profile(session, user)
            if profile is not None:
                print(profile)
                col.insert_one(profile)
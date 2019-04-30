from abc import ABC, abstractmethod
from functools import wraps
import requests

class Call(object):
    @abstractmethod
    def __init__(self, url, params):
        self._url = url
        self._params = params

    @abstractmethod
    def __call__(self, func):
        pass

class Get(Call):
    def __init__(self, url, params):
        super().__init__(url, params)
    
    def __call__(self, func):
        @wraps(func)
        def wrapper(session, **kwargs):
            url = self._url.format(**kwargs)
            response = session.get(url, params=self._params)
            if response.status_code == 200:
                return func(session, body=response.json())
            return None
        return wrapper

class Gets(Call):
    def __init__(self, url):
        super().__init__(url, {
            'offset': 0,
            'limit': 10
        })

    def __call__(self, func):
        @wraps(func)
        def wrapper(session, **kwargs):
            url = self._url.format(**kwargs)
            response = session.get(url, params=self._params)
            while response.status_code == 200:
                body = response.json()
                yield func(session, body=response.json())
                if body['paging']['is_end']:
                    break
                self._params['offset'] += self._params['limit']
                response = session.get(url, params=self._params)
        return wrapper

@Get('https://www.zhihu.com/api/v4/members/{user}', {'include': 'gender, educations, employments'})
def parse_user_profile(session, **kwargs):
    return {
        'userid': kwargs['body']['url_token'],
        'gender': kwargs['body']['gender'],
        'school': [education['school']['name'] for education in kwargs['body']['educations'] if 'school' in education],
        'company': [employment['company']['name'] for employment in kwargs['body']['employments'] if 'company' in employment]
    }

@Gets('https://www.zhihu.com/api/v4/members/{user}/followees')
def parse_user_followees(session, **kwargs):
    return [data['url_token'] for data in kwargs['body']['data']]

@Gets('https://www.zhihu.com/api/v4/questions/{question}/answers')
def parse_answer_authors(session, **kwargs):
    return [data['author']['url_token'] for data in kwargs['body']['data'] if not data['is_collapsed'] and len(data['author']['url_token']) > 0]

@Gets('https://www.zhihu.com/api/v4/topics/{topic}/feeds/essence')
def parse_topic_essence(session, **kwargs):
    return [data['target']['question']['id'] for data in kwargs['body']['data'] if data['target']['type'] == 'answer']

@Gets('https://www.zhihu.com/api/v4/topics/{topic}/feeds/top_question')
def parse_top_questions(session, **kwargs):
    return [data['target']['id'] for data in kwargs['body']['data'] if data['target']['type'] == 'question']
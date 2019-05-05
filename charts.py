#coding:utf-8

import pymongo
import numpy as np
import matplotlib.pyplot as plt
from pylab import mpl
import inspect
import re

def count_google(collection):
    data = list(collection.aggregate(
        [
            {
                '$unwind':
                {
                    'path': '$school',
                    'includeArrayIndex': 'schoolIndex',
                    'preserveNullAndEmptyArrays': False
                }
            },
            {
                '$match':
                {
                    'company':
                    {
                        '$regex': '.*Google.*',
                        '$options': 'i'
                    }
                }
            },
            {
                '$project':
                {
                    'userid': 1,
                    'gender': 1,
                    'school': 1
                }
            },
            {
                '$group':
                {
                    '_id': '$school',
                    'count':
                    {
                        '$sum': 1
                    }
                }
            },
            {
                '$sort':
                {
                    'count': -1
                }
            },
            {
                '$limit': 15
            }
        ]
    ))

    schools = [i['_id'] for i in data]
    counts = [i['count'] for i in data]

    mpl.rcParams['font.sans-serif'] = ['FangSong']
    mpl.rcParams['axes.unicode_minus'] = False
    
    fig, ax = plt.subplots(figsize=(16,9))

    y_pos = np.arange(len(schools))
    ax.barh(y_pos, counts, color=(0.1,0.1,1.0,0.5))
    ax.set_yticks([])
    ax.invert_yaxis()
    ax.set_title('Where does most of Googlers graduate from')

    for i, school in enumerate(schools):
        ax.text(0.1, i, school, ha='left', va='center')

    fig.tight_layout()

    plt.savefig(f'results/{inspect.stack()[0][3]}.png')

def count_lastCompany(collection):
    data = list(collection.aggregate(
        [
            {
                '$unwind':
                {
                    'path': '$company',
                    'includeArrayIndex': 'companyIndex',
                    'preserveNullAndEmptyArrays': False
                }
            },
            {
                '$match':
                {
                    'company':
                    {
                        '$regex': '.*(Google)|(谷歌)|(咕果).*',
                        '$options': 'ix'
                    }
                }
            },
            {
                '$lookup':
                {
                    'from': 'user',
                    'let':
                    {
                        'uid': '$userid',
                        'cix': '$companyIndex'
                    },
                    'pipeline':
                    [
                        {
                            '$unwind':
                            {
                                'path': '$company',
                                'includeArrayIndex': 'companyIndex',
                                'preserveNullAndEmptyArrays': False
                            }
                        },
                        {
                            '$match':
                            {
                                '$expr':
                                {
                                    '$and':
                                    [
                                        {
                                            '$eq':
                                            [
                                                '$userid',
                                                '$$uid'
                                            ]
                                        },
                                        {
                                            '$gt':
                                            [
                                                '$companyIndex',
                                                '$$cix'
                                            ]
                                        }
                                    ]
                                }
                            }
                        },
                        {
                            '$project':
                            {
                                '_id': 0,
                                'company': 1
                            }
                        }
                    ],
                    'as': 'lastCompany'
                }
            },
            {
                '$unwind':
                {
                    'path': '$lastCompany',
                }
            },
            {
                '$group':
                {
                    '_id': '$lastCompany.company',
                    'count':
                    {
                        '$sum': 1
                    }
                }
            },
            {
                '$sort':
                {
                    'count': -1
                }
            },
            {
                '$limit': 15
            }
        ]
    ))

    companies = [i['_id'] for i in data]
    counts = [i['count'] for i in data]

    mpl.rcParams['font.sans-serif'] = ['FangSong']
    mpl.rcParams['axes.unicode_minus'] = False
    
    fig, ax = plt.subplots(figsize=(16,9))

    y_pos = np.arange(len(companies))
    ax.barh(y_pos, counts, color=(0.1,0.1,1.0,0.5))
    ax.set_yticks([])
    ax.invert_yaxis()
    ax.set_title('Where does most of Googlers come from?')

    for i, company in enumerate(companies):
        ax.text(0.1, i, company, ha='left', va='center')

    fig.tight_layout()

    plt.savefig(f'results/{inspect.stack()[0][3]}.png')

def count_company(collection):
    data = list(collection.aggregate(
        [
            {
                '$unwind':
                {
                    'path': '$company'
                }
            },
            {
                '$group':
                {
                    '_id': '$company',
                    'count':
                    {
                        '$sum': 1
                    }
                }
            },
            {
                '$sort':
                {
                    'count': -1
                }
            },
            {
                '$limit': 15
            }
        ]
    ))

    companies = [i['_id'] for i in data]
    counts = [i['count'] for i in data]

    mpl.rcParams['font.sans-serif'] = ['FangSong']
    mpl.rcParams['axes.unicode_minus'] = False
    
    fig, ax = plt.subplots(figsize=(16,9))

    y_pos = np.arange(len(companies))
    ax.barh(y_pos, counts, color=(0.1,0.1,1.0,0.5))
    ax.set_yticks([])
    ax.invert_yaxis()
    ax.set_title('Where does most of zhihu users come from?')

    for i, company in enumerate(companies):
        ax.text(0.1, i, company, ha='left', va='center')

    fig.tight_layout()

    plt.savefig(f'results/{inspect.stack()[0][3]}.png')

def count_nextCompany(collection):
    data = list(collection.aggregate(
        [
            {
                '$unwind':
                {
                    'path': '$company',
                    'includeArrayIndex': 'companyIndex',
                    'preserveNullAndEmptyArrays': False
                }
            },
            {
                '$match':
                {
                    'company':
                    {
                        '$regex': '.*(Microsoft)|(微软).*',
                        '$options': 'ix'
                    }
                }
            },
            {
                '$lookup':
                {
                    'from': 'user',
                    'let':
                    {
                        'uid': '$userid',
                        'cix': '$companyIndex'
                    },
                    'pipeline':
                    [
                        {
                            '$unwind':
                            {
                                'path': '$company',
                                'includeArrayIndex': 'companyIndex',
                                'preserveNullAndEmptyArrays': False
                            }
                        },
                        {
                            '$match':
                            {
                                '$expr':
                                {
                                    '$and':
                                    [
                                        {
                                            '$eq':
                                            [
                                                '$userid',
                                                '$$uid'
                                            ]
                                        },
                                        {
                                            '$lt':
                                            [
                                                '$companyIndex',
                                                '$$cix'
                                            ]
                                        }
                                    ]
                                }
                            }
                        },
                        {
                            '$project':
                            {
                                '_id': 0,
                                'company': 1
                            }
                        }
                    ],
                    'as': 'nextCompany'
                }
            },
            {
                '$unwind':
                {
                    'path': '$nextCompany',
                }
            },
            {
                '$group':
                {
                    '_id': '$nextCompany.company',
                    'count':
                    {
                        '$sum': 1
                    }
                }
            },
            {
                '$sort':
                {
                    'count': -1
                }
            },
            {
                '$limit': 15
            }
        ]
    ))

    companies = [i['_id'] for i in data]
    counts = [i['count'] for i in data]

    mpl.rcParams['font.sans-serif'] = ['FangSong']
    mpl.rcParams['axes.unicode_minus'] = False
    
    fig, ax = plt.subplots(figsize=(16,9))

    y_pos = np.arange(len(companies))
    ax.barh(y_pos, counts, color=(0.1,0.1,1.0,0.5))
    ax.set_yticks([])
    ax.invert_yaxis()
    ax.set_title('Where does most of Microsoft employees go to?')

    for i, company in enumerate(companies):
        ax.text(0.1, i, company, ha='left', va='center')

    fig.tight_layout()

    plt.savefig(f'results/{inspect.stack()[0][3]}.png')

with pymongo.MongoClient('localhost', 27017) as client:
    collection = client.zhihu.user

    count_google(collection)
    count_lastCompany(collection)
    count_company(collection)
    count_nextCompany(collection)
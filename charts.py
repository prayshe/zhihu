#coding:utf-8

import pymongo
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import font_manager as fm
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
    plt.rcParams['savefig.dpi'] = 300
    plt.rcParams['figure.dpi'] = 300
    
    fig, ax = plt.subplots(figsize=(8,4))

    wedges, _, _ = ax.pie(counts, autopct=lambda pct: int((pct*sum(counts)/100.)+0.5), wedgeprops=dict(width=0.5),
                    startangle=40, textprops=dict(color="w"), counterclock=False)

    ax.legend(wedges, schools, bbox_to_anchor=(1, 0, 0.5, 1), loc='center left')

    fig.tight_layout()
    plt.savefig(f'results/{inspect.stack()[0][3]}.png', bbox_inches="tight", pad_inches=0)

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
    plt.rcParams['savefig.dpi'] = 300
    plt.rcParams['figure.dpi'] = 300
    
    fig, ax = plt.subplots(figsize=(8,4))

    wedges, _, _ = ax.pie(counts, autopct=lambda pct: int((pct*sum(counts)/100.)+0.5), wedgeprops=dict(width=0.5),
                    startangle=40, textprops=dict(color="w"), counterclock=False)

    ax.legend(wedges, companies, bbox_to_anchor=(1, 0, 0.5, 1), loc='center left')

    fig.tight_layout()
    plt.savefig(f'results/{inspect.stack()[0][3]}.png', bbox_inches="tight", pad_inches=0)

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
    plt.rcParams['savefig.dpi'] = 300
    plt.rcParams['figure.dpi'] = 300
    
    fig, ax = plt.subplots(figsize=(8,4))

    wedges, _, _ = ax.pie(counts, autopct=lambda pct: int((pct*sum(counts)/100.)+0.5), wedgeprops=dict(width=0.5),
                    startangle=40, textprops=dict(color="w"), counterclock=False)

    ax.legend(wedges, companies, bbox_to_anchor=(1, 0, 0.5, 1), loc='center left')

    fig.tight_layout()
    plt.savefig(f'results/{inspect.stack()[0][3]}.png', bbox_inches="tight", pad_inches=0)

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
    plt.rcParams['savefig.dpi'] = 300
    plt.rcParams['figure.dpi'] = 300
    
    fig, ax = plt.subplots(figsize=(8,4))

    wedges, _, _ = ax.pie(counts, autopct=lambda pct: int((pct*sum(counts)/100.)+0.5), wedgeprops=dict(width=0.5),
                    startangle=40, textprops=dict(color="w"), counterclock=False)

    ax.legend(wedges, companies, bbox_to_anchor=(1, 0, 0.5, 1), loc='center left')

    fig.tight_layout()
    plt.savefig(f'results/{inspect.stack()[0][3]}.png', bbox_inches="tight", pad_inches=0)

with pymongo.MongoClient('localhost', 27017) as client:
    collection = client.zhihu.user

    count_google(collection)
    count_lastCompany(collection)
    count_company(collection)
    count_nextCompany(collection)
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import pandas as pd
import numpy as np
from scipy import stats
import snscrape.modules.twitter as sntwitter
import datetime
import itertools
import time



def get_posts_from_user_df(to_user='', from_user='', text='', max_items=5000, max_searches=5, delta_days=90, verbose=False):
    
    """
    Utility function for performing incremental search for a specific user.

        There is a known bug in the current version of snscrape:
            https://github.com/JustAnotherArchivist/snscrape/issues/423

        The objective of this function is to overcome the issue by performing incremental searchers and stop only when 
        the error happens too often.
    
    Returns a pandas dataframe
    
    """

    delta_time = datetime.timedelta(days=delta_days)

    t_end = (datetime.datetime.today().date() + datetime.timedelta(days=1))
    t_start = t_end - delta_time

    t_start.strftime('%Y-%m-%d'), t_end.strftime('%Y-%m-%d')

    df_lst = []

    ns = 0
    ni = 0

    n_errors = 0

    while (ns < max_searches) and (ni < max_items):

        search_query = '{} from:{} to:{} since:{} until:{}'.format(
            text, from_user, to_user, t_start.strftime('%Y-%m-%d'), t_end.strftime('%Y-%m-%d'))    

        if verbose:
            print(search_query)

        try:
            df_u = pd.DataFrame(sntwitter.TwitterSearchScraper(search_query).get_items())
            df_lst.append(df_u)

            ns += 1
            ni += df_u.shape[0]


        except:
            n_errors += 1

            if n_errors > 3:
                print('    Too many consecutive failures. Will stop now.')
                print('\n  Last query: ' + search_query + '\n')
                time.sleep(10)
                break

            print('    Something went wrong, probably the known bug of snscraper. Will try to continue... [{}/3]'.format(n_errors))
            time.sleep(3)


        t_start -= delta_time
        t_end -= delta_time

        if verbose:
            print(ns, ni)
        
    return pd.concat(df_lst).iloc[:max_items,:]


def get_birthday_from_posts(profile):

    """ 
        Naive attempt to get birthday date from posts.
        
        If it cannot find anything reasonable for the year, it will assume it is 1900.

    """

    df_u = pd.DataFrame(itertools.islice(sntwitter.TwitterSearchScraper(
        '"happy birthday"' ' to:' + profile + ' since:2018-01-01 until:2021-01-01').get_items(), 5000))


    if df_u.size > 0:

        df_u1 = df_u[['content', 'date']]
        df_u1.insert(1, 'date_post', df_u1.iloc[:].date.apply(lambda x: x.date()))

        dfu2 = df_u1[['date_post', 'content']].groupby('date_post').count()
        date_birth = dfu2[dfu2.content==dfu2.content.max()].index.values[-1]

        dfu3 = df_u1[df_u1.date_post==date_birth]

        def guess_year_birth(content):

            def trim_spaces(text):

                while text.find('  ') > 0:
                    text = text.replace('  ', ' ')

                return text.strip()

            text = trim_spaces(content).lower()   
            ix = text.find('years old')
            yo = [x for x in text[:ix].split(' ') if x.isnumeric()]

            if len(yo)>0 and yo != []:
                year_birth = date_birth.year-int(yo[-1])
                if year_birth < 1 or year_birth > 99:
                    year_birth = 0

            else:
                year_birth = 0

            return year_birth


        # apply on twittes
        a = dfu3.content.apply(lambda x: guess_year_birth(x)).values

        if len(a)>0 and np.array(a).max()>0:
            year_birth = stats.mode(a[a>0])[0][0]
        else:
            year_birth = 1900

        birthday = datetime.datetime(year_birth, 
                          date_birth.month,
                          date_birth.day).date()
    else:
        birthday = None  

    #if birthday is not None:
    #    s_user.loc['Birthday'] = birthday
    
    return birthday





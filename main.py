"""

    """

import json
import re

import pandas as pd
from githubdata import GithubData


class GDUrl :
    with open('gdu.json' , 'r') as fi :
        gj = json.load(fi)

    cur = gj['cur']
    trg = gj['trg']

gu = GDUrl()

btic_repo_url = 'https://github.com/imahdimir/d-uniq-BaseTickers'

btick = 'BaseTicker'
ipojd = 'IPOJDate'
cname = 'CompanyName'
naam = 'نام شركت'
date = 'date'

def take_until_ticker_in_pranthesis(istr) :
    ptr = r'^(.+\()\s?.+\)\s?-.+'
    st = re.sub(ptr , r'\1' , istr)

    ptr = r'\([^\(]*$'
    ou = re.sub(ptr , '' , st)

    return ou.strip()

def main() :
    pass

    ##
    fn = 'Cleaned_Stock_Prices_14010122.parquet'
    prdf = pd.read_parquet(fn)
    ##
    if date in prdf.columns :
        prdf = prdf.sort_values('date' , ascending = False)

    prdfv = prdf.head()
    ## titles differ for the same ticker if only the market has changed which is not important in this point
    prdf = prdf[['name' , 'title']]
    ##
    prdf = prdf.drop_duplicates()
    ##

    btic_repo = GithubData(btic_repo_url)
    btic_repo.clone_overwrite_last_version()
    ##
    bdfpn = btic_repo.data_fps[0]
    bdf = pd.read_parquet(bdfpn)
    ##
    bdf = bdf.reset_index()
    bdf = bdf[[btick]]
    ##
    prdf['name'] = prdf['name'].apply(lambda x : mf.norm_fa_str(x))
    ##
    prdf = prdf.drop_duplicates(subset = ['name'])
    ##
    bdf = bdf.merge(prdf , left_on = btick , right_on = 'name' , how = 'left')
    ##
    bdf = bdf.sort_values(btick)
    ##
    bdf = bdf[[btick , 'title']]
    ##
    msk = bdf['title'].notna()

    fu = take_until_ticker_in_pranthesis
    bdf.loc[msk , cname] = bdf.loc[msk , 'title'].apply(fu)
    ##
    bdf = bdf[[btick , cname]]
    ##
    bdf = bdf.set_index(btick)
    ##
    bdf.to_parquet(bdfpn)
    ##
    commit_msg = 'added company name from Morteza Aghajan price data crawled from TSETMC.com'
    btic_repo.commit_and_push_to_github_data_target(commit_msg)

    ##
    btic_repo.rmdir()

    ##
    msk = bdf[cname].isna()
    df1 = bdf[msk]

    ##

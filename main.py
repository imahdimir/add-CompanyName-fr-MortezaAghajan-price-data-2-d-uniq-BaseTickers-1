"""

    """

import json
import re

import pandas as pd
from githubdata import GithubData
from mirutil.df_utils import save_as_prq_wo_index as sprq


class GDUrl :
    with open('gdu.json' , 'r') as fi :
        gj = json.load(fi)

    cur = gj['cur']
    src = gj['src']
    trg = gj['trg']

gu = GDUrl()

class ColName:
    ftic = 'FirmTicker'
    cname = 'CompanyName'
    name = 'name'
    titl = 'title'
    naam = 'نام شركت'

c = ColName()

def take_until_ticker_in_pranthesis(istr) :
    ptr = r'^(.+\()\s?.+\)\s?-.+'
    st = re.sub(ptr , r'\1' , istr)

    ptr = r'\([^\(]*$'
    ou = re.sub(ptr , '' , st)

    return ou.strip()

def main() :
    pass

    ##
    fp = 'data.prq'
    df = pd.read_parquet(fp)
    ## titles differ for the same ticker if only the market has changed which is not important in this point
    df = df[[c.name , c.titl]]
    ##
    df = df.drop_duplicates()
    ##
    df[c.name] = df[c.name].str.strip()
    ##
    df = df.drop_duplicates(subset = [c.name])
    ##

    gd_src = GithubData(gu.src)
    gd_src.overwriting_clone()
    ##
    ds = gd_src.read_data()
    ##

    da = pd.merge(ds, df , left_on = c.ftic , right_on = c.name , how = 'left')
    ##
    da = da.dropna()
    da = da[[c.ftic , c.titl]]
    ##
    fu = take_until_ticker_in_pranthesis
    da[c.cname] = da[c.titl].apply(fu)
    ##

    da = da[[c.cname , c.ftic]]
    ##
    da[c.cname] = da[c.cname].str.strip()
    ##

    da = da.astype('string')
    ##

    gd_trg = GithubData(gu.trg)
    gd_trg.overwriting_clone()
    ##

    fp = gd_trg.local_path / 'data.prq'
    sprq(da , fp)
    ##

    msg = 'init by: '
    msg += gu.cur
    ##

    gd_trg.commit_and_push(msg)

    ##

    gd_src.rmdir()
    gd_trg.rmdir()

    ##

##
if __name__ == '__main__' :
    main()

##

##

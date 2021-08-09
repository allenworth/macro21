# -*- coding: utf-8 -*-
"""
Created on Mon Feb 15 16:13:36 2021

@author: allen
"""
import pandas as pd

''' OECD Function '''
def get_from_oecd(sdmx_query):
    '''
    Read data from OECD database.

    Parameters
    ----------
    sdmx_query : STRING
        Use OECD API generator to determine the string. This function takes
        the portion starting with the datatable and ending with the country /
        measurement.

    Returns
    -------
    DataFrame
        Output of OECD database.

    '''
    return pd.read_csv(
        f"https://stats.oecd.org/SDMX-JSON/data/{sdmx_query}?contentType=csv"
    )


def MEI_BOP6_time(series):
    '''
    Convert time strings from MEI table to pandas datetime.

    Parameters
    ----------
    series : Pandas Series of Strings

    Returns
    -------
    Pandas Series
        Series of pandas datetime.

    '''
    
    df = pd.DataFrame([time.split('-') for time in series])
    df['toparse'] = df[1] + '-' + df[0]
    return pd.to_datetime(df.toparse)
    
def MEI_BOP6_pivot_and_label(df):
    '''
    Converts OECD MEI tabular output for one country to pivot table with a
    datetime index and BoP concepts as the column labels. Also adds more 
    pythonic labeling. Last this function flips the sign of the financial
    account so the BoP sums to 0.

    Parameters
    ----------
    df : Pandas DataFrame
        DataFrame to pivot, re-index and re-label.

    Returns
    -------
    bop : Pandas DataFrame
        DataFrame with time index and BoP concept column labels.

    '''
    bop = df[['Subject', 'Time', 'Value']].drop_duplicates().pivot(
        columns='Subject', index='Time', values='Value').resample(
            'Q').ffill()
    rename = {'Current account, balance': 'current',
             'Goods, balance': 'goods',
             'Services, balance': 'services',
             'Primary income, balance': 'primary',
             'Secondary income, balance': 'secondary',
             'Capital account, balance': 'capital',
             'Financial account, net': 'financial',
             'Financial derivatives, net': 'deriv',
             'Net errors and omissions': 'errors',
             'Reserve assets, net acquisition of financial assets': 'reserves',
             'Other investment, net': 'other',
             'Direct investment, net': 'fdi',
             'Portfolio investment, net': 'portfolio'}
    bop = bop.rename(columns=rename)
    finAcct = ['financial', 'fdi', 'portfolio', 'other', 'deriv']
    finAcctShort = bop.columns.tolist()
    finAcct_available = [acct for acct in finAcct if acct in finAcctShort]
    bop[finAcct_available] = bop[finAcct_available] * -1
    order  = ['current', 'goods', 'services', 'primary', 'secondary',
              'financial', 'fdi', 'portfolio', 'other', 'deriv', 'errors',
              'reserves', 'capital']
    order_available = [label for label in order if label in finAcctShort]
    bop = bop[order_available]
    
    return bop
    
def oecd_bop_pull(country, pgdp=True):
    '''
    Output high level balance of payments nets. Can return either USD millions
    or percentage of 5 year rolling average GDP.

    Parameters
    ----------
    country : STRING
        Three digit OECD country / aggregate code e.g., 'USA'.
    pgdp : STRING, optional
        Whether to return % GDP or $ mm. The default is True.

    Returns
    -------
    usbop : Pandas DataFrame
        
    '''
    fa = 'B6FADI01+B6FAPI10+B6FAOI01+B6FAFD01+B6FARA01+'
    ca = 'B6FATT01+B6BLTT01+B6BLTD01+B6BLSE01+B6BLPI01+B6BLSI01+B6CATT00+' + (
        'B6EOTT01')
    measure = 'CXCU'
    oecd = get_from_oecd(
        'MEI_BOP6/' + fa + ca +'.' + country + '.' + measure + '.Q/all')
    if oecd.SUBJECT.values[0] == 'Semantic Error - Found empty OR expression':
        usbop = 'Semantic Error - Found empty OR expression'
    else:
        
        oecd['Time'] = MEI_BOP6_time(oecd.Time)
    
        # usbop = oecd.copy()
        usbop = MEI_BOP6_pivot_and_label(oecd)
        if pgdp == True:
           df = get_from_oecd(f'SNA_TABLE1/{country}.B1_GA.CXC/all')
           df['Time'] = pd.to_datetime([str(int(t)) + '-1-01' for t in df.TIME])
           df = df[['Time', 'Value']].set_index('Time').resample(
               'Q').ffill().rolling(20, min_periods=1).mean()
           usbop['gdp'] = df.Value
           usbop['gdp'] = usbop.gdp.fillna(method='ffill')  
           usbop = usbop.divide(usbop.gdp, axis=0).drop('gdp', axis=1)

    return usbop

def oecd_country_codes():
    '''
    Dict of OECD country / aggregate codes

    Returns
    -------
    dt : DICT
        
    '''
    dt = {'Australia': 'AUS',
         'Austria': 'AUT',
         'Belgium': 'BEL',
         'Canada': 'CAN',
         'Czech Republic': 'CZE',
         'Denmark': 'DNK',
         'Finland': 'FIN',
         'France': 'FRA',
         'Germany': 'DEU',
         'Greece': 'GRC',
         'Ireland': 'IRL',
         'Poland': 'POL',
         'Portugal': 'PRT',
         'Spain': 'ESP',
         'Switzerland': 'CHE',
         'United Kingdom': 'GBR',
         'Colombia': 'COL',
         'Estonia': 'EST',
         'Russia': 'RUS',
         'Hungary': 'HUN',
         'Iceland': 'ISL',
         'Netherlands': 'NLD',
         'Indonesia': 'IDN',
         'Japan': 'JPN',
         'Luxembourg': 'LUX',
         'Slovak Republic': 'SVK',
         'Chile': 'CHL',
         'South Africa': 'ZAF',
         "China (People's Republic of)": 'CHN',
         'Slovenia': 'SVN',
         'New Zealand': 'NZL',
         'Israel': 'ISR',
         'Italy': 'ITA',
         'Mexico': 'MEX',
         'Korea': 'KOR',
         'United States': 'USA',
         'Latvia': 'LVA',
         'European Union (28 countries)': 'EU28',
         'Sweden': 'SWE',
         'Norway': 'NOR',
         'Turkey': 'TUR',
         'Brazil': 'BRA',
         'Lithuania': 'LTU',
         'Saudi Arabia': 'SAU',
         'Euro area (19 countries)': 'EA19',
         'Argentina': 'ARG',
         'Costa Rica': 'CRI',
         'Bulgaria': 'BGR',
         'Cyprus': 'CYP',
         'Croatia': 'HRV',
         'Romania': 'ROU',
         'Malta': 'MLT'}
    return dt

# To Test

'''
country = oecd_country_codes()
for ct in country.values():
    try:
        df = oecd_bop_pull(country=ct, pgdp=True)
        if type(df) is str:
            print(ct, ' ', df)
        else:
            print(ct, ' ', df.count().sum())
    except KeyError:
        print(ct, ' ', 'pull failed')
'''    

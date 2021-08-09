# -*- coding: utf-8 -*-
"""
Created on Thu Dec 24 13:49:16 2020

@author: allen
"""
import pandas as pd
import sys
sys.path.append('C:/Users/allen/Google Drive/Python Scripts/AW_modules')
import utilities as util

''' OECD Function '''
def get_from_oecd(sdmx_query):
    return pd.read_csv(
        f"https://stats.oecd.org/SDMX-JSON/data/{sdmx_query}?contentType=csv"
    )


def oecd_bop_pull(country, pgdp=True):    
    fa = 'B6FATT01+B6FADI01+B6FAPI10+B6FAOI01+B6FAFD01+B6FARA01+'
    ca = 'B6FATT01+B6BLTT01+B6BLTD01+B6BLSE01+B6BLPI01+B6BLSI01+B6CATT00+' + (
        'B6EOTT01')
    # if pgdp == True:
      #   measure = 'NCCU'
    # else:
      #  measure = 'CXCU'
    measure = 'CXCU'
    oecd = get_from_oecd(
        'MEI_BOP6/' + fa + ca +'.' + country + '.' + measure + '.Q/all')
    
    df = pd.DataFrame([time.split('-') for time in oecd.Time])
    df['toparse'] = df[1] + '-' + df[0]
    oecd['Time'] = pd.to_datetime(df.toparse)
    
    usbop = oecd[['Subject', 'Time', 'Value']].drop_duplicates().pivot(
        columns='Subject', index='Time', values='Value').resample('M').ffill()
    usbop.columns = ['capital', 'current', 'fdi', 'financial', 'deriv',
                     'goods', 'errors', 'other', 'portfolio', 'primary',
                     'reserves', 'secondary', 'services']
    usbop = usbop[['current', 'goods', 'services', 'primary', 'secondary',
                   'financial', 'fdi', 'portfolio', 'other', 'deriv', 'capital',
                   'reserves', 'errors']]
    usbop[['financial', 'fdi', 'portfolio', 'other', 'deriv']] = usbop[
                       ['financial', 'fdi', 'portfolio', 'other', 'deriv']] * -1
    '''
    if pgdp == True:
        oecd2 = get_from_oecd('SNA_TABLE1/B1_GA.' + country + '.C')
        df = pd.DataFrame([time.split('-') for time in oecd2.Time])
        df['toparse'] = df[1] + '-' + df[0]
        oecd2['Time'] = pd.to_datetime(df.toparse)
    '''    
    return usbop

def read_data(from_internet=True):
    if from_internet == True:
        # Data download - GDP and PCE
        dataF = util.fred_revised(['PCEPILFE', 'PCEPI', 'GDPC1', 'GDPPOT'])
        
        # GDP
        gdp = pd.DataFrame()
        gdp[['RGDP', 'PRGRD']] = dataF[['GDPC1', 'GDPPOT']].resample('Q').max()
        gdp = gdp.pct_change(4)
        gdp.to_csv('C:/Users/allen/Google Drive/Python Scripts/Repositories/20201222Macro/gdp.csv')
        
        # Inflation
        pce = pd.DataFrame()
        pce[['Core', 'Headline']] = dataF[['PCEPILFE', 'PCEPI']].resample('M').max()
        pce = pce.pct_change(12)
        pce.to_csv('C:/Users/allen/Google Drive/Python Scripts/Repositories/20201222Macro/pce.csv')
        
        # Data download - Debt Service and Credit Creation
        ngdp = util.fred_revised('gdp').resample('Q').max()
        ngdp.to_csv('C:/Users/allen/Google Drive/Python Scripts/Repositories/20201222Macro/ngdp.csv')
        
        dataC = pd.DataFrame()
        dataC[['nonfin', 'hh', 'totPrivate', 'centralGovt']] = util.fred_revised(
            ['QUSNAMUSDA', 'QUSHAMUSDA', 'QUSPAMUSDA', 'QUSGAMXDCA']).resample('Q').max()
        
        csv = pd.read_csv('https://www.bis.org/statistics/full_bis_dsr_csv.zip')
        csv = csv[csv.BORROWERS_CTY=='US']
        csvT = csv.loc[:, '1999-Q1':].T / 100
        csvT.columns = ['hh', 'nonfin', 'totPrivate']
        
        # Credit Creation
        dataC = dataC.subtract(dataC.shift(4)).divide(ngdp.gdp, axis=0)
        dataC.to_csv('C:/Users/allen/Google Drive/Python Scripts/Repositories/20201222Macro/cc.csv')
        
        # Debt Service Coverage
        csvT = csvT.reset_index()
        csvT['index'] = pd.to_datetime(csvT['index'])
        csvT = csvT.set_index('index').resample('Q').max()
        csvT.to_csv('C:/Users/allen/Google Drive/Python Scripts/Repositories/20201222Macro/debt_service.csv')
        
        # Data download - Unemployment, wage growth
        slack = pd.DataFrame()
        slack[['ue', 'pop_workingAge', 'comp', 'capUtil']] = util.fred_revised(
            ['unrate', 'LFWA64TTUSM647S', 'A576RC1', 'tcu'])
        slack['compPC'] = slack.comp / (slack.pop_workingAge / 1e9)
        slack.to_csv('C:/Users/allen/Google Drive/Python Scripts/Repositories/20201222Macro/slack.csv')
        
        # Slack us slack
        
        # Data download - trade balance, portfolio flows
        bop = oecd_bop_pull('USA')
        bop['gdp'] = util.fred_revised('GDP').resample('Q').max().resample('M').ffill()
        bopP = bop.divide(bop.gdp, axis=0).drop('gdp', axis=1)
        bopP.to_csv('C:/Users/allen/Google Drive/Python Scripts/Repositories/20201222Macro/bop.csv')
        
        # Markets - data download
        mkt = pd.DataFrame()
        mkt['10yr'] = util.fred_revised('dgs10').dgs10
        mkt['2yr'] = util.fred_revised('dgs2').dgs2
        mkt['spy'] = util.yahoo_data_pull_full_history('SPY')['Adj Close']
        mkt['qqq'] = util.yahoo_data_pull_full_history('QQQ')['Adj Close']
        mkt['iwm'] = util.yahoo_data_pull_full_history('iwm')['Adj Close']
        mkt['acwx'] = util.yahoo_data_pull_full_history('acwx')['Adj Close']
        mkt['bamlHY'] = util.fred_revised('BAMLH0A0HYM2')
        mkt['bamlBBB'] = util.fred_revised('BAMLC0A4CBBB')
        mkt.to_csv('C:/Users/allen/Google Drive/Python Scripts/Repositories/20201222Macro/mkt.csv')

    elif from_internet == False:
         gdp = pd.read_csv('C:/Users/allen/Google Drive/Python Scripts/Repositories/20201222Macro/gdp.csv')
         ngdp = pd.read_csv('C:/Users/allen/Google Drive/Python Scripts/Repositories/20201222Macro/ngdp.csv')
         pce = pd.read_csv('C:/Users/allen/Google Drive/Python Scripts/Repositories/20201222Macro/pce.csv')
         dataC = pd.read_csv('C:/Users/allen/Google Drive/Python Scripts/Repositories/20201222Macro/cc.csv')
         csvT = pd.read_csv('C:/Users/allen/Google Drive/Python Scripts/Repositories/20201222Macro/debt_service.csv')
         slack = pd.read_csv('C:/Users/allen/Google Drive/Python Scripts/Repositories/20201222Macro/slack.csv')
         bopP = pd.read_csv('C:/Users/allen/Google Drive/Python Scripts/Repositories/20201222Macro/bop.csv')
         mkt = pd.read_csv('C:/Users/allen/Google Drive/Python Scripts/Repositories/20201222Macro/mkt.csv')

    data = {'gdp': gdp, 'inflation': pce, 'ngdp': ngdp,
            'creditCreation': dataC, 'DSC': csvT, 'slack': slack,
            'bop': bopP, 'mkt': mkt}
    return data


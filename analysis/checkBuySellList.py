import os
from datetime import datetime

import FinanceDataReader as fdr
import exchange_calendars as ecals
import pandas as pd
from pykrx import stock
from pytz import timezone
from tqdm import tqdm


def get_stock(company_code_name_df, company_code, start_year):
    company_code = str(company_code)
    start_year = str(start_year)

    ## 주가정보 dataframe 만들기
    df = fdr.DataReader(company_code, start_year)
    df = df.reset_index()

    ## 주식회사명 주가정보 dataframe에 추가하기
    target_company = company_code_name_df[company_code_name_df['Symbol'] == company_code].Name.values[0]
    target_code = company_code_name_df[company_code_name_df['Symbol'] == company_code].Symbol.values[0]
    df['Name'] = target_company
    df['Code'] = target_code
    return df


def check_buy_sell_list():
    # 날짜지정
    XKRX = ecals.get_calendar("XKRX")
    today = datetime.today()
    last_open_day = XKRX.previous_close(today.strftime("%Y-%m-%d")).astimezone(timezone('Asia/Seoul'))
    if XKRX.is_session(today.strftime("%Y-%m-%d")):
        last_open_day = today

    # 한국거래소 상장종목 전체
    df_krx = fdr.StockListing('KRX')

    ###### ETF, ETN(선물) 제거
    etn_list = stock.get_etn_ticker_list(last_open_day.strftime("%Y%m%d"))
    etf_list = stock.get_etf_ticker_list(last_open_day.strftime("%Y%m%d"))

    # mask 씌울 항목 선정
    mask_etn = df_krx['Symbol'].isin(etn_list)
    mask_etf = df_krx['Symbol'].isin(etf_list)

    # ~를 포함하게 되면 mask의 값을 제외, ~을 제외하면 mask의 값을 포함입니다.
    df_krx = df_krx[~mask_etn]
    df_krx = df_krx[~mask_etf]

    ## dataframe 만들기
    company_code_name_df = df_krx[['Symbol', 'Market', 'Name']]

    total = {}

    target_data = company_code_name_df[company_code_name_df['Market'] != 'KONEX'].Symbol  ## 비상장데이터 제외한 나머지

    cnt = 200
    for idx, i in enumerate([i for i in range(1, int(len(target_data) / 200))]):
        cnt = i * 200
        cnt_2 = (i + 1) * 200

        total[0] = target_data[:200]
        total[i] = target_data[cnt:cnt_2]

    ## 분산하지 않은 경우 : 총 걸린 시간 = 약 60여분

    total_list = []
    for i in tqdm(target_data):  ## 전체 작업률 프로세스 알려주는 기능 (Tip)
        try:
            total_list.append(get_stock(company_code_name_df, i, '2022').values)
        except Exception as err:
            pass

    ## 주가 dataframe 하나로 합치기
    stock_total = pd.DataFrame(total_list[0])

    for i in tqdm(range(1, len(total_list))):
        stock_total = pd.concat([stock_total, pd.DataFrame(total_list[i])], axis=0)

    stock_total.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Change', 'Name', 'Code']
    df = stock_total

    # 1. MA, Bollinger Percent
    df['MA20'] = df["Close"].rolling(window=20).mean()
    df['Stddev'] = df["Close"].rolling(window=20).std()
    df['Upper'] = df["MA20"] + (df["Stddev"] * 2)
    df['Lower'] = df["MA20"] - (df["Stddev"] * 2)
    df = df[(df["Upper"] - df["Lower"]) != 0]
    df['PB'] = (df["Close"] - df["Lower"]) / (df["Upper"] - df["Lower"])
    #### MA, Bollinger Percent End ####

    # 2. II, II Percnet
    df['II'] = df.apply(lambda x: ((2 * x["Close"]) - x["High"] - x["Low"]) / (x["High"] - x["Low"]) * x["Volume"] if (
                                                                                                                                  x[
                                                                                                                                      "High"] -
                                                                                                                                  x[
                                                                                                                                      "Low"]) != 0 else 50,
                        1)
    df['IIP21'] = df["II"].rolling(window=21).sum() / df["Volume"].rolling(window=21).sum() * 100
    ####II, II Percnet End####

    # 3. MFI
    ##typical price 먼저
    df['TP'] = (df['High'] + df['Low'] + df['Close']) / 3

    ##positive, native money flow설정
    df['Positive_MF'] = 0
    df['Nagative_MF'] = 0

    ## for 조건문 써주고 range를 통해서 범위 설정을 해준다
    for i in range(len(df.Close) - 1):
        if df.TP.values[i] < df.TP.values[i + 1]:  # i번째의 가격이 i+1일보다 높으면, i+1일째의 중심가격이랑 i+1일의 거래량 곱이 Positive MF로 가게 된다.
            df.Positive_MF.values[i + 1] = df.TP.values[i + 1] * df.Volume.values[i + 1]
            df.Nagative_MF.values[i + 1] = 0
        else:  # 반대의 경우는 Nagative MF로 가게되고
            df.Nagative_MF.values[i + 1] = df.TP.values[i + 1] * df.Volume.values[i + 1]
            df.Positive_MF.values[i + 1] = 0

    ## 10일동안 긍정 현금 흐름의 합을 10일동안의 부정 현금 흐름의 합으로 나는 결과는 MFR(money flow ratio)로 저장한다.
    df['MFR'] = df.Positive_MF.rolling(window=10).sum() / df.Nagative_MF.rolling(window=10).sum()

    ## 10일 기준으로 계산한 결과
    df['MFI10'] = 100 - 100 / (1 + df['MFR'])
    #### MFI End ####

    #### 4. stochastic def stochastic(df, n=5, m=3, t=3):

    df['ndays_high%d' % 5] = df.High.rolling(window=5).max()
    df['ndays_low%d' % 5] = df.Low.rolling(window=5).min()

    df["fast_k"] = df.apply(
        lambda x: 100 * (x["Close"] - x["ndays_low%d" % 5]) / (x["ndays_high%d" % 5] - x["ndays_low%d" % 5]) if (x[
                                                                                                                     "ndays_high%d" % 5] -
                                                                                                                 x[
                                                                                                                     "ndays_low%d" % 5]) != 0 else 50,
        1)

    df['slow_k'] = df.fast_k.rolling(3).mean()
    df['slow_d'] = df.slow_k.rolling(3).mean()

    df_1 = df[df["Date"] == last_open_day.strftime("%Y-%m-%d")]

    # 매수 모듈

    # Module A
    ## Bollinger Band: 0% < x < 5%
    ## II > 0
    ## Volume 1000000
    ## Name: 스팩 제거

    Module_A = df_1[(df_1["PB"] > 0.00) & (df_1["PB"] < 0.05) & (df_1["IIP21"] > 0) & (df_1["Volume"] > 1000000) & (
            df_1['Name'] != df_1['Name'].str.contains("스팩"))]

    # Module B
    ## Bollinger Band: 80% < x < 90%
    ## MFI: 80 < x < 90
    ## Volume 1000000
    ## Name: 스팩 제거

    Module_B = df_1[(df_1["PB"] > 0.80) & (df_1["PB"] < 0.85) & (df_1["MFI10"] > 80) & (df_1["MFI10"] < 90) & (
            df_1["Volume"] > 1000000) & (df_1['Name'] != df_1['Name'].str.contains('^스팩^'))]

    if len(Module_A) > 0:
        Module_A.to_csv('files/buy/Module_A_' + last_open_day.strftime("%Y%m%d") + '.csv',
                        index=False)  ## 구분자를 탭으로 하여 저장. 인덱스칼럼은 저장 안함.
    if len(Module_B) > 0:
        Module_B.to_csv('files/buy/Module_B_' + last_open_day.strftime("%Y%m%d") + '.csv',
                        index=False)  ## 구분자를 탭으로 하여 저장. 인덱스칼럼은 저장 안함.

    # In[ ]:

    # Module C
    ## Stochastic(k가 d를 상향돌파할때): slow_k > slow_d
    ## Slow K가 20보다 낮을 때 매수
    ## Volume 1000000
    ## Name: 스팩 제거
    Module_C = df_1[(df_1["MFI10"] > 80) & (df_1["slow_k"] > df_1["slow_d"]) & (df_1["slow_k"] < 20) & (
            df_1["Volume"] > 1000000) & (df_1['Name'] != df_1['Name'].str.contains('^스팩^'))]

    if len(Module_C) > 0:
        Module_C.to_csv('files/buy/Module_C_' + last_open_day.strftime("%Y%m%d") + '.csv',
                        index=False)  ## 구분자를 탭으로 하여 저장. 인덱스칼럼은 저장 안함.

    # sell list
    if os.path.exists("files/hold/holding_list_" + last_open_day.strftime("%Y%m%d") + ".csv"):  # 해당 경로에 파일이 있는지 체크한다.
        sell_df = pd.read_csv("files/hold/holding_list_" + last_open_day.strftime("%Y%m%d") + ".csv")
        sell_df['Code'] = sell_df['Code'].astype(str)
        for index, row in sell_df.iterrows():
            if len(row['Code']) < 6:
                for i in range(6 - len(row['Code'])):
                    row['Code'] = "0" + row['Code']
        df_2 = pd.merge(left=df_1, right=sell_df, how="inner", on="Code")
        df_2.to_csv('files/hold/current_' + last_open_day.strftime("%Y%m%d") + '.csv', index=False)
        sell_list_module_a = df_2[(df_2["PB"] < 0.80) & (df_2["IIP21"] < 0) & df_2["Logic"] == "A"]
        sell_list_module_b1 = df_2[
            (df_2["PB"] < 0.50) & (df_2["PB"] >= 0.20) & (df_2["MFI10"] < 50) & (df_2["MFI10"] > 20) & df_2[
                "Logic"] == "B"]
        sell_list_module_b2 = df_2[(df_2["PB"] < 0.20) & (df_2["MFI10"] < 20) & df_2["Logic"] == "B"]
        sell_list_module_c = df_2[(df_2["slow_k"] > 80) & (df_2["Logic"] == "C")]
        if len(sell_list_module_a) > 0:
            sell_list_module_a.to_csv('files/sell/sell_list_module_a_' + last_open_day.strftime("%Y%m%d") + '.csv',
                                      index=False)
        if len(sell_list_module_b1) > 0:
            sell_list_module_b1.to_csv('files/sell/sell_list_module_b1_' + last_open_day.strftime("%Y%m%d") + '.csv',
                                       index=False)
        if len(sell_list_module_b2) > 0:
            sell_list_module_b2.to_csv('files/sell/sell_list_module_b2_' + last_open_day.strftime("%Y%m%d") + '.csv',
                                       index=False)
        if len(sell_list_module_c) > 0:
            sell_list_module_c.to_csv('files/sell/sell_list_module_c_' + last_open_day.strftime("%Y%m%d") + '.csv',
                                      index=False)

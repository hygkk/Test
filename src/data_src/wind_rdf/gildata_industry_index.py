from functools import lru_cache
import datetime as dt
import pandas as pd

from .common_func_4_gildata import MARKET_CODE_NUM_DICT
from .util import sql_2_pddf
from src.utils.utils import date_yyyymmdd

@lru_cache(maxsize=2)
def get_industry_index_info(create_index_organization_code=1052, public_index_type=161002, end_date_null_flag=False):
    """
    相关代码需要去聚源的网站上查询, 默认参数为中信二级行业
    :param create_index_organization_code: 发布机构代码
    :param public_index_type: 指数类别
    :return:
    """
    if end_date_null_flag:
        sql = f"SELECT gildata.LC_IndexBasicInfo.IndexCode,gildata.SecuMain.* FROM gildata.LC_IndexBasicInfo " \
              f"INNER JOIN gildata.SecuMain ON gildata.LC_IndexBasicInfo.IndexCode = gildata.SecuMain.InnerCode " \
              f"where CreatIndexOrgCode = {create_index_organization_code} and PubIndexType = {public_index_type} " \
              f"and EndDate is Null"
    else:
        sql = f"SELECT   " \
              f"gildata.LC_IndexBasicInfo.IndexCode," \
              f"gildata.LC_IndexBasicInfo.PubDate," \
              f"gildata.LC_IndexBasicInfo.EndDate," \
              f"gildata.SecuMain.* " \
              f"FROM gildata.LC_IndexBasicInfo " \
              f"INNER JOIN gildata.SecuMain ON gildata.LC_IndexBasicInfo.IndexCode = gildata.SecuMain.InnerCode " \
              f"where CreatIndexOrgCode = {create_index_organization_code} and PubIndexType = {public_index_type} "
    df = sql_2_pddf(sql)
    return df


@lru_cache(maxsize=2)
def get_stock_industry_class_raw_df(standard=37, secu_category=1):
    """
    相关代码需要去聚源的网站上查询, 默认参数为中信行业分类
    :param standard:
    :param if_performed: 要回溯历史，这个参数拿掉
    :param secu_category:
    :return:
    """
    sql = f"SELECT  " \
          f" gildata.LC_ExgIndustry.CompanyCode," \
          f"gildata.LC_ExgIndustry.FirstIndustryCode,gildata.LC_ExgIndustry.FirstIndustryName," \
          f"gildata.LC_ExgIndustry.SecondIndustryCode,gildata.LC_ExgIndustry.SecondIndustryName," \
          f"gildata.LC_ExgIndustry.ThirdIndustryCode,gildata.LC_ExgIndustry.ThirdIndustryName," \
          f"gildata.LC_ExgIndustry.FourthIndustryCode,gildata.LC_ExgIndustry.FourthIndustryName," \
          f"gildata.LC_ExgIndustry.CancelDate,gildata.LC_ExgIndustry.InfoPublDate," \
          f"gildata.SecuMain.*" \
          f" FROM gildata.LC_ExgIndustry " \
          f"INNER JOIN gildata.SecuMain ON gildata.LC_ExgIndustry.CompanyCode = gildata.SecuMain.CompanyCode " \
          f"where  Standard = {standard}  and gildata.SecuMain.SecuCategory = {secu_category}"
    df1 = sql_2_pddf(sql)
    df1.rename(columns={'InfoPublDate': 'EffectiveDate'}, inplace=True)

    # 单独处理科创板
    sql = f'SELECT  ' \
          f" gildata.LC_STIBExgIndustry.CompanyCode," \
          f"gildata.LC_STIBExgIndustry.FirstIndustryCode,gildata.LC_STIBExgIndustry.FirstIndustryName," \
          f"gildata.LC_STIBExgIndustry.SecondIndustryCode,gildata.LC_STIBExgIndustry.SecondIndustryName," \
          f"gildata.LC_STIBExgIndustry.ThirdIndustryCode,gildata.LC_STIBExgIndustry.ThirdIndustryName," \
          f"gildata.LC_STIBExgIndustry.FourthIndustryCode,gildata.LC_STIBExgIndustry.FourthIndustryName," \
          f"gildata.LC_STIBExgIndustry.CancelDate,gildata.LC_STIBExgIndustry.EffectiveDate," \
          f"gildata.SecuMain.*" \
          f'FROM gildata.LC_STIBExgIndustry ' \
          f'INNER JOIN gildata.SecuMain ON gildata.LC_STIBExgIndustry.CompanyCode = gildata.SecuMain.CompanyCode ' \
          f'where  Standard = {standard} and gildata.SecuMain.SecuCategory = {secu_category}'
    df2 = sql_2_pddf(sql)

    df = pd.concat([df1, df2])
    return df


@lru_cache(maxsize=2)
def get_industry_index_class_link(industry_standard=37, inner_index_code_list=None):
    """
    获取行业指数代码和行业分类的关系
    :param industry_standard:
    :return:
    """
    if inner_index_code_list is None:
        sql = f"select gildata.LC_CorrIndexIndustry.IndexCode, gildata.CT_IndustryType.IndustryCode from gildata.LC_CorrIndexIndustry " \
              f"inner join gildata.CT_IndustryType on gildata.CT_IndustryType.IndustryNum = gildata.LC_CorrIndexIndustry.IndustryCode " \
              f"where IndustryStandard = {industry_standard} "
        output_df = sql_2_pddf(sql)
        return output_df
    else:
        inner_index_code_list_str = "','".join([str(x) for x in inner_index_code_list])
        sql = f"select gildata.LC_CorrIndexIndustry.IndexCode, gildata.CT_IndustryType.IndustryCode from gildata.LC_CorrIndexIndustry " \
              f"inner join gildata.CT_IndustryType on gildata.CT_IndustryType.IndustryNum = gildata.LC_CorrIndexIndustry.IndustryCode " \
              f"where IndustryStandard = {industry_standard} and IndexCode in ('{inner_index_code_list_str}')"
        output_df = sql_2_pddf(sql)
        return output_df


def get_stock_industry_class(wind_code, industry_class_name='中信二级', date=None):
    if date is None:
        date = date_yyyymmdd(dt.date.today())
    else:
        date = date_yyyymmdd(date)
    wind_code_split = wind_code.split('.')
    code = wind_code_split[0]
    market_code = wind_code_split[1]
    market_num = MARKET_CODE_NUM_DICT[market_code]

    if industry_class_name == '中信一级':
        industry_index_info_df = get_industry_index_info(create_index_organization_code=1052, public_index_type=161001,
                                                         end_date_null_flag=False)
        industry_standard_num = 37
        stock_industry_df = get_stock_industry_class_raw_df(standard=industry_standard_num, secu_category=1)
        industry_index_class_link_df = get_industry_index_class_link(industry_standard=industry_standard_num,
                                                                     inner_index_code_list=tuple(
                                                                         industry_index_info_df['IndexCode'].to_list()))
        raw_data = stock_industry_df[
            (stock_industry_df['SecuCode'] == code) & (stock_industry_df['SecuMarket'] == market_num)]
        if raw_data.empty:
            return None

        industry_inner_code_chosen = None
        effective_date_chosen = None
        for i in raw_data.index:
            # 不同时期可能隶属于不同的子版块

            cancel_date = raw_data.loc[i, 'CancelDate']

            if pd.isna(cancel_date) or date < date_yyyymmdd(cancel_date):
                # 没有被cancel，有可能在生效期内
                effective_date = raw_data.loc[i, 'EffectiveDate']
                if date >= date_yyyymmdd(effective_date):
                    # 在生效期之后，可以选
                    if effective_date_chosen is None:
                        effective_date_chosen = effective_date
                        industry_inner_code_chosen = raw_data.loc[i, 'FirstIndustryCode']
                    elif effective_date_chosen < effective_date:
                        # 都生效时，选生效期晚的
                        effective_date_chosen = effective_date
                        industry_inner_code_chosen = raw_data.loc[i, 'FirstIndustryCode']
                    else:
                        pass

        if industry_inner_code_chosen is None:
            # 如果这样都没选中，则看看是什么原因
            earliest_effective_date = raw_data['EffectiveDate'].min()
            if date < date_yyyymmdd(earliest_effective_date):
                # 当前日期竟然比最早的生效日期都要早，选最早的生效日期
                idx = raw_data['EffectiveDate'].idxmin()
                industry_inner_code_chosen = raw_data.loc[idx, 'SecondIndustryCode']
            else:
                raise NotImplementedError('Check your code!')

        link_df_line = industry_index_class_link_df[
            industry_index_class_link_df['IndustryCode'] == industry_inner_code_chosen]
        # assert link_df_line.shape[0] == 1
        output_index_wind_code_set = set()
        for index_inner_code in link_df_line['IndexCode'].values:
            # 不同时期行业可能对应不同指数
            # index_inner_code = link_df_line['IndexCode'].values[0]
            index_info_raw_line = industry_index_info_df[industry_index_info_df['IndexCode'] == index_inner_code]
            assert index_info_raw_line.shape[0] == 1
            index_code = index_info_raw_line['SecuCode'].values[0]
            index_market_num = index_info_raw_line['SecuMarket'].values[0]
            index_wind_code = index_code + '.' + MARKET_CODE_NUM_DICT.inverse[index_market_num]
            # return index_wind_code
            pub_date = date_yyyymmdd(index_info_raw_line['PubDate'].values[0])
            end_date = index_info_raw_line['EndDate'].values[0]
            if pd.isna(end_date):
                # 并没有终止，仍旧在使用
                # 先不区分指数发布日期，之后再筛选一个发布最早的
                output_index_wind_code_set.add((index_wind_code, pub_date))
            else:
                end_date = date_yyyymmdd(end_date)
                if date <= end_date:
                    output_index_wind_code_set.add((index_wind_code, pub_date))
        # assert len(output_index_wind_code_set) >= 1
        len_output_index_wind_code_set = len(output_index_wind_code_set)
        if len_output_index_wind_code_set == 1:
            return output_index_wind_code_set.pop()[0]
        elif len_output_index_wind_code_set > 1:
            index_wind_code, pub_date = output_index_wind_code_set.pop()
            for _ in range(len_output_index_wind_code_set - 1):
                index_wind_code_this, pub_date_this = output_index_wind_code_set.pop()
                if pub_date_this < pub_date:
                    index_wind_code = index_wind_code_this
                    pub_date = pub_date_this
            return index_wind_code
        else:
            raise ValueError(
                f'Can not get industry class of {wind_code} on {date}: len(output_index_wind_code_set) == {len_output_index_wind_code_set}')
    elif industry_class_name == '中信二级':
        industry_index_info_df = get_industry_index_info(create_index_organization_code=1052, public_index_type=161002, end_date_null_flag=False)
        industry_standard_num = 37
        stock_industry_df = get_stock_industry_class_raw_df(standard=industry_standard_num, secu_category=1)
        industry_index_class_link_df = get_industry_index_class_link(industry_standard=industry_standard_num,
                                                                     inner_index_code_list=tuple(industry_index_info_df['IndexCode'].to_list()))
        raw_data = stock_industry_df[(stock_industry_df['SecuCode'] == code) & (stock_industry_df['SecuMarket'] == market_num)]
        if raw_data.empty:
            return None
        # assert raw_data.shape[0] == 1, f'Error: raw_data = \n{raw_data}\n wind_code = {wind_code}'
        # 4 debug

        industry_inner_code_chosen = None
        effective_date_chosen = None
        for i in raw_data.index:
            # 不同时期可能隶属于不同的子版块

            cancel_date = raw_data.loc[i, 'CancelDate']

            if pd.isna(cancel_date) or date < date_yyyymmdd(cancel_date):
                # 没有被cancel，有可能在生效期内
                effective_date = raw_data.loc[i, 'EffectiveDate']
                if date >= date_yyyymmdd(effective_date):
                    # 在生效期之后，可以选
                    if effective_date_chosen is None:
                        effective_date_chosen = effective_date
                        industry_inner_code_chosen = raw_data.loc[i, 'SecondIndustryCode']
                    elif effective_date_chosen < effective_date:
                        # 都生效时，选生效期晚的
                        effective_date_chosen = effective_date
                        industry_inner_code_chosen = raw_data.loc[i, 'SecondIndustryCode']
                    else:
                        pass

        if industry_inner_code_chosen is None:
            # 如果这样都没选中，则看看是什么原因
            earliest_effective_date = raw_data['EffectiveDate'].min()
            if date < date_yyyymmdd(earliest_effective_date):
                # 当前日期竟然比最早的生效日期都要早，选最早的生效日期
                idx = raw_data['EffectiveDate'].idxmin()
                industry_inner_code_chosen = raw_data.loc[idx, 'SecondIndustryCode']
            else:
                raise NotImplementedError('Check your code!')

        link_df_line = industry_index_class_link_df[industry_index_class_link_df['IndustryCode'] == industry_inner_code_chosen]
        # assert link_df_line.shape[0] == 1
        output_index_wind_code_set = set()
        for index_inner_code in link_df_line['IndexCode'].values:
            # 不同时期行业可能对应不同指数
            # index_inner_code = link_df_line['IndexCode'].values[0]
            index_info_raw_line = industry_index_info_df[industry_index_info_df['IndexCode'] == index_inner_code]
            assert index_info_raw_line.shape[0] == 1
            index_code = index_info_raw_line['SecuCode'].values[0]
            index_market_num = index_info_raw_line['SecuMarket'].values[0]
            index_wind_code = index_code + '.' + MARKET_CODE_NUM_DICT.inverse[index_market_num]
            # return index_wind_code
            pub_date = date_yyyymmdd(index_info_raw_line['PubDate'].values[0])
            end_date = index_info_raw_line['EndDate'].values[0]
            if pd.isna(end_date):
                # 并没有终止，仍旧在使用
                # 先不区分指数发布日期，之后再筛选一个发布最早的
                output_index_wind_code_set.add((index_wind_code, pub_date))
            else:
                end_date = date_yyyymmdd(end_date)
                if date <= end_date:
                    output_index_wind_code_set.add((index_wind_code, pub_date))
        # assert len(output_index_wind_code_set) >= 1
        len_output_index_wind_code_set = len(output_index_wind_code_set)
        if len_output_index_wind_code_set == 1:
            return output_index_wind_code_set.pop()[0]
        elif len_output_index_wind_code_set > 1:
            index_wind_code, pub_date = output_index_wind_code_set.pop()
            for _ in range(len_output_index_wind_code_set - 1):
                index_wind_code_this, pub_date_this = output_index_wind_code_set.pop()
                if pub_date_this < pub_date:
                    index_wind_code = index_wind_code_this
                    pub_date = pub_date_this
            return index_wind_code
        else:
            raise ValueError(f'Can not get industry class of {wind_code} on {date}: len(output_index_wind_code_set) == {len_output_index_wind_code_set}')
    else:
        raise NotImplementedError(f"Unknown industry_class_name = {industry_class_name}")
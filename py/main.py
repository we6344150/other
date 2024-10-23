# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import time
from typing import Union
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.responses import FileResponse,HTMLResponse,StreamingResponse
from fastapi.middleware.gzip import GZipMiddleware
from cache_pandas import timed_lru_cache
# from Reduce_fastload import reduce_fastload
import oracledb
from sqlalchemy import create_engine
from pprint import pprint

app = FastAPI()
#app.add_middleware(GZipMiddleware, minimum_size=1000) 
#@app.on_event("startup")
@timed_lru_cache(seconds=60000, maxsize=None)
def load_Db() -> pd.DataFrame:
    start1 = time.perf_counter()
    print("app starting up...")
    # 使用 SQLAlchemy Engine 连接 Oracle 数据库
    # dsn = oracledb.makedsn('cnn-rds-rg-dsp-sit.cabcuuku89wx.rds.cn-north-1.amazonaws.com.cn',
    #                         '1521', 'DSPSIT')
    # connection = oracledb.connect(user='sellout', password='2wsx@WSX', dsn=dsn)
    dsn = oracledb.makedsn('10.177.192.85',
                            '1521', service_name='dspdev')
    connection = oracledb.connect(user='DSPDEV_USR', password='P3^s6Pfp2', dsn=dsn)
    # 创建一个游标对象
    cursor = connection.cursor()

    # 定义SQL查询语句
    # sql_query = "SELECT * FROM T_SO_DOWNSTREAM_COLLECT_DATA"
    sql_query="SELECT * FROM T_SO_DOWNSTREAM_COLLECT_DATA"
    # cursor.execute(sql_query)
    # 获取列名
    
    # result=cursor.fetchall()
    # # 执行查询并将结果保存到DataFrame中
    # df = pd.DataFrame(result, columns=column_names)
    # 分页参数（例如：每次读取1000行）
    batch_size = 1000000
    # 初始化一个空列表来保存每一批次的数据
    df = []
    # 循环读取数据
    offset = 0
    count = 0
    while True:
        count += 1
        if count==11:
            break
        # 构造分页查询语句
        query_with_offset = f"{sql_query} OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY"
        print("offset:",offset)
        start2 = time.perf_counter()
        cursor.execute(query_with_offset)
        column_names = [desc[0] for desc in cursor.description]
        result=cursor.fetchall()
        # 执行查询并将结果转换为DataFrame
        batch_df = pd.DataFrame(result, columns=column_names)
        end2 = time.perf_counter()
        runTime2 = end2 - start2
        # 输出运行时间
        print("第",offset,"次，运行时间：", runTime2, "秒")
        # 如果这一批次没有数据了，则跳出循环
        if not len(batch_df):
            break
        # 将批次数据添加到列表中
        df.append(batch_df)

        # 更新偏移量以获取下一批数据
        offset += batch_size

    # 合并所有批次的DataFrame
    if df:
        df = pd.concat(df, ignore_index=True)
    else:
        df = pd.DataFrame()  # 若没有数据，则创建一个空DataFrame

    # 关闭游标和连接
    cursor.close()
    connection.close()
    print(df)
    end1 = time.perf_counter()
    runTime1 = end1 - start1
    # 输出运行时间
    print("运行时间：", runTime1, "秒")
    # 读取数据到 DataFrame
    print("df列数",len(df))
    print("df内存",df.memory_usage().sum()/(1024*1024))
    start3 = time.perf_counter()
    # 产品线统计
    pl_agg = df.groupby(["CUSTOMER_CODE",
                   "CUSTOMER_NAME",
                   "YEAR", 
                   "MONTH", 
                   "SUB_CUSTOMER_CODE", 
                   "SUB_CUSTOMER_NAME",
                   "PRODUCT_GROUP_CODE",
                   "PRODUCT_GROUP_NAME",
                   "PRODUCT_LINE_CODE",
                   "PRODUCT_LINE_NAME"],observed=True).agg({"SALES_TOTAL_AMOUNT":np.sum})
    pl_agg = pl_agg.reset_index()
    # 行转列
    pl = pl_agg.pivot(
        index=["CUSTOMER_CODE","CUSTOMER_NAME","YEAR", "MONTH", "SUB_CUSTOMER_CODE", "SUB_CUSTOMER_NAME"], 
        columns=["PRODUCT_LINE_CODE"],
        values="SALES_TOTAL_AMOUNT")
    pl = pl.reset_index()
    # 产品组统计
    pg_agg = pl_agg.groupby(["CUSTOMER_CODE",
                   "CUSTOMER_NAME",
                   "YEAR", 
                   "MONTH", 
                   "SUB_CUSTOMER_CODE", 
                   "SUB_CUSTOMER_NAME",
                   "PRODUCT_GROUP_CODE",
                   "PRODUCT_GROUP_NAME"],observed=True).agg({"SALES_TOTAL_AMOUNT":np.sum})
    pg_agg = pg_agg.reset_index()

    # 行转列
    pg = pg_agg.pivot(
        index=["CUSTOMER_CODE","CUSTOMER_NAME","YEAR", "MONTH", "SUB_CUSTOMER_CODE", "SUB_CUSTOMER_NAME"], 
        columns=["PRODUCT_GROUP_CODE"],
        values="SALES_TOTAL_AMOUNT")
    pg = pg.reset_index()
    # 月份统计
    month_agg = pg_agg.groupby(["CUSTOMER_CODE",
                   "CUSTOMER_NAME",
                   "YEAR", 
                   "MONTH", 
                   "SUB_CUSTOMER_CODE", 
                   "SUB_CUSTOMER_NAME"],observed=True).agg({"SALES_TOTAL_AMOUNT":np.sum}).rename(columns={"SALES_TOTAL_AMOUNT":"MONTH_AGG"})

    # 合并产品线，产品组
    result = pd.merge(pl, pg, how='left', on=["CUSTOMER_CODE","CUSTOMER_NAME","YEAR", "MONTH", "SUB_CUSTOMER_CODE", "SUB_CUSTOMER_NAME"])

    result = pd.merge(result, month_agg, how='left', on=["CUSTOMER_CODE","CUSTOMER_NAME","YEAR", "MONTH", "SUB_CUSTOMER_CODE", "SUB_CUSTOMER_NAME"])
    end3 = time.perf_counter()
    runtime3=end3-start3
    print("load_Db计算运行时间：", runtime3, "秒")
    
    filter_values = result.query("CUSTOMER_CODE == '603001'")
    
    filter_values.to_csv("2023_06-result3.csv",encoding="utf_8_sig")
    
  
    return  pd.DataFrame.from_dict(result)

@app.on_event("startup")
@timed_lru_cache(seconds=600, maxsize=None)
def load_data() -> pd.DataFrame:
    # start = time.perf_counter()
    start1 = time.perf_counter()
    print("app starting up...")
    #df = pd.read_parquet('export.parquet')
    df = pd.read_feather('processed_data.feather')
    #df = pd.read_feather('T_SO_DOWNSTREAM_COLLECT_DATA.feather')
    #df = pd.read_csv('0112.zip', compression='zip', low_memory=False)
    #df = pd.read_csv("export.csv", low_memory=False)
    #process2 =reduce_fastload("export.csv", use_feather=True)
    #process2.reduce_data()
    #print("读入feather文件")
    #df = process2.reload_data()
    # data = df.query('SALES_TOTAL_AMOUNT != 0')
    pd.set_option('display.unicode.ambiguous_as_wide', True)
    pd.set_option('display.unicode.east_asian_width', True)
    pd.set_option('display.max_columns', None) 
    pd.set_option('display.width', 180) # 设置打印宽度(**重要**)
    print(len(df))
    print(df.memory_usage().sum()/(1024*1024))
    end1 = time.perf_counter()
    runTime1 = end1 - start1
    runTime_ms1 = runTime1 * 1000
    # 输出运行时间
    print("运行时间：", runTime1, "秒")
    print("运行时间：", runTime_ms1, "毫秒")
    start = time.perf_counter()
    
    # 产品线统计
    pl_agg = df.groupby(["CUSTOMER_CODE",
                   "CUSTOMER_NAME",
                   "YEAR", 
                   "MONTH", 
                   "SUB_CUSTOMER_CODE", 
                   "SUB_CUSTOMER_NAME",
                   "PRODUCT_GROUP_CODE",
                   "PRODUCT_GROUP_NAME",
                   "PRODUCT_LINE_CODE",
                   "PRODUCT_LINE_NAME"],observed=True).agg({"SALES_TOTAL_AMOUNT":np.sum})
    pl_agg = pl_agg.reset_index()
    # 行转列
    pl = pl_agg.pivot(
        index=["CUSTOMER_CODE","CUSTOMER_NAME","YEAR", "MONTH", "SUB_CUSTOMER_CODE", "SUB_CUSTOMER_NAME"], 
        columns=["PRODUCT_LINE_CODE"],
        values="SALES_TOTAL_AMOUNT")
    pl = pl.reset_index()
    # 产品组统计
    pg_agg = pl_agg.groupby(["CUSTOMER_CODE",
                   "CUSTOMER_NAME",
                   "YEAR", 
                   "MONTH", 
                   "SUB_CUSTOMER_CODE", 
                   "SUB_CUSTOMER_NAME",
                   "PRODUCT_GROUP_CODE",
                   "PRODUCT_GROUP_NAME"],observed=True).agg({"SALES_TOTAL_AMOUNT":np.sum})
    pg_agg = pg_agg.reset_index()

    # 行转列
    pg = pg_agg.pivot(
        index=["CUSTOMER_CODE","CUSTOMER_NAME","YEAR", "MONTH", "SUB_CUSTOMER_CODE", "SUB_CUSTOMER_NAME"], 
        columns=["PRODUCT_GROUP_CODE"],
        values="SALES_TOTAL_AMOUNT")
    pg = pg.reset_index()
    # 月份统计
    month_agg = pg_agg.groupby(["CUSTOMER_CODE",
                   "CUSTOMER_NAME",
                   "YEAR", 
                   "MONTH", 
                   "SUB_CUSTOMER_CODE", 
                   "SUB_CUSTOMER_NAME"],observed=True).agg({"SALES_TOTAL_AMOUNT":np.sum}).rename(columns={"SALES_TOTAL_AMOUNT":"MONTH_AGG"})

    # 合并产品线，产品组
    
    print("DATAFrame>pl:",pl)
    print("DATAFrame>pg:",pg)
    result = pd.merge(pl, pg, how='left', on=["CUSTOMER_CODE","CUSTOMER_NAME","YEAR", "MONTH", "SUB_CUSTOMER_CODE", "SUB_CUSTOMER_NAME"])

    result = pd.merge(result, month_agg, how='left', on=["CUSTOMER_CODE","CUSTOMER_NAME","YEAR", "MONTH", "SUB_CUSTOMER_CODE", "SUB_CUSTOMER_NAME"])
    
    

    end = time.perf_counter()
    runTime = end - start
    runTime_ms = runTime * 1000
    # 输出运行时间
    print("load_data运行时间：", runTime, "秒")
    print("load_data运行时间：", runTime_ms, "毫秒")
    print(df.memory_usage(index=True).sum()/(1024*1024))
   #filter_values = result.query('CUSTOMER_NAME == "重庆众联达电气有限公司"')
    #filter_values = result[result['CUSTOMER_CODE'].str == "99301"]
    #result.set_index('CUSTOMER_CODE')
    filter_values = result.query('CUSTOMER_CODE == 99301')
    
    #(result['CUSTOMER_CODE'])='99301'
    #&(result['YEAR']='2022')&(result['MONTH']='2')
    #print(fileter_values)
    filter_values.to_csv("2023_06-result.csv",encoding="utf_8_sig")
    # print("------------result--------------")
    # count = 0
    # for key, value in result.items():
    #     print(f"Key: {key}, Value: {value}")
    #     count += 1
    #     if count == 5:
    #         break
    # print("------------result--------------")
    print("len",len(result))
    return  pd.DataFrame.from_dict(result)
@app.get("/customer/{customer_id}")
def read_item(customer_id: int):
    start = time.perf_counter()
    #result.query('CUSTOMER_CODE == @customer_id').to_csv(customer_id+".csv",encoding="utf_8_sig")
    #filtered_value = result[= 99301]
    

    if customer_id > 0 :
        queryString = "CUSTOMER_CODE == "+ str(customer_id)
        #queryString = "CUSTOMER_CODE == '"+ str(customer_id)+"'"
        filename = str(customer_id)+".csv"
        #def iterfile():
        #    yield from  result.query(queryString)
        res= StreamingResponse(iter([load_data().query(queryString).to_csv(encoding="utf_8_sig")]),
        #return StreamingResponse(iter([result.query(queryString).to_csv()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename="+filename}
        )
        end = time.perf_counter()
        runTime = end - start
        runTime_ms = runTime * 1000
        # 输出运行时间
        print("查询运行时间：", runTime, "秒")
        return res
    #result.query(queryString).to_csv(str(customer_id)+".csv",encoding="utf_8_sig") # 
        
    else :
        return StreamingResponse(iter([load_Db().to_csv(encoding="utf_8_sig")]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=all.csv"}
        )
        #return FileResponse(result.to_csv("all.csv",encoding="utf_8_sig"), headers=headers)
    end = time.perf_counter()
    runTime = end - start
    runTime_ms = runTime * 1000
    # 输出运行时间
    #print("查询运行时间：", runTime, "秒")
    #print("查询运行时间：", runTime_ms, "毫秒")


   
    #return {"customer_id": customer_id}
@app.get("/file/split")
def splitCSV():
    # 读取csv文件，chunksize参数用于分块读取数据
    chunksize = 1250000
    k = 10

    for chunk in pd.read_csv('T_SO_DOWNSTREAM_COLLECT_DATA_202401231803.csv', chunksize=chunksize):
        # 对于每一个chunk（即每个分块的数据），保存为新的csv文件
        chunk.to_csv(f'small_file_{k}.csv', index=False)
        k=k+1
        #chunks.append(chunk)

    # 注意：上述代码会生成名为small_file_0.csv, small_file_1.csv等的小文件

    # 如果你想基于某种逻辑进行分割，例如根据某个字段值的不同，则需要在处理每个chunk时添加相应的逻辑

@app.get("/convert/feather")
def convert():
    print("STRAT CONVERT")
    start = time.perf_counter()
    data = pd.read_csv("T_SO_DOWNSTREAM_COLLECT_DATA.csv", low_memory=False)
    # 将DataFrame保存为Feather格式
    data.to_feather("T_SO_DOWNSTREAM_COLLECT_DATA.feather")
    end = time.perf_counter()
    runTime = end - start
    print("查询运行时间：", runTime, "秒")
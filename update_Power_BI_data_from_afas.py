

from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
import os
import time


import time
import json
import asyncio
import aiohttp
from aiohttp import ClientSession, ClientTimeout
import csv 

import pyodbc
import pandas as pd


import logging

logger = logging.getLogger(__name__)

import threading







with open('credentials.txt', 'rb') as file:
    file_data = json.load(file)
    site_url = file_data["site_url"]
    client_id = file_data["client_id"]
    client_secret = file_data["client_secret"]
    token = file_data["afas_token"]


with open('List_get_connectors.txt', 'rb') as fp:
    list_get_conn = json.load(fp)


app_principal = { 
  'client_id' : client_id , 
  'client_secret' : client_secret , 
} 






def send_to_sharepoint(logger, file_name):
    context_auth = AuthenticationContext(url=site_url) 
    context_auth.acquire_token_for_app(client_id=app_principal[ 'client_id' ], 
    client_secret=app_principal[ 'client_secret' ]) 

    ctx = ClientContext(site_url, context_auth)



    target_url = "/sites/PROJ-PowerBiData/Gedeelde documenten/Power Bi"
    target_folder = ctx.web.get_folder_by_server_relative_url(target_url)
    size_chunk = 10000000

    local_path = f"C:/Development/Python/For Power BI/{file_name}.csv"
    start_time = time.time()

    def print_upload_progress(offset):
        file_size = os.path.getsize(local_path)
        logger.info("Uploaded '{0}' bytes from '{1}'...[{2}%]".format(offset, file_size, round(offset / file_size * 100, 2)))

        print("Uploaded '{0}' bytes from '{1}'...[{2}%]".format(offset, file_size, round(offset / file_size * 100, 2)))


    with open(f"{file_name}.csv", 'rb') as f:
        uploaded_file = target_folder.files.create_upload_session(f, size_chunk,
                                                              print_upload_progress).execute_query()

    #uploaded_file = target_folder.files.create_upload_session(local_path, size_chunk, print_upload_progress).execute_query()
    print('File {0} has been uploaded successfully'.format(uploaded_file.serverRelativeUrl))
    logger.info('File {0} has been uploaded successfully'.format(uploaded_file.serverRelativeUrl))
    
    fin_end_time = time.time()
    elapsed_time = fin_end_time - start_time
    print('Elapsed time: ', elapsed_time)
    logger.info('Finished, file_name = %s Elapsed time: %s', file_name, elapsed_time)


def getRESTConnMetainfoEndpoint(omgeving, getconnector_naam):
    return "{}/connectors/{}".format(str('https://83607.rest.afas.online/ProfitRestServices'), getconnector_naam)





async def get_by_porshon(conector,skip_i, session):
    params = dict(Authorization = "AfasToken " + token)
    getconnector = conector +f'?skip={skip_i*100000}&take=100000'
    async with session.get(getRESTConnMetainfoEndpoint('83607', getconnector), headers=params) as response:
        return {skip_i: await response.json()}



async def get_data_from_afas(conector,skip):
  
    timeout = ClientTimeout(total=6000)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        results = []
        for i in  range(len(skip)):
            results.append(get_by_porshon(conector,skip[i], session))
        
        return await asyncio.gather(*results)

async def main_get_afas(conector,skip):

    result = await get_data_from_afas(conector,skip)
    return result  











def get_afas_asyncio(logger,conector=None):
    logger1 = logging.getLogger(__name__)

    logging.basicConfig(filename= f'myapp{conector}.log', format="%(asctime)s - %(levelname)s - %(message)s",
        style="%",
        datefmt="%Y-%m-%d %H:%M", level=logging.INFO)
    logger1.info('Started *********************************')

    

    print(conector)
    print(logger)
    
    skip = 0
    qure_respons = []
    
    start_time = time.time()
    
    skip = list(range(0, 20))
    while True:
        if len(skip) == 0:
            break
        
        result = asyncio.run(main_get_afas(conector,skip))
        
        skip = []

        for i in result:
            for key, value in i.items():
                errorCode = [x for x in value if x == 'errorCode']
                #print(errorCode)
                if len(errorCode)!=0:
                    skip.append(key)
                    #print(key)
                    
                else:
                    try:
                        qure_respons.extend(value['rows'])
                    except:
                        #print(value)
                        with open('data.json', 'w') as f:
                            logger.exception('Table data could not be retrieved: %s, message: %s',conector,  value)
                            json.dump(value, f)
    
    
    
    fin_end_time = time.time()
    elapsed_time = fin_end_time - start_time
    logger.info('Finished %s Elapsed time: %s', conector, elapsed_time)
    print('Elapsed time: ', elapsed_time)

    try:
        with open(conector +'.csv',  'w', encoding = 'utf=8',  newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames = list(qure_respons[0].keys())) 
            writer.writeheader() 
            writer.writerows(qure_respons)
            #df.to_csv(file)
    except:
        logger.exception('Data is not written to a file: %s',conector +'.csv')
        raise IOError("")

def send_to_sql():

    server = 'yourservername' 
    database = 'AdventureWorks' 
    username = 'username' 
    password = 'yourpassword' 
    
    server = 'afaspower-bi.database.windows.net'
    database = 'afas'
    username = 'afas_power_bi_sa'
    password = '85Ped1chF'
    driver= '{ODBC Driver 17 for SQL Server}'

    start_time = time.time()

    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'api_bi_salesorderlineitems'")


    row = cursor.fetchone()
    ListofCol = []
    while row:
        ListofCol.append(str(row[0]) )
        row = cursor.fetchone()


    with open('names.txt', 'rb') as fp:
        n_list = json.load(fp)


    n_list = n_list[0:100]    
    #print(list(n_list[0].keys()))
    

    del ListofCol[0]

    df = pd.DataFrame(n_list, columns=list(n_list[0].keys()))
    df.rename(columns ={'Bedrag_regelkorting__BV_': 'Bedrag_regelkorting_BV', 'ED_-_SOS_calculation': 'ED_SOS_calculation'}, inplace=True)
    #df.drop(['id'], axis=1)

    df = df[ListofCol]

    col_list = ", ".join(ListofCol)
    col_list = col_list.replace('-_', '')

    placeholders = ", ".join(['?'] * len(col_list.split(", ")))
    mogrify = ",".join(['%s'] * len(col_list.split(", ")))

  

    count = 0

    list_row = []
    for index, row in df.iterrows():
        list_row.append(str(tuple(list(row))))


        cursor.execute(f"INSERT INTO api_bi_salesorderlineitems ({col_list}) values({placeholders})",list(row))
        
        #print(count)
        count +=1
  
    insert_st  = f"INSERT INTO api_bi_salesorderlineitems({col_list}) values{list_row}"


    cnxn.commit()
    cursor.close()
    fin_end_time = time.time()
    elapsed_time = fin_end_time - start_time

    print('Elapsed time: ', elapsed_time)



def execute(logger,get,exeption_list):
    

        get_afas_asyncio(logger,get)
        send_to_sharepoint(logger,get)



if __name__ == '__main__':
    start_time = time.time()

    logging.basicConfig(filename='myapp.log', format="%(asctime)s - %(levelname)s - %(message)s",
        style="%",
        datefmt="%Y-%m-%d %H:%M", level=logging.INFO)
    logger.info('Started *********************************')

   
    get_list_test =["BI_Warehouse","BI_Article_image"]

    exeption_list = []

    for get in list_get_conn:

        try:

            get_afas_asyncio(logger,get)
            send_to_sharepoint(logger,get)

        except:
            logger.exception('Table execution error: %s', get)

            exeption_list.append(get)

    logger.exception('Exeption List: %s', exeption_list)


    for get in exeption_list:
        try:


            get_afas_asyncio(logger,get)
            send_to_sharepoint(logger,get)

        except:
            logger.exception('Table execution error: %s', get)





    fin_end_time = time.time()
    elapsed_time = fin_end_time - start_time
    logger.info('Finished **************************, Elapsed time: %s', elapsed_time)
    print('Elapsed time: ', elapsed_time)


import time
import json
import asyncio
import aiohttp
from aiohttp import ClientSession, ClientTimeout
import csv 

import pyodbc
import pandas as pd
import upload_to_sharepoint

import logging

logger = logging.getLogger(__name__)

import threading



def getRESTConnMetainfoEndpoint(omgeving, getconnector_naam):
    return "{}/connectors/{}".format(str('https://83607.rest.afas.online/ProfitRestServices'), getconnector_naam)





async def get_by_porshon(conector,skip_i, session):
    token = "PHRva2VuPjx2ZXJzaW9uPjE8L3ZlcnNpb24+PGRhdGE+NkE2NDMzQjU1Rjg5NDE5ODg1RjVERDkwRjdBODJEMjdDNUI4QTM5OUVFM0I0MjAyODQ3OEM5QkIyQUY4OEE4MzwvZGF0YT48L3Rva2VuPg=="
    params = dict(Authorization = "AfasToken " + token)#'PHRva2VuPjx2ZXJzaW9uPjE8L3ZlcnNpb24+PGRhdGE+QjBCNTg1QjJBNDNBNDAzOEE2NDUyNEZFNzc4QTFFMjdDQTIzOUY3MTQ3NDQ5N0JGQzg0QTZEQjkxM0NEODMzMjwvZGF0YT48L3Rva2VuPg==')
    getconnector = conector +f'?skip={skip_i*100000}&take=100000'
    #print(getconnector)
    async with session.get(getRESTConnMetainfoEndpoint('83607', getconnector), headers=params) as response:
        return {skip_i: await response.json()}



async def get_data_from_afas(conector,skip):
    """
    Параллельное скачивание с каждой ссылки
    (собираем awitable запросы в список, потом параллельно собираем результат через asyncio.gather)
    """
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
    #print(skip)
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
                    print(key)
                    
                else:
                    try:
                        qure_respons.extend(value['rows'])
                    except:
                        #print(value)
                        with open('data.json', 'w') as f:
                            logger.exception('Table data could not be retrieved: %s, message: %s',conector,  value)
                            json.dump(value, f)
    
    
    
    #print(qure_respons)
    fin_end_time = time.time()
    elapsed_time = fin_end_time - start_time
    logger.info('Finished %s Elapsed time: %s', conector, elapsed_time)
    print('Elapsed time: ', elapsed_time)

    #df = pd.DataFrame(qure_respons, columns=list(qure_respons[0].keys()))
    #print(df)

    #df.rename(columns ={'Bedrag_regelkorting__BV_': 'Bedrag_regelkorting_BV', 'ED_-_SOS_calculation': 'ED_SOS_calculation'}, inplace=True)



    #print(qure_respons[0].keys())
    #with open("names.txt", "w") as fp:
    #    json.dump(qure_respons, fp)
    try:
        with open(conector +'.csv',  'w', encoding = 'utf=8',  newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames = list(qure_respons[0].keys())) 
            writer.writeheader() 
            writer.writerows(qure_respons)
            #df.to_csv(file)
    except:
        logger.exception('Data is not written to a file: %s',conector +'.csv')

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
    print(list(n_list[0].keys()))
    

    del ListofCol[0]

    df = pd.DataFrame(n_list, columns=list(n_list[0].keys()))
    df.rename(columns ={'Bedrag_regelkorting__BV_': 'Bedrag_regelkorting_BV', 'ED_-_SOS_calculation': 'ED_SOS_calculation'}, inplace=True)
    #df.drop(['id'], axis=1)

    df = df[ListofCol]

    # Insert Dataframe into SQL Server:
    #df.to_sql('api_bi_salesorderlineitems',cnxn,if_exists='replace')
    col_list = ", ".join(ListofCol)
    col_list = col_list.replace('-_', '')

    placeholders = ", ".join(['?'] * len(col_list.split(", ")))
    mogrify = ",".join(['%s'] * len(col_list.split(", ")))

  

    #print(mogrify)
    #print(len(col_list.split(", ")))
    count = 0

    list_row = []
    for index, row in df.iterrows():
        #print(f"INSERT INTO api_bi_salesorderlineitems ({col_list}) values({placeholders})")
        list_row.append(str(tuple(list(row))))


        cursor.execute(f"INSERT INTO api_bi_salesorderlineitems ({col_list}) values({placeholders})",list(row))
        
        #print(count)
        count +=1
  
    insert_st  = f"INSERT INTO api_bi_salesorderlineitems({col_list}) values{list_row}"

    #print(", ".join(list_row))

    #cursor.execute(f"INSERT INTO api_bi_salesorderlineitems({col_list}) values{",".join(list_row)}")
    #cursor.execute(f"INSERT INTO api_bi_salesorderlineitems({col_list}) values{",".join(list_row[0])}")

    cnxn.commit()
    cursor.close()
    fin_end_time = time.time()
    elapsed_time = fin_end_time - start_time

    print('Elapsed time: ', elapsed_time)




if __name__ == '__main__':
    start_time = time.time()

    logging.basicConfig(filename='myapp.log', format="%(asctime)s - %(levelname)s - %(message)s",
        style="%",
        datefmt="%Y-%m-%d %H:%M", level=logging.INFO)
    logger.info('Started *********************************')

    list_get_conn = [
    "BI_Marge_Grootboekrekeningen",
    "BI_Marge_Financiele_mutaties",
    "BI_Warehouse",
    "BI_Verrekenprijs",
    "BI_verkoopprijs",
    "BI_verkoopfactuurregels",
    "BI_Verkoopfactuur",
    "BI_Stock_per_Warehouse",
    "BI_SalesOrders",
    "BI_SalesOrderLineItems_Changed_on_today",
    "BI_SalesOrderLineItems",
    "BI_Sales_teams",
    "BI_PurchaseOrderLineItems",
    "BI_Pakbonregels",
    "BI_Mutaties_Last_Stock_transfer",
    "BI_Items",
    "BI_Item_group",
    "BI_FbUnitBasicItem",
    "BI_DeliveryNoteLineItems",
    "BI_Debtors",
    "BI_DebtorInvoicesLineItems",
    "BI_Creditors",
    "BI_Artikelcockpit",
    #"BI_Article_image",
    "BI_Administrations",
    "BI_Mutaties_Afboekingen_Warehouse"]

    get_list_test =["BI_Warehouse","BI_Article_image"]

    get_list_hour =["BI_verkoopfactuurregels",
    "BI_Stock_per_Warehouse",
    
    ]
    exeption_list = []

    for get in get_list_hour:

        try:

            #daemon_thread = threading.Thread(target=get_afas_asyncio, args=(logger,get))
            #daemon_thread.setDaemon(True)
            #daemon_thread.start()

            get_afas_asyncio(logger,get)
            upload_to_sharepoint.send_to_sharepoint(logger,get)

        except:
            logger.exception('Table execution error: %s', get)

            exeption_list.append(get)

    fin_end_time = time.time()
    elapsed_time = fin_end_time - start_time
    logger.info('Finished **************************, Elapsed time: %s', elapsed_time)
    print('Elapsed time: ', elapsed_time)



    #send_to_sql()
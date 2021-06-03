#!/home/somefolder/anaconda3/bin/python3
# coding: utf-8

# # Задаем диапазон дат

# In[1]:


from datetime import datetime, timedelta
import pandas as pd
import re
from oauth2client.service_account import ServiceAccountCredentials
import tqdm


yesterday = (datetime.today() - timedelta(days=1)).date().strftime('%Y-%m-%d')  # если нужно вчера

date_from = yesterday
date_to = yesterday
date_from_ar = date_from.split('-')
date_to_ar = date_to.split('-')

# Устанавливаем диапазон дат для Adwords
min_ = ''.join(date_from.split('-'))
max_ = ''.join(date_to.split('-'))


# # Выгрузка расходов из Google Adwords

# In[2]:


import sys
import googleads
from googleads import adwords

from googleads.adwords import AdWordsClient
import pandas as pd


import os
this_dir = os.path.dirname(os.path.abspath(__file__))
file_googleads = os.path.join(this_dir, 'googleads.yaml')
client = adwords.AdWordsClient.LoadFromStorage(file_googleads)

# Initialize appropriate service.
report_downloader = client.GetReportDownloader(version='v201809')

# Create report definition.
report_definition = {
    'reportName': 'KEYWORDS_PERFORMANCE_REPORT',
    'dateRangeType': 'CUSTOM_DATE',
    'reportType': 'KEYWORDS_PERFORMANCE_REPORT',
    'downloadFormat': 'CSV',
    'selector': {
        "dateRange": {
            "min": min_,
            "max": max_
        },
        'fields': ['Date', 'Cost', 'Clicks', 'Impressions', 'CampaignId', 'CampaignName', 'Id',
                   'Criteria', 'AccountCurrencyCode', 'ExternalCustomerId']
    }
  }

res_adw = report_downloader.DownloadReportAsString(
          report_definition, skip_report_header=False,
          skip_column_header=False, skip_report_summary=True,
          include_zero_impressions=False)


# In[3]:


tokens = res_adw.split('\n')

lists = []
for line in tokens:
    if len(line.split(',')) > 1:
        lists.append(line.split(','))

        
stat_dict = {}

for i, line in enumerate(lists):
    if i == 1:                  # Первую строку записываем ключами словаря   
        for col in line:
            stat_dict[col] = []
    else:                    #   Другие строки записываем как значения ключей словаря
        for j, item in enumerate(line):
            if len(line) < 10:
                continue
            elif len(line) == 11:  # Этот блок обрабатывает строки, которые неправильно распарсились
                line[1] = ','.join([line[1], line[2]])
                line.pop(2)
                stat_dict[list(stat_dict.keys())[j]].append(line[j])                    
            else:
                stat_dict[list(stat_dict.keys())[j]].append(line[j])        
        
        
adw = pd.DataFrame(stat_dict)
adw


# In[4]:


# Преобразовываем типы для агрегирования
adw['Day'] = adw['Day'].apply(lambda x : datetime.strptime(x, '%Y-%m-%d').date())

df_types = {
    'Cost': 'float',
    'Clicks': 'int',
    'Impressions': 'int'
}

adw = adw.astype(dtype=df_types)


# Форматируем расходы в Adwords
adw['Cost'] = adw['Cost']/1000000


# Удаляем Total
try:
    if adw[adw['Day'].str.contains('Total')].index[0]:
        adw.drop(adw[adw['Day'].str.contains('Total')].index[0], axis=0, inplace=True)
except:
    pass

adw


# In[5]:


adw.rename(columns={'Day': 'date',
                      'Cost': 'cost',
                      'Clicks': 'clicks',
                      'Impressions': 'impressions',
                      'Campaign ID': 'campaignId',
                      'Campaign': 'campaign',
                      'Keyword ID': 'keywordId',
                      'Keyword': 'keyword',
                      'Currency': 'currency',
                      'Customer ID': 'accountId',
                     }, inplace=True  
)


# In[29]:


from datetime import datetime  
from datetime import timedelta 

import pandas as pd
from google import cloud
from google.cloud import bigquery

from google.oauth2 import service_account


DATE = min_
PROJECT = "some-project"
DATASET = "some_project"
TABLE = "googleAdsCosts_7140163225_{}".format(DATE)

this_dir = os.path.dirname(os.path.abspath(__file__))
file_json = os.path.join(this_dir, 'some-project-a9ba071e557c.json')
credentials = service_account.Credentials.from_service_account_file(file_json)

bq_client = bigquery.Client(project=PROJECT, credentials=credentials)

job_config = bigquery.LoadJobConfig(
    time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="date",  # name of the column to use for partitioning
        #expiration_ms=7776000000  # 90 days
        #require_partition_filter=True,
        
    ),
    write_disposition='WRITE_EMPTY'
)

load_job = bq_client.load_table_from_dataframe(
            adw, '.'.join([PROJECT, DATASET, TABLE]), job_config = job_config
)

result = load_job.result()

print("Written {} rows to {}".format(result.output_rows, result.destination))
print("Partitioning: {}".format(result.time_partitioning))


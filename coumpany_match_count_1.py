#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import mysql.connector as sql


# In[2]:


#db_connection2 = sql.connect(host='localhost', database='singapore_linkedin_output', user='root', password='sgllenovo')
db_connection1 = sql.connect(host='localhost', database='sgbizpro', user='root', password='sgllenovo')


# In[3]:


df2 = pd.read_csv('linkedin_company_get_uen.csv',low_memory=False,error_bad_lines=False)
df1 = pd.read_sql('SELECT * FROM organization_copy', con=db_connection1)


# In[40]:


company_names_s=df2['atcompany'].unique().tolist()
df2['atcompany'].unique().shape


# In[35]:


df = pd.DataFrame(columns=['linkedin_company', 'uen'])


# In[39]:


k=['PTE','LTD','PRIVATE','LIMITED']
dg=''
dgf='(?=.*'
dge=')'
count=1
for f in company_names_s:
    try:
        print(str(count)+' '+f)
        count+=1
        a=f.split()
        #print(a)
        dg=''
        company_n=[el for el in a if not any(ignore in el for ignore in k)]
        for company_word in company_n:
            dg+=dgf+company_word+dge
        #print(company_n)
        #print(dg)
        t=df1[df1['Organization Name'].str.contains(dg,regex=True,na=False)]
        try:
            df=df.append(pd.DataFrame([[f,str(t['Registration Number'])]],columns=df.columns))
        except Exception as e:
            print(f+' '+e)
        #print(f)
    except Exception as e:
        print(e)


# In[38]:


df.to_csv('LINKEDIN_company_uen_update.csv',sep=';',encoding='utf-8',index=None)


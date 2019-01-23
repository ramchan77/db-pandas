#!/usr/bin/env python
# coding: utf-8

# In[17]:


import pandas as pd
#


# In[23]:


#df1 = pd.read_csv('organization_copy.csv',sep=';',low_memory=False,error_bad_lines=False)
#df2 = pd.read_csv('geek_bing_mapped.csv',low_memory=False,error_bad_lines=False)


# In[105]:


df1 = pd.read_csv('organization_copy.csv',sep=';',low_memory=False,error_bad_lines=False)
df2 = pd.read_csv('hr_contacts.csv',sep=';',low_memory=False,error_bad_lines=False)


df2['uen']=''

company_names_s=df2['organization'].unique().tolist()[0:1500]


# In[316]:

count=1
k=['PTE.','LTD.','PTE','LTD','PRIVATE','LIMITED']
dg=''
dgf='(?=.*'
dge=')'
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
            if len(t['Registration Number'].tolist())==1:
                print(str(t['Registration Number'].tolist()[0]))
                df2.loc[df2['organization'] == f, 'uen'] = str(t['Registration Number'].tolist()[0])
        except Exception as e:
            print(f+' '+e)
        #print(f)
    except Exception as e:
        print(e)


# In[318]:


df2.to_csv('LINKEDIN_DATA_with_UEN_updated.csv',sep=';',encoding='utf-8',index=None)


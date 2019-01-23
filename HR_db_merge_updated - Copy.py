#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import mysql.connector as sql


# In[195]:


db_connection2 = sql.connect(host='localhost', database='singapore_linkedin_output', user='root', password='sgllenovo')
db_connection1 = sql.connect(host='localhost', database='django', user='root', password='sgllenovo')


# In[46]:


df2 = pd.read_sql("SELECT * from link_hr_copy", con=db_connection2)
df1 = pd.read_sql('SELECT * FROM outlook_hr', con=db_connection1)


# In[52]:


company_names_df2=df2['atcompany'].unique().tolist()
company_names_df1=df1[['company_name']]


# In[277]:


df3=pd.DataFrame(columns=df1.columns)
df4=pd.DataFrame(columns=df1.columns)


# In[313]:


k=['PTE','LTD','PRIVATE','LIMITED']
dg=''
dgf='(?=.*'
dge=')'
count=1



for f in company_names_df2:
    if f is None: continue
    if len(f)>3 and f!='SINGAPORE':
        try:
            if f is None: continue
            print(str(count)+' '+str(f))
            count+=1
            a=f.split()
            print(a)
            dg=''
            company_n=[el for el in a if not any(ignore in el for ignore in k)]
            for company_word in company_n:
                dg+=dgf+company_word+dge
            print(company_n)
            #print(dg)
            t=company_names_df1[company_names_df1['company_name'].str.contains(dg,regex=True,na=False)]
            df2s=df2.loc[df2['atcompany'].str.lower() == f.lower()]
            updated_name_list=[]
            #print(t.shape[0])
            for tt in t['company_name'].str.lower():
                #print(tt)
                df1s=df1.loc[df1['company_name'].str.lower() == tt]
                for df2s_name in df2s['Fullname'].str.lower():
                    srs1=df1s['full_name'].loc[df1s['full_name'].str.lower() == df2s_name]
                    #for df1s_name in df1s['full_name']:
                        #srs1=df1.loc[(df1['company_name'].str.lower() == tt) & (df1['full_name'].str.lower() == df2s_name)]
                        #df1.loc[(df1['company_name'].str.lower() == tt) & (df1['full_name'].str.lower() == df2s_name), 'Profile_link']=str(df2.loc[(df2['atcompany'].str.lower() == f.lower()) & (df2['Fullname'].str.lower() == df2s_name)]['Input_Link'].tolist()[0])
                    #print(srs1.shape[0])
                    #if srs1.shape[0]==0:
                        #try:
                            #srs=df2.loc[(df2['atcompany'].str.lower() == f.lower()) & (df2['Fullname'].str.lower() == df2s_name)]
                            #df3=df3.append(pd.DataFrame([[str(srs['Fullname'].tolist()[0]),str(srs['Position'].tolist()[0]),str(srs['atcompany'].tolist()[0]),str(srs['UEN'].tolist()[0]),str(srs['Connections'].tolist()[0]),str(srs['Input_Link'].tolist()[0])]],columns=[ u'full_name',u'designation',u'company_name',u'uen',u'Connections',u'Profile_link']))
                            #print('Added To DF3 : '+f+' ~~~ '+df2s_name+' ~~~')
                        #except Exception as e:
                            #print(e)
                    if srs1.shape[0]==1:
                        try:
                            df1.loc[(df1['company_name'].str.lower() == tt) & (df1['full_name'].str.lower() == df2s_name), 'Profile_link']=str(df2.loc[(df2['atcompany'].str.lower() == f.lower()) & (df2['Fullname'].str.lower() == df2s_name)]['Input_Link'].tolist()[0])
                            print('Profile Link Updated For : '+tt+' ~~~ '+df2s_name+' ~~~')
                            df4=df4.append(df1.loc[(df1['company_name'].str.lower() == tt) & (df1['full_name'].str.lower() == df2s_name)])
                            updated_name_list.append(df2s_name)
                        except Exception as e:
                            print(e)
            #print(updated_name_list)
            df2s_a=df2.loc[(df2['atcompany'].str.lower() == f.lower()) & (~df2.Fullname.str.lower().isin(list(set(updated_name_list))))]
            for df2s_name_a in df2s_a['Fullname'].str.lower():
                srs=df2.loc[(df2['atcompany'].str.lower() == f.lower()) & (df2['Fullname'].str.lower() == df2s_name_a)]
                df3=df3.append(pd.DataFrame([[str(srs['Fullname'].tolist()[0]),str(srs['Position'].tolist()[0]),str(srs['atcompany'].tolist()[0]),str(srs['UEN'].tolist()[0]),str(srs['Connections'].tolist()[0]),str(srs['Input_Link'].tolist()[0])]],columns=[ u'full_name',u'designation',u'company_name',u'uen',u'Connections',u'Profile_link']))
                print('Added To DF3 : '+f+' ~~~ '+df2s_name_a+' ~~~')
            #try:
                #df=df.append(pd.DataFrame([[f,str(t['Registration Number'])]],columns=df.columns))
            #except Exception as e:
                #print(f+' '+e)
            #print(f)
        except Exception as e:
            print(e)        


df1.to_csv('Outlook_HR_data_updated_1.csv',sep=';',encoding='utf-8',index=None)
df3.to_csv('Link_HR_data_updated_with_duplicates_1.csv',sep=';',encoding='utf-8',index=None)
df3.drop_duplicates().to_csv('Link_HR_data_updated_1.csv',sep=';',encoding='utf-8',index=None)
df4.drop_duplicates().to_csv('outlook_HR_data_updated_df4.csv',sep=';',encoding='utf-8',index=None)

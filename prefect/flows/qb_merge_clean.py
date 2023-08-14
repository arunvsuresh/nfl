#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[5]:


pd.set_option('display.max_columns', None)


# In[3]:


import nfl_data_py as nfl


# In[69]:


pfr = nfl.import_pfr(s_type='pass', years=[2019])


# In[70]:


qbr = nfl.import_qbr(years=[2019], frequency='weekly')


# In[76]:


qb_stats = pfr.merge(qbr, left_on='pfr_player_name', right_on='name_display', how='inner')


# In[85]:


qb_stats = qb_stats.drop(columns=['receiving_drop', 'receiving_drop_pct'])


# In[96]:


qb_stats = qb_stats.drop(columns=['def_times_blitzed', 'def_times_hurried', 'def_times_hitqb'])


# In[101]:


qb_stats = qb_stats.dropna()


# In[67]:


adv_stats = pd.read_parquet('advstats_week_pass_2019.parquet')


# In[68]:


ngs = pd.read_csv('ngs_2019_passing.csv.gz', compression='gzip')


# In[69]:


adv_stats[(adv_stats.pfr_player_name == 'Mitchell Trubisky')]


# In[70]:


ngs[(ngs.week == 1) & (ngs.player_display_name == 'Mitchell Trubisky')]


# In[71]:


adv_stats.pfr_player_name = adv_stats.pfr_player_name.str.strip()


# In[72]:


ngs.player_display_name = ngs.player_display_name.str.strip()


# In[73]:


df = pd.merge(adv_stats, ngs, left_on=['pfr_player_name', 'week'], right_on=['player_display_name', 'week'], how='inner')


# In[76]:


df[(df.pfr_player_name == 'Mitchell Trubisky')]


# In[ ]:





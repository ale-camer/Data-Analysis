"""
This script execute a Web Scraping process in the following URLs:
    
    - www.timesjobs.com
    - www.flexjobs.com

It takes, more or less, 10 hours to download all data from timesjobs. 

To run the algorithm it not need it a power computer but beware of 
keeping it with electricity supply.
"""


import re
import numpy as np
import pandas as pd
from tqdm import tqdm
import requests as req
from time import sleep
from bs4 import BeautifulSoup as bs
import warnings
warnings.filterwarnings("ignore")

print("Downloading data from timesjobs.com")
url = 'https://www.timesjobs.com/candidate/job-search.html?searchType=personalizedSearch&from=submit&txtKeywords=data+science&txtLocation='
html = req.get(url) # downloading data
soup = bs(html.text, 'lxml') # formatting data
jobs = soup.find_all('li', class_="clearfix job-bx wht-shd-bx") # finding all jobs in first page

data = pd.DataFrame(columns=['Company','Experience','Location','Key Skills','Posted','Description']) # creating empty Pandas dataframe
for job in jobs: # iterate over every job feature in the first page and storing them in the dataframe
    
    company = job.find('h3', class_="joblist-comp-name").text # name of the company
    location = job.find('ul', class_="top-jd-dtl clearfix").span.text # location of the company
    experience = job.find('ul', class_="top-jd-dtl clearfix").li.text.split('card_travel')[1] # experience required
    keySkills = job.find('ul', class_="list-job-dtl clearfix").text.split('KeySkills:')[1].replace(' ','').replace(',',', ') # skills required
    posted = job.find('span',class_='sim-posted').span.text # when was posted the job post
    
    new_url = job.find('ul', class_="list-job-dtl clearfix").a['href'] # getting job description url
    new_html = req.get(new_url) # downloading data
    new_soup = bs(new_html.text, 'lxml') # formatting data
    description = new_soup.find('div', class_="jd-desc job-description-main").get_text().replace('\n',' ').replace('\t',' ').replace('\r',' ').replace('Job Description','') # job description

    data = data.append({ # storing data
        'Company':company,
        'Experience':experience,
        'Location':location,
        'Key Skills':keySkills,
        'Posted':posted,
        'Description':description
        }, ignore_index=True)
    
i = 2 # iterator
supra_lista = [] # creting empty list in order to store lists with data
while True: # iterating over every web page 
    
    try:
        print(i)
    
        url2 = f'https://www.timesjobs.com/candidate/job-search.html?from=submit&actualTxtKeywords=python&searchBy=0&rdoOperator=OR&searchType=personalizedSearch&luceneResultSize=25&postWeek=60&txtKeywords=data+science&pDate=I&sequence={i}&startPage=1'
        html = req.get(url2) # downloading data
        soup = bs(html.text, 'lxml') # formatting data
        jobs = soup.find_all('li', class_="clearfix job-bx wht-shd-bx") # finding all jobs in each web page
        
        lista = [] # creating empty list in order to store data
        for job in jobs:
            
            company = job.find('h3', class_="joblist-comp-name").text.replace('\n','').replace('\r','') # name of the company
            location = job.find('ul', class_="top-jd-dtl clearfix").span.text # location of the company
            experience = job.find('ul', class_="top-jd-dtl clearfix").li.text.split('card_travel')[1] # experience required
            keySkills = job.find('ul', class_="list-job-dtl clearfix").text.split('KeySkills:')[1].replace(' ','').replace(',',', ').replace('\n','').replace('\r','') # skills required
            posted = job.find('span',class_='sim-posted').span.text # when was posted the job post
        
            new_url = job.find('ul', class_="list-job-dtl clearfix").a['href'] # getting job description url
            new_html = req.get(new_url) # downloading data
            new_soup = bs(new_html.text, 'lxml') # formatting data
            description = new_soup.find('div', class_="jd-desc job-description-main").get_text().replace('\n',' ').replace('\t',' ').replace('\r',' ').replace('Job Description','') # job description
    
            lista.append((company,location,experience,keySkills,posted,description)) # storing each job data
            
        supra_lista.append(lista) # storing all jobs data from each web page
            
        i += 1 # changing iterator value in order to scrape next page
        
        sleep(1)
        if len(lista) == 0: # if there is no data in the page the while loop will stop
            break
        
    except:
        pass
    
print("Preprocessing data")
dfTemp = pd.DataFrame() # creating temporary empty Pandas dataframe
for i in tqdm(range(len(supra_lista))): # iterating over list with each page data
    dfTemp = dfTemp.append(supra_lista[i],ignore_index=True) # storinge each page data in the temporary empty dataframe
dfTemp.columns = ['Company','Location','Experience','Key Skills','Posted','Description'] # changing column names
data = pd.concat([data,dfTemp],axis=0) # concatenating first page data with the rests
data.reset_index(inplace=True,drop=True) # reseting index

data['Posted ii'] = data.Posted.apply(lambda a: re.sub('[^0-9]','',a)) # keeping only numbers from the posted date column
for row in data.index: # formatting rows from posted date column without numbers
    if data.loc[row,'Posted'] == 'Posted today': data.loc[row,'Posted ii'] = 0 # today data is equal to 0
    elif data.loc[row,'Posted'] == 'Posted a month ago': data.loc[row,'Posted ii'] = 30 # data from more than a month ago is equal to 30
    elif data.loc[row,'Posted'] == 'Posted few days ago': data.loc[row,'Posted ii'] = 7 # data posted between the 7th and 31th day of the month is equal to 7
    else: pass
data['Posted ii'] = pd.to_numeric(data['Posted ii']) # changing column format to numeric

data.to_csv('timesjobs.csv') # saving data

print("Downloading data from flexjobs.com")
url = 'https://www.flexjobs.com/search?search=data+science&location='
html = req.get(url) # downloading data 
soup = bs(html.text, 'lxml') # formatting data
jobs = soup.find_all('li', {"class":"m-0 row job"}) # finding all jobs in first page

data = pd.DataFrame(columns=['Title','Posted','Flexibility','Nationality','Job Type','Career Level','Travel Required','Hours per Week',
                             'Company','Benefits','Description']) # creating empty Pandas dataframe
for job, i in zip(jobs, range(len(jobs))): # iterate over every job feature in the first page and storing them in the dataframe
    
    print(i) # page number
    title = job.find('div',{'class':'row'}).a.text # job title
    posted = job.find('div',{'class':'job-age'}).text.replace('\n','') # when was posted the job post
    flexibility = job.find('span',{'class':'job-tag d-inline-block me-2 mb-1'}).get_text() # job flexibility
    nationality = job.find('div',{'class':'col pe-0 job-locations text-truncate'}).text.replace('\n','').replace('\xa0','') # nationality

    new_url = 'https://www.flexjobs.com/' + job.find('div', class_="col text-nowrap pe-0").a['href'] # job description url
    new_html = req.get(new_url) # downloading data
    new_soup = bs(new_html.text, 'lxml') # formatting data
    
    try: # job type
        job_type = new_soup.find('table',{'class':'job-details'}).get_text().split('Job Type:')[1].replace('\n',' ').split(' ')[1]
    except:
        pass
    try: # career level
        career_level = new_soup.find('table',{'class':'job-details'}).get_text().split('Career Level:')[1].replace('\n',' ').split(' ')[1]
    except:
        pass
    try: # travel required
        travel_required = new_soup.find('table',{'class':'job-details'}).get_text().split('Travel Required:')[1].replace('\n',' ').split(' ')[1]
    except:
        travel_required = np.nan
    try: # hours per week
        hours_per_week = new_soup.find('table',{'class':'job-details'}).get_text().split('Hours per Week:')[1].replace('\n',' ').split(' ')[1]
    except:
        hours_per_week = np.nan
    try: # company
        company = new_soup.find('table',{'class':'job-details'}).get_text().split('Company:')[1].replace('\n',' ').split(' ')[1]
    except:
        pass
    try: # job benefits
        benefits = new_soup.find('table',{'class':'job-details'}).get_text().split('Benefits:')[1].replace('\n',' ').split(' ')[1]
    except:
        pass
    try: # job description
        description = new_soup.find('div', class_="job-description").text
    except:
        pass

    data = data.append({ # storing first page data
        'Title':title,
        'Posted':posted,
        'Flexibility':flexibility,
        'Nationality':nationality,
        'Job Type':job_type,
        'Career Level':career_level,
        'Travel Required':travel_required,
        'Hours per Week':hours_per_week,
        'Company':company,
        'Benefits':benefits,
        'Description':description}, ignore_index=True)

i = 2 # iterator
supra_lista = [] # creting empty list in order to store lists with data
while True:
    
    try:
        print(i) # page number
    
        url2 = f'https://www.flexjobs.com/search?location=&page={i}&search=data+science'
        html = req.get(url2) # downloading data
        soup = bs(html.text, 'lxml') # formatting data
        jobs = soup.find_all('li', {"class":"m-0 row job"}) # finding all jobs in each web page
        
        lista = []
        for job in jobs:
            
            title = job.find('div',{'class':'row'}).a.text # job title
            posted = job.find('div',{'class':'job-age'}).text.replace('\n','') # when job was posted
            flexibility = job.find('span',{'class':'job-tag d-inline-block me-2 mb-1'}).get_text() # job flexibility
            nationality = job.find('div',{'class':'col pe-0 job-locations text-truncate'}).text.replace('\n','').replace('\xa0','') # job nationality
        
            new_url = 'https://www.flexjobs.com/' + job.find('div', class_="col text-nowrap pe-0").a['href'] # each page url
            new_html = req.get(new_url) # downloading data
            new_soup = bs(new_html.text, 'lxml') # formatting data
            
            try: # job type
                job_type = new_soup.find('table',{'class':'job-details'}).get_text().split('Job Type:')[1].replace('\n',' ').split(' ')[1]
            except:
                job_type = np.nan
            try: # career level
                career_level = new_soup.find('table',{'class':'job-details'}).get_text().split('Career Level:')[1].replace('\n',' ').split(' ')[1]
            except:
                np.nan
            try: # travel required
                travel_required = new_soup.find('table',{'class':'job-details'}).get_text().split('Travel Required:')[1].replace('\n',' ').split(' ')[1]
            except:
                travel_required = np.nan
            try: # hours per week
                hours_per_week = new_soup.find('table',{'class':'job-details'}).get_text().split('Hours per Week:')[1].replace('\n',' ').split(' ')[1]
            except:
                hours_per_week = np.nan
            try: # company
                company = new_soup.find('table',{'class':'job-details'}).get_text().split('Company:')[1].replace('\n',' ').split(' ')[1]
            except:
                pass
            try: # job benefits
                benefits = new_soup.find('table',{'class':'job-details'}).get_text().split('Benefits:')[1].replace('\n',' ').split(' ')[1]
            except:
                pass
            try: # job description
                description = new_soup.find('div', class_="job-description").text
            except:
                pass
    
            lista.append((title,posted,flexibility,nationality,job_type,career_level, # storing each job data
                          travel_required,hours_per_week,company,benefits,description))
            
        supra_lista.append(lista) # storing all jobs data from each web page
            
        i += 1 # changing iterator value in order to scrape next page
        
        sleep(1)
        if len(lista) == 0: # if there is no data in the page the while loop will stop
            break
        
    except: pass

print("Preprocessing data")
dfTemp = pd.DataFrame() # creating temporary empty Pandas dataframe
for i in tqdm(range(len(supra_lista))): # iterating over list with each page data
    dfTemp = dfTemp.append(supra_lista[i],ignore_index=True) # storinge each page data in the temporary empty dataframe
dfTemp.columns = ['Title','Posted','Flexibility','Nationality','Job Type','Career Level','Travel Required','Hours per Week',
                             'Company','Benefits','Description'] # changing column names
data = pd.concat([data,dfTemp],axis=0) # concatenating first page data with the rests
data.reset_index(inplace=True,drop=True) # reseting index

data['Posted ii'] = data.Posted.apply(lambda a: re.sub('[^0-9]','',a)) # keeping only numbers from the posted date column
data['Posted ii'] = np.where(data['Posted ii'] == '', 0, data['Posted ii'])
data.to_csv('flexjobs.csv') # saving data
##### FUNCTION TO CONVERT PMCID TO PMID

import urllib.request as request
import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError, HTTPError, Timeout
from urllib3.util.retry import Retry
import json

def ncbi_id_converter_batch(retrieved_df, email):

    # we only need one type of id per article to check the NCBI API
    # the pmid should be the first choice rather than doi
    
    # we are going to be resetting the index several times so lets create a new column to hold the current index
    retrieved_df['index'] = retrieved_df.index 
    
    
    # get the pmid_list
    pmid_list = []
    doi_list = []
    pmcid_list = []
    
    # lets get one uid for each record 
    for index, row in retrieved_df.iterrows():
        if type(row['pmid']) == str:
            pmid_list.append(row['pmid'])
        elif type(row['doi']) == str:
            doi_list.append(row['doi'])
        elif type(row['pmcid']) == str:
            pmcid_list.append(row['pmcid'])
            
    print(f'Now running the NCBI converter.\nWe have {len(pmid_list)} PMIDs, {len(doi_list)} DOIs and {len(pmcid_list)} PMCIDs to search for')
    
    if len(pmid_list) == 0 and len(doi_list) == 0 and len(pmcid_list) == 0:
        print('The previous step failed, please wait a few minutes and try again with the same command')
        exit()

    base_url = 'https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/'
    email = email
    headers= {'Accept':'application/json'}

    for input_list in ['pmid_list', 'doi_list', 'pmcid_list']:
        
        # set the appropriate column as the index
        col = str(input_list).replace('_list', '')
        print(f'Working on the {col} List...')
        retrieved_df.set_index(col, drop=False, inplace = True)
        
        # each process starts at record 0 and retrieves in batches up to a max of 200 per batch.
        start = 0
        batch_size = 200

        # set the total number to check
        total = len(input_list)
       
        # create a while loop to run the batches until the end of the list
        while start < total:
            batch_size = min(batch_size, (total - start))
            end = start + batch_size
            
            # set the parameters for the api
            params = {'ids':str(','.join(input_list[start:end])),
                     'tool':"genepopi_search_developer",
                     'versions':'no',
                     'format':'json',
                     'email':email}
            
            
            attempt = 1
            while attempt <3:
                try:
                    # send the request using the batch list of pmids
                    r = requests.get(base_url, headers=headers,params=params)
                    # ensure the status response is ok e.g. 2**
                    r.raise_for_status()
                    attempt = 3 
                    # convert json to dictionary
                    response =json.loads(r.text)

                    # write the dictionaries to the holding list
                    records = response['records']
                    
                    # now we have 200 records to loop through.
                    # we can use the new index to fill in the missing values for the other ids.
                    # check the record for a error message (when the id is not known)
                    for record in records:
                        status = record.get('status')
                        # if there is no error message we can try parse out the extra ids
                        if status != 'error':
                            # get the index to update the values
                            i = record.get(col)
                            # loop through each key value pair and insert the value if missing
                            for key, value in record.items():
                                if type(retrieved_df.at[i,key]) == float:
                                    retrieved_df.at[i,key] = value
                                 
                        
                        
                except:
                    attempt+=1
                    pass
            # iterate the batch size for our request    
            start += batch_size
    # now lets reset the retrieved_df index back to the original index
    retrieved_df.set_index('index', drop =True, inplace = True)

    return retrieved_df

###### INSTALLING EDIRECT TO USE THE NCBI API TO COLLECT DATA

import os
import wget

def output_files ():
    if os.path.isfile('./output/medline/edirect_setup.sh') == False:
        # check for the Edirect setup and install it if not already present
        path_to_edirect = str(str('export PATH=${\PATH}:') + str(os.path.realpath('.')) + str('/output/medline/edirect'))
        path_to_edirect = path_to_edirect.replace('\\', '')
        edirect_setup_script = f'''#!/bin/bash
        # this script aims to set up edirect for use in cadmus
        # first check if edirect is present in the home directory 
        echo checking if edirect is already installed
        DIR="./output/medline/edirect"
        if [ -d "$DIR" ]; then
        check=1
        else
        check=0
        fi
        if (($check ==0)); then 
        echo edirect not installed, begining download
        yes | sh -c "$(./output/medline/install-edirect.sh)"
        echo "{path_to_edirect}" >> ./.bash_profile
        echo "export NCBI_API_KEY=$1" >> ./.bash_profile
        echo install finished
        else 
        echo edirect already installed
        exit;
        fi'''

        edirect_setup_script = edirect_setup_script.split('\n')
        for i in range(len(edirect_setup_script)):
            while edirect_setup_script[i][0] == ' ':
                edirect_setup_script[i] = edirect_setup_script[i][1:]

        with open('./output/medline/edirect_setup.sh', 'w') as fp:
            for item in edirect_setup_script:
                # write each item on a new line
                fp.write(item)
                fp.write('\n')
        
        os.chmod("./output/medline/edirect_setup.sh", 0o755)

    if os.path.isfile('./output/medline/install-edirect.sh') == False:
        url = "https://ftp.ncbi.nlm.nih.gov/entrez/entrezdirect/install-edirect.sh"
        wget.download(url, out=f"./output/medline")
        print('\n')
        f = open("./output/medline/install-edirect.sh", "r")
        edirect_install_script = f.read()
        f.close()
        os.remove("./output/medline/install-edirect.sh")
        edirect_install_script = edirect_install_script.split('\n')
        edirect_install_script = edirect_install_script[:93]
        with open('./output/medline/install-edirect.sh', 'w') as fp:
            for item in edirect_install_script:
                if item != 'cd ~':
                    # write each item on a new line
                    fp.write(item)
                    fp.write('\n')
                else:
                    fp.write('cd ./output/medline')
                    fp.write('\n')
                
        os.chmod("./output/medline/install-edirect.sh", 0o755)


#### PIPELINE MODULE TO RUN TERMINAL COMMAND IN PYTHON

# import the necessary libraries
import sys
import shutil
import subprocess
import shlex

# here are the functions used by edirect
def execute(cmmd, data=""):
    if isinstance(cmmd, str):
        cmmd = shlex.split(cmmd)
    res = subprocess.run(cmmd, input=data,
                        capture_output=True,
                        encoding='UTF-8')
    return res.stdout.strip()

def pipeline(cmmds):
    def flatten(cmmd):
        if isinstance(cmmd, str):
            return cmmd
        else:
            return shlex.join(cmmd)
    if not isinstance(cmmds, str):
        cmmds = ' | '.join(map(flatten, cmmds))
    res = subprocess.run(cmmds, shell=True,
                        capture_output=True,
                        encoding='UTF-8')
    return res.stdout.strip()

def efetch(*, db, id, format, mode=""):
    cmmd = ('efetch', '-db', db, '-id', str(id), '-format', format)
    if mode:
        cmmd = cmmd + ('-mode', mode)
    return execute(cmmd)

##### FUNCTION TO CONVERT THE PMCID IN ACTUAL DATA    

import subprocess
import os
from cadmus.retrieval.edirect import pipeline
import glob 
import zipfile

# we can use the search terms provided to query pubmed using edirect esearch.
# This will provide us with a text file of papers in medline format
def search_terms_to_medline(query_string, api_key):
    if os.path.isfile('./.bash_profile') == False:
        if os.path.isdir('./output/medline/edirect') == False:
            pass
        else:
            path_to_edirect = str(str('export PATH=${\PATH}:') + str(os.path.realpath('.')) + str('/output/medline/edirect'))
            path_to_edirect = path_to_edirect.replace('\\', '')
            bash_profile_file = [
                path_to_edirect,
                f"export NCBI_API_KEY={api_key}"
            ]
            with open('./.bash_profile', 'w') as fp:
                for item in bash_profile_file:
                    # write each item on a new line
                    fp.write(item)
                    fp.write('\n')
    else:
        os.remove('./.bash_profile')
        path_to_edirect = str(str('export PATH=${PATH}:') + str(os.path.realpath('.')) + str('/output/medline/edirect'))
        path_to_edirect = path_to_edirect.replace('\\', '')
        bash_profile_file = [
            path_to_edirect,
            f"export NCBI_API_KEY={api_key}"
        ]
        with open('./.bash_profile', 'w') as fp:
            for item in bash_profile_file:
                # write each item on a new line
                fp.write(item)
                fp.write('\n')
    subprocess.call(["./output/medline/edirect_setup.sh", api_key])
    # send the query string by esearch then retrieve by efetch in medline format
    if type(query_string) == str:
        search_results = pipeline(f'esearch -db pubmed -query "{query_string}" | efetch -format medline')
        if len(glob.glob(f'./output/medline/txts/*.zip')) == 0:
            with zipfile.ZipFile("./output/medline/txts/medline_output.txt.zip", mode="a", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zip_file:
                zip_file.writestr("medline_output.txt", data=search_results)
                zip_file.testzip()
            zip_file.close()
            print('Medline Records retrieved and saved')
        else:
            with zipfile.ZipFile("./output/medline/txts/medline_output.txt.zip", "r") as z:
                for filename in z.namelist():
                    with z.open(filename) as f:
                        d = f.read()
                    f.close()
            z.close()
            d = str(str(d.decode('utf-8')) + '\n' + '\n' + str(search_results)).encode('utf-8')
            os.rename('./output/medline/txts/medline_output.txt.zip', './output/medline/txts/temp_medline_output.txt.zip')
            with zipfile.ZipFile("./output/medline/txts/medline_output.txt.zip", mode="a", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zip_file:
                zip_file.writestr("medline_output.txt", data=d)
                zip_file.testzip()
            zip_file.close()
            os.remove('./output/medline/txts/temp_medline_output.txt.zip')
            print('Medline Records retrieved and saved')
    else:
        #to avoid errors for large pmids list. We now chunk into smaller set of 9000. Finally we append every chunk in the medline text file.
        for i in range(len(query_string)):
            search_results = pipeline(f'esearch -db pubmed -query "{query_string[i]}" | efetch -format medline')
            if len(glob.glob(f'./output/medline/txts/*.zip')) == 0:
                with zipfile.ZipFile("./output/medline/txts/medline_output.txt.zip", mode="a", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zip_file:
                    zip_file.writestr("medline_output.txt", data=search_results)
                    zip_file.testzip()
                zip_file.close()
                print('Medline Records retrieved and saved')
            else:
                with zipfile.ZipFile("./output/medline/txts/medline_output.txt.zip", "r") as z:
                    for filename in z.namelist():
                        with z.open(filename) as f:
                            d = f.read()
                        f.close()
                z.close()
                d = str(str(d.decode('utf-8')) + '\n' + '\n' + str(search_results)).encode('utf-8')
                os.rename('./output/medline/txts/medline_output.txt.zip', './output/medline/txts/temp_medline_output.txt.zip')
                with zipfile.ZipFile("./output/medline/txts/medline_output.txt.zip", mode="a", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zip_file:
                    zip_file.writestr("medline_output.txt", data=d)
                    zip_file.testzip()
                zip_file.close()
                os.remove('./output/medline/txts/temp_medline_output.txt.zip')
            print(f'Medline Records retrieved and saved {i+1} out of {len(query_string)}')




### YOU CAN REQUEST 9,000 RECORDS AT THE TIME, SINCE OUR LIST IN MUCH BIGGER YOU NEED TO LOOP OVER IT BY BIN OF 9,000 IDS


if 9000 < len(input_function):
    print('Your list of PMIDs is greater than 9000, creating bins of 9000.')
chunks = [(',').join(input_function[x:x+9000]) for x in range(0, len(input_function), 9000)]
search_terms_to_medline(chunks, api_key)

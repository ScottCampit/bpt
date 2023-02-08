#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
bpt.py consists of functions that streamline BPT data I/O for quicker analyses, storing each workbook
into a BPT class object. Each workbook within the BPT class is stored as a Pandas DataFrame object.
"""
import os
import re
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import zipfile

import pandas as pd

def load_data_dict(dir_to_query:str) -> pd.DataFrame:
    """Find zipped directory containing the string 'dictionary' for a given year."""
    
    data_dict_flag = re.compile("dictionary")
    assert os.path.exists(dir_to_query), f"Path {dir_to_query} doesn't exist."
    for _, _, files in os.walk(dir_to_query):
        for file in files:
            if data_dict_flag.search(file):
                full_path_to_zip = os.path.join(dir_to_query, file)
                if full_path_to_zip.endswith('.zip'):
                    zippedFile = zipfile.ZipFile(full_path_to_zip, "r")
                    for zippedObj in zippedFile.namelist():
                        if zippedObj.endswith('.csv'):
                            data_dict = pd.read_csv(zippedFile.open(zippedObj))
    return data_dict

def load_sheet(dir_to_query:str, worksheet:str) -> pd.DataFrame:
    """
    Loads sheets corresponding to different sections of BPT. 
    An exception would be sheet 5 (county data), which spans multiple text files.

    Filename choices have the general form ma_x, where x denotes a specific sheet number.
    """
    years_for_exception = ['2014', '2015']
    for _, _, objs in os.walk(dir_to_query):
        tmp = list(objs)
        if any(x in dir_to_query for x in years_for_exception):
            dir_of_interest = [d for d in tmp if('_ma' in d)]
        else:
            dir_of_interest = [d for d in tmp if(worksheet in d)]
            
        for d in dir_of_interest:
            zmaobj = zipfile.ZipFile(os.path.join(dir_to_query, d), "r")
            with zmaobj as f:
                for name in f.namelist():
                    if worksheet in name:
                        df = pd.read_csv(zmaobj.open(name),
                                        sep='\t', 
                                        engine='python', 
                                        encoding='cp1252')
    return df

def load_county_data(dir_to_query:str, worksheet:str="ma_5") -> pd.DataFrame:
    """Load up benchmark data, which spans multiple zipped directories or text files by county."""
    county = []
    ma5_flag = re.compile(worksheet)
    years_for_exception = ['2014', '2015']
    for _, _, objs in os.walk(dir_to_query):
        tmp = list(objs)
        for obj in tmp:
            if ma5_flag.search(obj):
                fullzippath = os.path.join(dir_to_query, obj)
                zf = zipfile.ZipFile(fullzippath, "r")
                for f in zf.namelist():
                    if any(x in dir_to_query for x in years_for_exception):
                        if 'ma_5' in f: # Needed to hardcode 'ma_5' search since worksheet needs to contain '_ma'
                            if f.endswith('.txt'):
                                tmp = pd.read_csv(zf.open(f), 
                                                    sep='\t', 
                                                    engine='python', 
                                                    encoding='cp1252')
                                county.append(tmp)
                    else:
                        if f.endswith('.txt'):
                            tmp = pd.read_csv(zf.open(f), 
                                                sep='\t', 
                                                engine='python', 
                                                encoding='cp1252')
                            county.append(tmp)
    df = pd.concat(county, axis=1)
    return df

def replace_field_to_name(data_dict: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    """Replace df identifiers with whole name for improving interpretibility."""
    return df.rename(columns=dict(zip(data_dict['NAME'], 
                                  data_dict.iloc[:, data_dict.columns.str.contains(pat='FIELD')].squeeze())))

def data_loader(dirpath:str, worksheet:str, data_dict:pd.DataFrame):
    """Wrap functions above to load data into class field."""
    df = load_sheet(dirpath, worksheet)
    renamed_df = replace_field_to_name(data_dict, df)
    return renamed_df.dropna(subset=['BID ID (H-number, Plan ID, Segment ID)'])

class BPT:
    def __init__(self, yeardir):
        self.year = yeardir.split('/')[-2]
        self.data_dict = load_data_dict(yeardir)
        print(f"LOADING {self.year} BPT DATA")

        print("LOADING BASE PERIOD EXPERIENCE ('ma_1') DATA")
        self.ma_1   = data_loader(yeardir, worksheet='ma_1', data_dict=self.data_dict)
        print("LOADING PROJECTED ALLOWED COST ('ma_2') DATA")
        self.ma_2   = data_loader(yeardir, worksheet='ma_2', data_dict=self.data_dict)
        print("LOADING PROJECTED COST SHARING ('ma_3') DATA")
        self.ma_3   = data_loader(yeardir, worksheet='ma_3', data_dict=self.data_dict)
        print("LOADING PROJECTED REV REQUIREMENT ('ma_4') DATA")
        self.ma_4   = data_loader(yeardir, worksheet='ma_4', data_dict=self.data_dict)
        print("LOADING BENCHMARK ('ma_5') DATA")
        self.ma_5   = data_loader(yeardir, worksheet='ma_5', data_dict=self.data_dict)
        print("LOADING BID SUMMARY ('ma_6') DATA")
        self.ma_6   = data_loader(yeardir, worksheet='ma_6', data_dict=self.data_dict)
        print("LOADING OPTIONAL SUPPLEMENTAL BENEFITS ('ma_7') DATA")
        self.ma_7   = data_loader(yeardir, worksheet='ma_7', data_dict=self.data_dict)
        

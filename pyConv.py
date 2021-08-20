from logging import StrFormatStyle
import pandas as pd
import re
import json
import uuid
import warnings
import numpy as np
from pandas.io.formats.format import DataFrameFormatter
import streamlit as st
import os
import base64
from io import BytesIO


from streamlit.type_util import data_frame_to_bytes



def remove_rcna(df):
    """
    Removes empty columns and rows from df
    """
    df.dropna(how = 'all', axis = 'columns', inplace = True)
    df.dropna(how = 'all', axis = 'rows', inplace = True)
    return df

#===========================================================================================================================================
#TODO: add something to catch typos

def verLocal(df): 
    """ 
    Creates verbatimLocality column from user specified columns
    """

    locality_cols= []
    df = df.assign(verbatimLocality = "")

    print(df.columns)

    while True:
        entry = input('Please select columns from dataframe to add to verbatimLocality (type d when done): ')
        if entry.lower() == 'd':
            break
       # elif entry.lower() not in df.columns:
       #     print ("Column not found in dataframe")
       #     continue
        else:
            locality_cols.append(entry)

    df["verbatimLocality"] = df[locality_cols].astype(str).apply(", ".join, axis=1)

    return df

#===========================================================================================================================================
#TODO: This needs to be modified to handle universal data
#HOW: Let the user decide which words convert to which materialSampleType
#print out unique list of what's in there
#ask them to write a dictionary or fix it; column of theirs and fill in column with options

def matSampType(df):
  ## More description to status column -- in connection with GENOME

  dct = pd.read_csv("https://raw.githubusercontent.com/futres/fovt-data-mapping/master/Mapping%20Files/MST_dict.csv")

  if df["Status"].eq(dct["userTerm"]):
    inpt = input(f'Would you like to replace {df["Status"]} with {dct["userTerm"]}? ')

    if inpt.lower() == "yes":
      df["Status"] = str(dct["userTerm"])

  else:
    ask = input(f'Whould you like to replace {df["Status"]}?')

    if ask.lower() == "yes":
      replace = input(f'What would you like to replace {df["Status"]} with? ')
      df["Status"] = str(replace)
      dct["userTerm"].append(df["Status"])
      dct["replacedWith"].append(str(replace))

  dct.to_csv('MST_dict.csv')
  
  return(df, dct)

#===========================================================================================================================================
#TODO: make for non-english labels

def sex(df):
    """ 
    Standardizes sex values with GEOME vocabulary 
    """
    female = df['Sex'].eq("F", "f")
    male = df['Sex'].eq("M", "m")
    df['Sex'][(female == False)&(male==False)] = "not collected"
    df['Sex'][female == True] = "female"
    df['Sex'][male == True] = "male"
    return df

#===========================================================================================================================================

def inConv(df):
    """
    Converts length from inches to millimeters
    """
    df['Length'] = df['Length'] * 25.4
    return df

#===========================================================================================================================================

def lbsConv(df):
    """
    Converts weight from pounds to grams
    """
    df['Weight'] = df['Weight'] * 453.59237
    return df

#===========================================================================================================================================

def cmConv(df):
    """
    Converts length from cenitmeters to millimeters
    """
    df['Length'] = df['Length'] * 10
    return df

#===========================================================================================================================================

def kgConv(df):
    """
    Converts weight from kilograms to grams
    """
    df['Weight'] = df['Weight'] * 1000
    return df

#===========================================================================================================================================

def mConv(df):
    """
    Converts length from meters to millimeters
    """
    df['Length'] = df['Length'] * 1000
    return df

#===========================================================================================================================================

def mgConv(df):
    """
    Converts weight from milligrams to grams
    """
    df['Weight'] = df['Weight'] / 1000
    return df

#===========================================================================================================================================
#ask which column is EventDate or use column eventDate (should have it based off READ.md)

def yc(df):
    """
    Create and populate yearCollected through the date column
    """
    df = df.assign(yearCollected = df['Date'].str[:4])
    df = df.rename(columns = {"Date" : "verbatimEventDate"})
    return df

#===========================================================================================================================================

def colcheck(df):
    """
    Checks dataframe columns and flags column names that do not 
    match with template. 
    Template found here: https://github.com/futres/template/blob/master/template.csv
    """
    print("Checking Dataframe Columns")

    geome_col_names = pd.read_csv("https://raw.githubusercontent.com/futres/fovt-data-mapping/master/Mapping%20Files/template_col_names.csv")
    df_col_names = df.columns
    error = str(list(set(df_col_names) - set(geome_col_names["Template Column Names"])))
    required_columns = ['eventID', 'country','locality','yearCollected','samplingProtocol',
                        'materialSampleID', 'basisOfRecord','scientificName','diagnosticID',
                        'measurementMethod','measurementUnit','measurementType','measurementValue']
    missing_req = str(list(set(required_columns) - set(df_col_names)))
        
#have it break if the set difference isn't zero

    output = str(f"These column names do not match the template: {error} \n These required columns are missing: {missing_req}")
    return output

#    # renames columns through user input
#    col_names = []
#    for i in range(len(df.columns)):
#        inpt = input("What would you like column " + str(i + 1) + " to be named?: ")
#        col_names.append(inpt)
#    df.columns = col_names
#    return df

#===========================================================================================================================================

def countryValidity(df):
    '''
    Checks to make sure all country names are valid according the GENOME.
    Valid countries can be found here: https://github.com/futres/fovt-data-mapping/blob/ade4d192a16dd329364362966eaa01d116950e1d/Mapping%20Files/geome_country_list.csv
    '''
    #print("Checking Validity of Countries")

    if "country" in df.columns:
        GENOMEcountries = pd.read_csv("https://raw.githubusercontent.com/futres/fovt-data-mapping/master/Mapping%20Files/geome_country_list.csv")
        invalid = str(list(set(df["country"]) - set(GENOMEcountries["GEOME_Countries"])))
        output = str(f"These countrys found in your data are not recognized by GEOME: {invalid}")
        return output
    else:
        output = str("The ""country"" column was not found in your dataframe, to apply the ""Country Validity"" function this column is required.")
        return output

#===========================================================================================================================================

def add_ms_and_evID(df):
    """
    Adds unique hex value materialSampleID and eventID to dataframe
    """
    df = df.assign(materialSampleID = [uuid.uuid4().hex for _ in range(len(df.index))])
    return df

#===========================================================================================================================================
#TODO: dynamically update the id_vars with everything accept the term columns
#How: Let the user give the column names or range id_vars

def dataMelt(df):
    """
    Converts dataframe into long format
    """
    dataCols = []
    dfCols = df.columns.values.tolist()
    df = df.assign(verbatimLocality = "")

    print(df.columns)

    while True:
        entry = input('Please select columns from dataframe to melt (type d when done): ')
        if entry.lower() == 'd':
            break
       # elif entry.lower() not in df.columns:
       #     print ("Column not found in dataframe")
       #     continue
        else:
            dataCols.append(entry)

    VARS = list(set(dfCols) - set(dataCols))

    ID_VARS = np.array(VARS)

    df = pd.melt(df, id_vars = ID_VARS, value_vars = dataCols, var_name = 'measurementType', value_name = 'measurementValue')
    return df

def get_table_download_link(df):
    """Generates a link allowing the data in a given panda dataframe to be downloaded
    in:  dataframe
    out: href string
    """
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()  # some strings <-> bytes conversions necessary here
    href = f'<a href="data:file/csv;base64,{b64}">Download csv file</a>'



#===========================================================================================================================================


st.write("""
# Data Cleaning - [READme](https://github.com/futres/fovt-data-mapping/tree/master/Scripts/pythonConversion#readme)
""")


page_names = ['matSampType', 'verLocal', 'sex', 'uc', 'yc', 'colcheck', 'countryValidity', 'eventID', 'dataMelt' ]

uploadedFile = st.file_uploader("Upload a CSV file", type=['csv'],accept_multiple_files=False)
if uploadedFile:
    df = pd.read_csv(uploadedFile)

    st.write("""
    # Data Pre-Cleaning
    """)
    st.dataframe(df,width=700, height=208)

    st.write(""" 
    # Select which functions you would like applied:""")
    matSamp = st.checkbox('Material Sample Type')
    verLoc = st.checkbox('Verbatim Locality')
    sex_var = st.checkbox('Sex')
    uc = st.checkbox('Unit Conversions')
    yc_var = st.checkbox('Year Collected')
    colCheck = st.checkbox('Column Check')
    counVald = st.checkbox('Country Validity')
    eveID = st.checkbox('Material Sample ID')
    datamelt = st.checkbox('Data Melt')

    if True:
        if matSamp:
            df = remove_rcna(df)
            if "materialSampleType" in df.columns:
                dct = pd.read_csv("https://raw.githubusercontent.com/futres/fovt-data-mapping/master/Mapping%20Files/MST_dict.csv")

                if (df["materialSampleType"].eq(dct["userTerm"])).any():
                    inpt = st.text_input(f'Would you like to replace {df["materialSampleType"]} with {dct["userTerm"]}? ')

                    if inpt.lower() == "yes":
                        df["materialSampleType"] = str(dct["userTerm"])
                    if inpt.lower() == "no":
                        count = 0
                else:
                    ask = st.text_input(f'Whould you like to replace {df["materialSampleType"].unique()}?')

                    if ask.lower() == "yes":
                        replace = st.text_input(f'What would you like to replace {df["materialSampleType"].unique()} with? ')
                        if replace:
                            uniq_vals = []
                            replace_split = []
                            uniq_vals = df["materialSampleType"].unique()
                            d = {}
                            replace_split = replace.split(",")
                            for i in range(len(uniq_vals)):
                                replace = df['materialSampleType'].eq(uniq_vals[i])
                                replacement = replace_split[i]
                                df['materialSampleType'][(replace) == True] = replacement
                                df['materialSampleType'][(replace) == False] = df['materialSampleType']
                                uniq_vals_new = pd.Series(uniq_vals)
                                #uniq_vals_new = uniq_vals_new.to_frame()
                                replace_split = pd.Series(replace_split)
                                #replace_split = replace_split.to_frame()
                                dct["userTerm"].append(uniq_vals_new)
                                dct["replacedWith"].append(replace_split)
                                #df_download= dct
                                #csv = df_download.to_csv(index=False)
                                #b64 = base64.b64encode(csv.encode()).decode()  # some strings
        if verLoc:
            df = remove_rcna(df)
            locality_cols= []
            if 'verbatimLocality' in df.columns:
                number = 0
            else:
                df = df.assign(verbatimLocality = "")

            cols = st.text_input("Input the columns you would like to add to verbaitmLocality seperated by commas")
            if cols:
                locality_cols = cols.split (",")

            df["verbatimLocality"] = df[locality_cols].astype(str).apply(", ".join, axis=1)

        if sex_var:
            df = remove_rcna(df)
            df = sex(df)
        if uc:
            df = remove_rcna(df)
            st.write("What units are your measurements in?")
            weight_pages = ["Grams", "Pounds", "Kilograms", "Milligrams"]
            length_pages = ["Millimeters","Inches", "Meters", "Centimeters"]
            weight = st.radio("Weight Measurements", weight_pages)
            length = st.radio("Length Measurements", length_pages)

            if weight == "Kilograms":
                df = kgConv(df)
            if weight == "Milligrams":
                df = mgConv(df)
            if weight == "Pounds":
                df = lbsConv(df)

            if length == "Meters":
                df = mConv(df)
            if length == "Centimeters":
                df = cmConv(df)
            if length == "Inches":
                df = inConv(df)
        if yc_var:
            df = remove_rcna(df)
            df = yc(df)
        if colCheck:
            df = remove_rcna(df)
        if counVald:
            df = remove_rcna(df)
        if eveID:
            df = remove_rcna(df)
            df = add_ms_and_evID(df)
        if datamelt:
            df = remove_rcna(df)
            dfCols = df.columns.values.tolist()
            if 'verbatimLocality' in df.columns:
                number = 0
            else:
                df = df.assign(verbatimLocality = "")

            cols = st.text_input("Input the columns in which you have your weight and length measurements stored in:")
            if cols:
                dataCols = cols.split (",")

                VARS = list(set(dfCols) - set(dataCols))

                ID_VARS = np.array(VARS)

                df = pd.melt(df, id_vars = ID_VARS, value_vars = dataCols, var_name = 'measurementType', value_name = 'measurementValue')

        df = df
        st.write("""
            # Data Post-Cleaning
            """)
        st.dataframe(df,width=700, height=208)
        if True:
            st.write("""
            # Feedback
            """)
            if colCheck:
                prnt = colcheck(df)
                st.write(prnt)
            if counVald:
                if "country" in df.columns:
                    prnt = countryValidity(df)
                    st.write(prnt)
                else:
                    prnt = countryValidity(df)
                    st.write(prnt)
            if matSamp:
                if "materialSampleType" in df.columns:
                    count = 1
                    #download = st.button('Generate materialSampleType CSV')
                    #if download:
                        #linko= f'<a href="data:file/csv;base64,{b64}" download="MST_dict.csv">Download MST_dict.csv</a>'
                        #st.markdown(linko, unsafe_allow_html=True)
                else:
                    st.write("The ""materialSampleType"" column was not found in your dataframe, to apply the ""Material Sample Type"" function this column is required.")
            clean_df = st.button("Click here to generate the cleaned version of your dataframe in CSV format")
            if clean_df:
                df_download = df
                csv = df_download.to_csv(index=False)
                b64 = base64.b64encode(csv.encode()).decode()  # some strings
                linko= f'<a href="data:file/csv;base64,{b64}" download="clean-df.csv">Download clean-df.csv</a>'
                st.markdown(linko, unsafe_allow_html=True)

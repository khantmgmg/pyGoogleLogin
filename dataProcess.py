import pandas as pd
import numpy as np
import json
import time
import re

def get_fiscal_quarter(month, year):
  if isinstance(year, str):
    year = year.replace(',', '')
  year = int(year)
  if month in ['October', 'November', 'December']:
      # return 'Q1 FY{}'.format(year+1)
      return f"Q1 FY{year+1}"
  elif month in ['January', 'February', 'March']:
      # return 'Q2 FY{}'.format(year)
      return f"Q2 FY{year}"
  elif month in ['April', 'May', 'June']:
      # return 'Q3 FY{}'.format(year)
      return f"Q3 FY{year}"
  else:
      # return 'Q4 FY{}'.format(year)
      return f"Q4 FY{year}"

class dataProcess:
  # Adding State/Region column to All_provider sheet
  def restructure_all_provider(all_villages_data, all_provider_data):
    allVillages = pd.json_normalize(all_villages_data)
    allProvider = pd.json_normalize(all_provider_data)
    allProvider = pd.merge(allProvider, allVillages[['Township', 'State_Region']], left_on=['Township'], right_on=['Township'], how='left')
    allProvider = allProvider.drop_duplicates()
    return json.loads(allProvider.to_json(orient='records'))

  # Calculate Total IPC data from IPC_additional and Patient record
  def total_ipc(ipc_additional, patient_record):
    prdf = pd.json_normalize(patient_record)
    prdf = prdf.query("(`Health Education` == 'Y' or `Health Education` == 'Yes') and `Township OD` != '' and `Test Result` != ''")
    prdf = pd.DataFrame(pd.json_normalize(prdf.to_dict("records")))
    prHe = prdf.groupby(['Organization', 'State Region', 'Township OD', 'Reporting Month', 'Reporting Year', 'Reported By', 'Type of Provider','Sex', 'Population Type']).agg({'Name':'count'})
    prHe = prHe.pivot_table(index=['Organization', 'State Region', 'Township OD', 'Reporting Month', 'Reporting Year', 'Reported By', 'Type of Provider'],
                             columns=['Sex', 'Population Type'],
                             values='Name',
                             aggfunc='sum',
                             fill_value=0)
    prHe.columns = prHe.columns.map('_'.join)
    prHe = prHe.reset_index()
    prHe['Male attendance'] = prHe['M_Residence'] + prHe['M_Migrant']
    prHe['Female attendance'] = prHe['F_Residence'] + prHe['F_Migrant']
    prHe['Total attendance'] = prHe['Male attendance'] + prHe['Female attendance']
    prHe['# of migrants included'] = prHe['M_Migrant'] + prHe['F_Migrant']
    prHeFinal = prHe[['Organization', 'State Region', 'Township OD', 'Reporting Month', 'Reporting Year', 'Reported By', 'Type of Provider', 'Male attendance', 'Female attendance', 'Total attendance', '# of migrants included']]
    prHeFinal = pd.json_normalize(prHeFinal.to_dict(orient='records'))
    prHeFinal['Type of Provider'] = prHeFinal['Type of Provider'].replace({
        'ICMV-V': 'Village ICMV',
        'ICMV-W': 'Worksite ICMV',
        'Township team': 'PMI-EM Township level team'
    })
    prHeColumns = {
        'State Region' : 'State/Region',
        'Township OD' : 'Township',
        'Reporting Month' : 'Reporting month',
        'Reporting Year' : 'Reporting year',
        'Reported By' : 'Person code',
        'Type of Provider' : 'Type of provider',
    }
    prHeFinal = prHeFinal.rename(columns=prHeColumns)
    prHeFinal = prHeFinal.fillna('')
    prHeFinal[['Male attendance', 'Female attendance', 'Total attendance', '# of migrants included']] = prHeFinal[['Male attendance', 'Female attendance', 'Total attendance', '# of migrants included']].astype(str)
    prHeFinal = json.loads(prHeFinal.to_json(orient='records'))
    finalIPC = ipc_additional + prHeFinal
    finalIPC = pd.json_normalize(finalIPC)
    finalIPC = finalIPC.loc[finalIPC['Total attendance'].str.strip() != '']
    # drop rows with zero data value in Total attendance column
    finalIPC = finalIPC.loc[finalIPC['Total attendance'].str.strip() != '0']
    # Converting dataframe to json format with columns as key and records as value
    finalIPC = json.loads(finalIPC.to_json(orient='records'))
    return finalIPC

  def prepare_mss(meeting_supervision_stockout):
    mss = pd.json_normalize(meeting_supervision_stockout)
    mss["Overall RDT stockout"] = np.where(
        ((mss["MV_RDT stock out"] == "Y") | (mss["OSDC_RDT stock out"] == "Y") | (mss["Meeting_RDT stock out"] == "Y") | (mss["OV_RDT stock out"] == "Y")), "Y",
        np.where(((mss["MV_RDT stock out"] != "") | (mss["OSDC_RDT stock out"] != "") | (mss["Meeting_RDT stock out"] != "") | (mss["OV_RDT stock out"] != "")), "N", ""))

    mss["Overall ACT stockout"] = np.where(
        ((mss["MV_ACT stock out"] == "Y") | (mss["OSDC_ACT stock out"] == "Y") | (mss["Meeting_ACT stock out"] == "Y") | (mss["OV_ACT stock out"] == "Y")), "Y",
        np.where(((mss["MV_ACT stock out"] != "") | (mss["OSDC_ACT stock out"] != "") | (mss["Meeting_ACT stock out"] != "") | (mss["OV_ACT stock out"] != "")), "N", ""))

    mss["Overall CQ stockout"] = np.where(
        ((mss["MV_CQ stock out"] == "Y") | (mss["OSDC_CQ stock out"] == "Y") | (mss["Meeting_CQ stock out"] == "Y") | (mss["OV_CQ stock out"] == "Y")), "Y",
        np.where(((mss["MV_CQ stock out"] != "") | (mss["OSDC_CQ stock out"] != "") | (mss["Meeting_CQ stock out"] != "") | (mss["OV_CQ stock out"] != "")), "N", ""))

    mss["Overall PQ stockout"] = np.where(
        ((mss["MV_PQ stock out"] == "Y") | (mss["OSDC_PQ stock out"] == "Y") | (mss["Meeting_PQ stock out"] == "Y") | (mss["OV_PQ stock out"] == "Y")), "Y",
        np.where(((mss["MV_PQ stock out"] != "") | (mss["OSDC_PQ stock out"] != "") | (mss["Meeting_PQ stock out"] != "") | (mss["OV_PQ stock out"] != "")), "N", ""))

    mss["Overall Stockout reported"] = np.where(
        (mss["Overall RDT stockout"] != "") | (mss["Overall ACT stockout"] != '') | (mss["Overall CQ stockout"] != '') | (mss["Overall PQ stockout"] != ''), "Y", "N")

    mss["All visit RDT stockout"] = np.where(
        ((mss["MV_RDT stock out"] == "Y") | (mss["OSDC_RDT stock out"] == "Y")), "Y",
        np.where(((mss["MV_RDT stock out"] != "") | (mss["OSDC_RDT stock out"] != "")), "N", ""))

    mss["All visit ACT stockout"] = np.where(
        ((mss["MV_ACT stock out"] == "Y") | (mss["OSDC_ACT stock out"] == "Y")), "Y",
        np.where(((mss["MV_ACT stock out"] != "") | (mss["OSDC_ACT stock out"] != "")), "N", ""))

    mss["All visit CQ stockout"] = np.where(
        ((mss["MV_CQ stock out"] == "Y") | (mss["OSDC_CQ stock out"] == "Y")), "Y",
        np.where(((mss["MV_CQ stock out"] != "") | (mss["OSDC_CQ stock out"] != "")), "N", ""))

    mss["All visit PQ stockout"] = np.where(
        ((mss["MV_PQ stock out"] == "Y") | (mss["OSDC_PQ stock out"] == "Y")), "Y",
        np.where(((mss["MV_PQ stock out"] != "") | (mss["OSDC_PQ stock out"] != "")), "N", ""))

    mss["All visit Stockout reported"] = np.where(
        (mss["All visit RDT stockout"] != "") | (mss["All visit ACT stockout"] != '') | (mss["All visit CQ stockout"] != '') | (mss["All visit PQ stockout"] != ''), "Y", "N")

    final_mss = json.loads(mss.to_json(orient='records'))
    return final_mss

  def prepare_patient_record(patient_record):
    prData = pd.json_normalize(patient_record)
    prData = prData.query("`Township OD` != '' and `Test Result` != '' and `Tested Date` != ''")
    prData = pd.json_normalize(prData.to_dict("records"))
    prData['Age Year'] = pd.to_numeric(prData['Age Year'], downcast="float")
    prData['Pregnancy'] = pd.to_numeric(prData['Pregnancy Month (Lactating mother - (-1))'], downcast="float")
    prData['ACT'] = pd.to_numeric(prData['Number of ACT tab treated (not indicated = 77)'], downcast="float")
    prData['CQ'] = pd.to_numeric(prData['Number of CQ tab treated (not indicated = 77)'], downcast="float")
    prData['PQ7.5'] = pd.to_numeric(prData['Number of PQ7.5mg tab treated (not indicated = 77) (Patient is treated with PQ15mg = 99)'], downcast="float")
    prData['PQ15'] = pd.to_numeric(prData['Number of PQ15mg tab treated (not indicated = 77) (Patient is treated with PQ7.5mg = 99)'], downcast="float")
    prData[['ACT', 'CQ' , 'PQ7.5', 'PQ15']] = prData[['ACT', 'CQ' , 'PQ7.5', 'PQ15']].replace([77, 99], 0)
    prData['PQ'] = prData['PQ7.5'] + (prData['PQ15']*2)
    prData['Age group'] = np.where(
        (prData['Age Year'] >= 15), '4. >=15 yr',
        np.where((prData['Age Year'] >= 10), '3. 10-14 yr',
        np.where((prData['Age Year'] >= 5), '2. 5-9 yr',
        np.where((prData['Age Year'] >= 1), '1. 1-4 yr','0. <1 yr')))
    )
    prData['Tested Date'] = pd.to_datetime(prData['Tested Date'], format='%d-%b-%Y')
    prData['Tested month'] = prData['Tested Date'] + pd.offsets.MonthEnd(0)
    prData['Tested Date'] = prData['Tested Date'].dt.strftime('%d-%b-%Y')
    prData['Tested month'] = prData['Tested month'].dt.strftime('%b-%Y')
    prData['Clinical audit'] = np.where((prData['Test Result'] == 'Negative'), 'NEG',
        np.where(((prData['Referred'] == 'Y') & (prData['Diagnosis'] == 'Severe')),'Referred (Severe)',
        np.where(((prData['Referred'] == 'Y') & (prData['Age group'] == '0. <1 yr')),'Referred (u1)',
        np.where(((prData['Referred'] == 'Y') & ((prData['Pregnancy'] == -1) | (prData['Pregnancy'] > 0))),'Referred (Preg/Lactating)',
        np.where(((prData['Referred'] == 'Y')),'Referred (Other)',
        np.where((((prData['Pregnancy'] > 0) | (prData['Pregnancy'] == -1)) & (prData['Test Result'] == 'Pf') & (prData['Age group'] == '0. <1 yr') & (prData['ACT'] == 3) & (prData['CQ'] == 0) & (prData['PQ'] == 0)),"NTG",
        np.where((((prData['Pregnancy'] > 0) | (prData['Pregnancy'] == -1)) & (prData['Test Result'] == 'Pf') & (prData['Age group'] == '1. 1-4 yr') & (prData['ACT'] == 6) & (prData['CQ'] == 0) & (prData['PQ'] == 0)),"NTG",
        np.where((((prData['Pregnancy'] > 0) | (prData['Pregnancy'] == -1)) & (prData['Test Result'] == 'Pf') & (prData['Age group'] == '2. 5-9 yr') & (prData['ACT'] == 12) & (prData['CQ'] == 0) & (prData['PQ'] == 0)),"NTG",
        np.where((((prData['Pregnancy'] > 0) | (prData['Pregnancy'] == -1)) & (prData['Test Result'] == 'Pf') & (prData['Age group'] == '3. 10-14 yr') & (prData['ACT'] == 18) & (prData['CQ'] == 0) & (prData['PQ'] == 0)),"NTG",
        np.where((((prData['Pregnancy'] > 0) | (prData['Pregnancy'] == -1)) & (prData['Test Result'] == 'Pf') & (prData['Age group'] == '4. >=15 yr') & (prData['ACT'] == 24) & (prData['CQ'] == 0) & (prData['PQ'] == 0)),"NTG",
        np.where((((prData['Pregnancy'] > 0) | (prData['Pregnancy'] == -1)) & (prData['Test Result'] == 'Pv') & (prData['Age group'] == '0. <1 yr') & (prData['ACT'] == 0) & (prData['CQ'] == 1) & (prData['PQ'] == 0)),"NTG",
        np.where((((prData['Pregnancy'] > 0) | (prData['Pregnancy'] == -1)) & (prData['Test Result'] == 'Pv') & (prData['Age group'] == '1. 1-4 yr') & (prData['ACT'] == 0) & (prData['CQ'] == 4) & (prData['PQ'] == 0)),"NTG",
        np.where((((prData['Pregnancy'] > 0) | (prData['Pregnancy'] == -1)) & (prData['Test Result'] == 'Pv') & (prData['Age group'] == '2. 5-9 yr') & (prData['ACT'] == 0) & (prData['CQ'] == 5) & (prData['PQ'] == 0)),"NTG",
        np.where((((prData['Pregnancy'] > 0) | (prData['Pregnancy'] == -1)) & (prData['Test Result'] == 'Pv') & (prData['Age group'] == '3. 10-14 yr') & (prData['ACT'] == 0) & (prData['CQ'] == 7.5) & (prData['PQ'] == 0)),"NTG",
        np.where((((prData['Pregnancy'] > 0) | (prData['Pregnancy'] == -1)) & (prData['Test Result'] == 'Pv') & (prData['Age group'] == '4. >=15 yr') & (prData['ACT'] == 0) & (prData['CQ'] == 10) & (prData['PQ'] == 0)),"NTG",
        np.where((((prData['Pregnancy'] > 0) | (prData['Pregnancy'] == -1)) & (prData['Test Result'] == 'Mixed') & (prData['Age group'] == '0. <1 yr') & (prData['ACT'] == 3) & (prData['CQ'] == 0) & (prData['PQ'] == 0)),"NTG",
        np.where((((prData['Pregnancy'] > 0) | (prData['Pregnancy'] == -1)) & (prData['Test Result'] == 'Mixed') & (prData['Age group'] == '1. 1-4 yr') & (prData['ACT'] == 6) & (prData['CQ'] == 0) & (prData['PQ'] == 0)),"NTG",
        np.where((((prData['Pregnancy'] > 0) | (prData['Pregnancy'] == -1)) & (prData['Test Result'] == 'Mixed') & (prData['Age group'] == '2. 5-9 yr') & (prData['ACT'] == 12) & (prData['CQ'] == 0) & (prData['PQ'] == 0)),"NTG",
        np.where((((prData['Pregnancy'] > 0) | (prData['Pregnancy'] == -1)) & (prData['Test Result'] == 'Mixed') & (prData['Age group'] == '3. 10-14 yr') & (prData['ACT'] == 18) & (prData['CQ'] == 0) & (prData['PQ'] == 0)),"NTG",
        np.where((((prData['Pregnancy'] > 0) | (prData['Pregnancy'] == -1)) & (prData['Test Result'] == 'Mixed') & (prData['Age group'] == '4. >=15 yr') & (prData['ACT'] == 24) & (prData['CQ'] == 0) & (prData['PQ'] == 0)),"NTG",
        np.where((((prData['Pregnancy'] == 0) | (prData['Pregnancy'] == "") | (prData['Pregnancy'].isnull())) & (prData['Test Result'] == 'Pf') & (prData['Age group'] == '0. <1 yr') & (prData['ACT'] == 3) & (prData['CQ'] == 0) & (prData['PQ'] == 0)),"NTG",
        np.where((((prData['Pregnancy'] == 0) | (prData['Pregnancy'] == "") | (prData['Pregnancy'].isnull())) & (prData['Test Result'] == 'Pf') & (prData['Age group'] == '1. 1-4 yr') & (prData['ACT'] == 6) & (prData['CQ'] == 0) & (prData['PQ'] == 1)),"NTG",
        np.where((((prData['Pregnancy'] == 0) | (prData['Pregnancy'] == "") | (prData['Pregnancy'].isnull())) & (prData['Test Result'] == 'Pf') & (prData['Age group'] == '2. 5-9 yr') & (prData['ACT'] == 12) & (prData['CQ'] == 0) & (prData['PQ'] == 2)),"NTG",
        np.where((((prData['Pregnancy'] == 0) | (prData['Pregnancy'] == "") | (prData['Pregnancy'].isnull())) & (prData['Test Result'] == 'Pf') & (prData['Age group'] == '3. 10-14 yr') & (prData['ACT'] == 18) & (prData['CQ'] == 0) & (prData['PQ'] == 4)),"NTG",
        np.where((((prData['Pregnancy'] == 0) | (prData['Pregnancy'] == "") | (prData['Pregnancy'].isnull())) & (prData['Test Result'] == 'Pf') & (prData['Age group'] == '4. >=15 yr') & (prData['ACT'] == 24) & (prData['CQ'] == 0) & (prData['PQ'] == 6)),"NTG",
        np.where((((prData['Pregnancy'] == 0) | (prData['Pregnancy'] == "") | (prData['Pregnancy'].isnull())) & (prData['Test Result'] == 'Pv') & (prData['Age group'] == '0. <1 yr') & (prData['ACT'] == 0) & (prData['CQ'] == 1) & (prData['PQ'] == 0)),"NTG",
        np.where((((prData['Pregnancy'] == 0) | (prData['Pregnancy'] == "") | (prData['Pregnancy'].isnull())) & (prData['Test Result'] == 'Pv') & (prData['Age group'] == '1. 1-4 yr') & (prData['ACT'] == 0) & (prData['CQ'] == 4) & (prData['PQ'] == 7)),"NTG",
        np.where((((prData['Pregnancy'] == 0) | (prData['Pregnancy'] == "") | (prData['Pregnancy'].isnull())) & (prData['Test Result'] == 'Pv') & (prData['Age group'] == '2. 5-9 yr') & (prData['ACT'] == 0) & (prData['CQ'] == 5) & (prData['PQ'] == 14)),"NTG",
        np.where((((prData['Pregnancy'] == 0) | (prData['Pregnancy'] == "") | (prData['Pregnancy'].isnull())) & (prData['Test Result'] == 'Pv') & (prData['Age group'] == '3. 10-14 yr') & (prData['ACT'] == 0) & (prData['CQ'] == 7.5) & (prData['PQ'] == 21)),"NTG",
        np.where((((prData['Pregnancy'] == 0) | (prData['Pregnancy'] == "") | (prData['Pregnancy'].isnull())) & (prData['Test Result'] == 'Pv') & (prData['Age group'] == '4. >=15 yr') & (prData['ACT'] == 0) & (prData['CQ'] == 10) & (prData['PQ'] == 28)),"NTG",
        np.where((((prData['Pregnancy'] == 0) | (prData['Pregnancy'] == "") | (prData['Pregnancy'].isnull())) & (prData['Test Result'] == 'Mixed') & (prData['Age group'] == '0. <1 yr') & (prData['ACT'] == 3) & (prData['CQ'] == 0) & (prData['PQ'] == 0)),"NTG",
        np.where((((prData['Pregnancy'] == 0) | (prData['Pregnancy'] == "") | (prData['Pregnancy'].isnull())) & (prData['Test Result'] == 'Mixed') & (prData['Age group'] == '1. 1-4 yr') & (prData['ACT'] == 6) & (prData['CQ'] == 0) & (prData['PQ'] == 7)),"NTG",
        np.where((((prData['Pregnancy'] == 0) | (prData['Pregnancy'] == "") | (prData['Pregnancy'].isnull())) & (prData['Test Result'] == 'Mixed') & (prData['Age group'] == '2. 5-9 yr') & (prData['ACT'] == 12) & (prData['CQ'] == 0) & (prData['PQ'] == 14)),"NTG",
        np.where((((prData['Pregnancy'] == 0) | (prData['Pregnancy'] == "") | (prData['Pregnancy'].isnull())) & (prData['Test Result'] == 'Mixed') & (prData['Age group'] == '3. 10-14 yr') & (prData['ACT'] == 18) & (prData['CQ'] == 0) & (prData['PQ'] == 21)),"NTG",
        np.where((((prData['Pregnancy'] == 0) | (prData['Pregnancy'] == "") | (prData['Pregnancy'].isnull())) & (prData['Test Result'] == 'Mixed') & (prData['Age group'] == '4. >=15 yr') & (prData['ACT'] == 24) & (prData['CQ'] == 0) & (prData['PQ'] == 28)),"NTG", "NNTG"
        )))))))))))))))))))))))))))))))))))
    final_pr = json.loads(prData.to_json(orient='records'))
    return final_pr

  def casemx_by_rpMth_PvNpv(patient_record):
    prData = pd.json_normalize(patient_record)
    caseMxByRpMthpvnpv = prData.groupby(['Organization', 'State Region', 'Township OD', 'Type of Provider',\
                                'Activity', 'Response to Case ID',\
                                'RHC in carbonless heading', 'Subcenter in carbonless heading',\
                                'Address in carbonless heading',\
                                'Reporting Month', 'Reporting Year','Village Categorization','Test Result','DOT status', 'DOT category', 'Clinical audit'\
                                ]).agg({'Name':'count'})
    caseMxByRpMthpvnpv = caseMxByRpMthpvnpv.reset_index()
    caseMxByRpMthpvnpv = caseMxByRpMthpvnpv.rename(columns={'Name':'Number of patients'})
    caseMxByRpMthpvnpv = caseMxByRpMthpvnpv.fillna('')
    caseMxByRpMthpvnpv = json.loads(caseMxByRpMthpvnpv.to_json(orient='records'))
    return caseMxByRpMthpvnpv

  def rpp_calc(patient_record):
    rpPerform = pd.json_normalize(patient_record)
    rpPerform = rpPerform.query("`Organization` != 'NMCP' and `Township OD` != '' and `Test Result` != '' and \
                                  (`Type of Provider` == 'ICMV-V' or \
                                  `Type of Provider` == 'ICMV-W' or \
                                  `Type of Provider` == 'MMW' or \
                                  `Type of Provider` == 'GP')")
    rpPerform = pd.json_normalize(rpPerform.to_dict("records"))
    # print(rpPerform.dtypes)
    rpPerform['Year in Carbonless'] = rpPerform['Year in Carbonless'].str.replace(',','')
    rpPerform['Reporting Year'] = rpPerform['Reporting Year'].replace({',',''})
    rpPerform = rpPerform.groupby(['Reported By', 'Month in Carbonless','Year in Carbonless', 'Reporting Month', 'Reporting Year']).agg({'Name':'count'})
    rpPerform = rpPerform.reset_index()
    rpPerform = rpPerform.rename(columns={'Name':'count Name'})
    rpPerformValues = json.loads(rpPerform.to_json(orient='records'))
    return rpPerformValues
  
  def responded_cases(patient_record):
    df = pd.json_normalize(patient_record)
    responseCaseIdDf = df[['Organization', 'State Region', 'Township OD', 'Reporting Month', 'Reporting Year', 'Activity', \
                           'Response to Case ID','Type of Provider']]
    responseCaseIdDf = responseCaseIdDf.query("`Response to Case ID` != ''")
    responseCaseIdDf = responseCaseIdDf.drop_duplicates()
    responseCaseIdDf['# of index cases responded'] = [len([x for x in re.split(',|/', i) if x]) for i in responseCaseIdDf['Response to Case ID']]
    responseCaseIdDf['Quarter of fiscal year'] = df.apply(lambda x: get_fiscal_quarter(x['Reporting Month'], x['Reporting Year']), axis=1)
    responseCaseIdDf = responseCaseIdDf.reindex(columns=['Organization', 'State Region', 'Township OD', 'Reporting Month', \
                                                         'Reporting Year', 'Activity', 'Response to Case ID',\
                                                         '# of index cases responded','Quarter of fiscal year','Type of Provider'])
    responseCaseId = json.loads(responseCaseIdDf.to_json(orient='records'))
    return responseCaseId
  
  def casemx_by_rpMth(patient_record):
    df = pd.json_normalize(patient_record)
    caseMxByRpMth = df.groupby(['Organization', 'State Region', 'Township OD', 'Type of Provider',\
                                'Activity', 'Response to Case ID',\
                                'RHC in carbonless heading', 'Subcenter in carbonless heading',\
                                'Address in carbonless heading',\
                                'Reporting Month', 'Reporting Year','Test Result',\
                                ]).agg({'Name':'count'})
    caseMxByRpMth = caseMxByRpMth.reset_index()
    caseMxByRpMth = caseMxByRpMth.rename(columns={'Name':'Number of patients'})
    caseMxByRpMth = json.loads(caseMxByRpMth.to_json(orient='records'))
    return caseMxByRpMth
  
  def positive_only(patient_record):
    df = pd.json_normalize(patient_record)
    positive = ['PF','PV','MIXED','Pf','Pv','Mixed','pf','pv','mixed']
    posOnly = df.loc[df['Test Result'].isin(positive)]
    posOnly = pd.json_normalize(posOnly.to_dict("records"))
    posOnly['Age Year'] = pd.to_numeric(posOnly['Age Year'], downcast="float")
    posOnly['Pregnancy'] = pd.to_numeric(posOnly['Pregnancy Month (Lactating mother - (-1))'], downcast="float")
    posOnly['Test Result'] = posOnly['Test Result'].str.upper()
    posOnly['ACT'] = pd.to_numeric(posOnly['Number of ACT tab treated (not indicated = 77)'], downcast="float")
    posOnly['CQ'] = pd.to_numeric(posOnly['Number of CQ tab treated (not indicated = 77)'], downcast="float")
    posOnly['PQ7.5'] = pd.to_numeric(posOnly['Number of PQ7.5mg tab treated (not indicated = 77) (Patient is treated with PQ15mg = 99)'], downcast="float")
    posOnly['PQ15'] = pd.to_numeric(posOnly['Number of PQ15mg tab treated (not indicated = 77) (Patient is treated with PQ7.5mg = 99)'], downcast="float")
    posOnly[['ACT', 'CQ' , 'PQ7.5', 'PQ15']] = posOnly[['ACT', 'CQ' , 'PQ7.5', 'PQ15']].replace([77, 99], 0)
    posOnly['PQ'] = posOnly['PQ7.5'] + (posOnly['PQ15']*2)
    posOnly['Clinical audit'] = np.where(
        ((posOnly['Referred'] == 'Y') & (posOnly['Diagnosis'] == 'Severe')),'Referred (Severe)',
        np.where(((posOnly['Referred'] == 'Y') & (posOnly['Age group'] == '0. <1 yr')),'Referred (u1)',
        np.where(((posOnly['Referred'] == 'Y') & ((posOnly['Pregnancy'] == -1) | (posOnly['Pregnancy'] > 0))),'Referred (Preg/Lactating)',
        np.where(((posOnly['Referred'] == 'Y')),'Referred (Other)',
        np.where((((posOnly['Pregnancy'] > 0) | (posOnly['Pregnancy'] == -1)) & (posOnly['Test Result'] == 'PF') & (posOnly['Age group'] == '0. <1 yr') & (posOnly['ACT'] == 3) & (posOnly['CQ'] == 0) & (posOnly['PQ'] == 0)),"NTG",
        np.where((((posOnly['Pregnancy'] > 0) | (posOnly['Pregnancy'] == -1)) & (posOnly['Test Result'] == 'PF') & (posOnly['Age group'] == '1. 1-4 yr') & (posOnly['ACT'] == 6) & (posOnly['CQ'] == 0) & (posOnly['PQ'] == 0)),"NTG",
        np.where((((posOnly['Pregnancy'] > 0) | (posOnly['Pregnancy'] == -1)) & (posOnly['Test Result'] == 'PF') & (posOnly['Age group'] == '2. 5-9 yr') & (posOnly['ACT'] == 12) & (posOnly['CQ'] == 0) & (posOnly['PQ'] == 0)),"NTG",
        np.where((((posOnly['Pregnancy'] > 0) | (posOnly['Pregnancy'] == -1)) & (posOnly['Test Result'] == 'PF') & (posOnly['Age group'] == '3. 10-14 yr') & (posOnly['ACT'] == 18) & (posOnly['CQ'] == 0) & (posOnly['PQ'] == 0)),"NTG",
        np.where((((posOnly['Pregnancy'] > 0) | (posOnly['Pregnancy'] == -1)) & (posOnly['Test Result'] == 'PF') & (posOnly['Age group'] == '4. >=15 yr') & (posOnly['ACT'] == 24) & (posOnly['CQ'] == 0) & (posOnly['PQ'] == 0)),"NTG",
        np.where((((posOnly['Pregnancy'] > 0) | (posOnly['Pregnancy'] == -1)) & (posOnly['Test Result'] == 'PV') & (posOnly['Age group'] == '0. <1 yr') & (posOnly['ACT'] == 0) & (posOnly['CQ'] == 1) & (posOnly['PQ'] == 0)),"NTG",
        np.where((((posOnly['Pregnancy'] > 0) | (posOnly['Pregnancy'] == -1)) & (posOnly['Test Result'] == 'PV') & (posOnly['Age group'] == '1. 1-4 yr') & (posOnly['ACT'] == 0) & (posOnly['CQ'] == 4) & (posOnly['PQ'] == 0)),"NTG",
        np.where((((posOnly['Pregnancy'] > 0) | (posOnly['Pregnancy'] == -1)) & (posOnly['Test Result'] == 'PV') & (posOnly['Age group'] == '2. 5-9 yr') & (posOnly['ACT'] == 0) & (posOnly['CQ'] == 5) & (posOnly['PQ'] == 0)),"NTG",
        np.where((((posOnly['Pregnancy'] > 0) | (posOnly['Pregnancy'] == -1)) & (posOnly['Test Result'] == 'PV') & (posOnly['Age group'] == '3. 10-14 yr') & (posOnly['ACT'] == 0) & (posOnly['CQ'] == 7.5) & (posOnly['PQ'] == 0)),"NTG",
        np.where((((posOnly['Pregnancy'] > 0) | (posOnly['Pregnancy'] == -1)) & (posOnly['Test Result'] == 'PV') & (posOnly['Age group'] == '4. >=15 yr') & (posOnly['ACT'] == 0) & (posOnly['CQ'] == 10) & (posOnly['PQ'] == 0)),"NTG",
        np.where((((posOnly['Pregnancy'] > 0) | (posOnly['Pregnancy'] == -1)) & (posOnly['Test Result'] == 'MIXED') & (posOnly['Age group'] == '0. <1 yr') & (posOnly['ACT'] == 3) & (posOnly['CQ'] == 0) & (posOnly['PQ'] == 0)),"NTG",
        np.where((((posOnly['Pregnancy'] > 0) | (posOnly['Pregnancy'] == -1)) & (posOnly['Test Result'] == 'MIXED') & (posOnly['Age group'] == '1. 1-4 yr') & (posOnly['ACT'] == 6) & (posOnly['CQ'] == 0) & (posOnly['PQ'] == 0)),"NTG",
        np.where((((posOnly['Pregnancy'] > 0) | (posOnly['Pregnancy'] == -1)) & (posOnly['Test Result'] == 'MIXED') & (posOnly['Age group'] == '2. 5-9 yr') & (posOnly['ACT'] == 12) & (posOnly['CQ'] == 0) & (posOnly['PQ'] == 0)),"NTG",
        np.where((((posOnly['Pregnancy'] > 0) | (posOnly['Pregnancy'] == -1)) & (posOnly['Test Result'] == 'MIXED') & (posOnly['Age group'] == '3. 10-14 yr') & (posOnly['ACT'] == 18) & (posOnly['CQ'] == 0) & (posOnly['PQ'] == 0)),"NTG",
        np.where((((posOnly['Pregnancy'] > 0) | (posOnly['Pregnancy'] == -1)) & (posOnly['Test Result'] == 'MIXED') & (posOnly['Age group'] == '4. >=15 yr') & (posOnly['ACT'] == 24) & (posOnly['CQ'] == 0) & (posOnly['PQ'] == 0)),"NTG",
        np.where((((posOnly['Pregnancy'] == 0) | (posOnly['Pregnancy'] == "") | (posOnly['Pregnancy'].isnull())) & (posOnly['Test Result'] == 'PF') & (posOnly['Age group'] == '0. <1 yr') & (posOnly['ACT'] == 3) & (posOnly['CQ'] == 0) & (posOnly['PQ'] == 0)),"NTG",
        np.where((((posOnly['Pregnancy'] == 0) | (posOnly['Pregnancy'] == "") | (posOnly['Pregnancy'].isnull())) & (posOnly['Test Result'] == 'PF') & (posOnly['Age group'] == '1. 1-4 yr') & (posOnly['ACT'] == 6) & (posOnly['CQ'] == 0) & (posOnly['PQ'] == 1)),"NTG",
        np.where((((posOnly['Pregnancy'] == 0) | (posOnly['Pregnancy'] == "") | (posOnly['Pregnancy'].isnull())) & (posOnly['Test Result'] == 'PF') & (posOnly['Age group'] == '2. 5-9 yr') & (posOnly['ACT'] == 12) & (posOnly['CQ'] == 0) & (posOnly['PQ'] == 2)),"NTG",
        np.where((((posOnly['Pregnancy'] == 0) | (posOnly['Pregnancy'] == "") | (posOnly['Pregnancy'].isnull())) & (posOnly['Test Result'] == 'PF') & (posOnly['Age group'] == '3. 10-14 yr') & (posOnly['ACT'] == 18) & (posOnly['CQ'] == 0) & (posOnly['PQ'] == 4)),"NTG",
        np.where((((posOnly['Pregnancy'] == 0) | (posOnly['Pregnancy'] == "") | (posOnly['Pregnancy'].isnull())) & (posOnly['Test Result'] == 'PF') & (posOnly['Age group'] == '4. >=15 yr') & (posOnly['ACT'] == 24) & (posOnly['CQ'] == 0) & (posOnly['PQ'] == 6)),"NTG",
        np.where((((posOnly['Pregnancy'] == 0) | (posOnly['Pregnancy'] == "") | (posOnly['Pregnancy'].isnull())) & (posOnly['Test Result'] == 'PV') & (posOnly['Age group'] == '0. <1 yr') & (posOnly['ACT'] == 0) & (posOnly['CQ'] == 1) & (posOnly['PQ'] == 0)),"NTG",
        np.where((((posOnly['Pregnancy'] == 0) | (posOnly['Pregnancy'] == "") | (posOnly['Pregnancy'].isnull())) & (posOnly['Test Result'] == 'PV') & (posOnly['Age group'] == '1. 1-4 yr') & (posOnly['ACT'] == 0) & (posOnly['CQ'] == 4) & (posOnly['PQ'] == 7)),"NTG",
        np.where((((posOnly['Pregnancy'] == 0) | (posOnly['Pregnancy'] == "") | (posOnly['Pregnancy'].isnull())) & (posOnly['Test Result'] == 'PV') & (posOnly['Age group'] == '2. 5-9 yr') & (posOnly['ACT'] == 0) & (posOnly['CQ'] == 5) & (posOnly['PQ'] == 14)),"NTG",
        np.where((((posOnly['Pregnancy'] == 0) | (posOnly['Pregnancy'] == "") | (posOnly['Pregnancy'].isnull())) & (posOnly['Test Result'] == 'PV') & (posOnly['Age group'] == '3. 10-14 yr') & (posOnly['ACT'] == 0) & (posOnly['CQ'] == 7.5) & (posOnly['PQ'] == 21)),"NTG",
        np.where((((posOnly['Pregnancy'] == 0) | (posOnly['Pregnancy'] == "") | (posOnly['Pregnancy'].isnull())) & (posOnly['Test Result'] == 'PV') & (posOnly['Age group'] == '4. >=15 yr') & (posOnly['ACT'] == 0) & (posOnly['CQ'] == 10) & (posOnly['PQ'] == 28)),"NTG",
        np.where((((posOnly['Pregnancy'] == 0) | (posOnly['Pregnancy'] == "") | (posOnly['Pregnancy'].isnull())) & (posOnly['Test Result'] == 'MIXED') & (posOnly['Age group'] == '0. <1 yr') & (posOnly['ACT'] == 3) & (posOnly['CQ'] == 0) & (posOnly['PQ'] == 0)),"NTG",
        np.where((((posOnly['Pregnancy'] == 0) | (posOnly['Pregnancy'] == "") | (posOnly['Pregnancy'].isnull())) & (posOnly['Test Result'] == 'MIXED') & (posOnly['Age group'] == '1. 1-4 yr') & (posOnly['ACT'] == 6) & (posOnly['CQ'] == 0) & (posOnly['PQ'] == 7)),"NTG",
        np.where((((posOnly['Pregnancy'] == 0) | (posOnly['Pregnancy'] == "") | (posOnly['Pregnancy'].isnull())) & (posOnly['Test Result'] == 'MIXED') & (posOnly['Age group'] == '2. 5-9 yr') & (posOnly['ACT'] == 12) & (posOnly['CQ'] == 0) & (posOnly['PQ'] == 14)),"NTG",
        np.where((((posOnly['Pregnancy'] == 0) | (posOnly['Pregnancy'] == "") | (posOnly['Pregnancy'].isnull())) & (posOnly['Test Result'] == 'MIXED') & (posOnly['Age group'] == '3. 10-14 yr') & (posOnly['ACT'] == 18) & (posOnly['CQ'] == 0) & (posOnly['PQ'] == 21)),"NTG",
        np.where((((posOnly['Pregnancy'] == 0) | (posOnly['Pregnancy'] == "") | (posOnly['Pregnancy'].isnull())) & (posOnly['Test Result'] == 'MIXED') & (posOnly['Age group'] == '4. >=15 yr') & (posOnly['ACT'] == 24) & (posOnly['CQ'] == 0) & (posOnly['PQ'] == 28)),"NTG", "NNTG"
        ))))))))))))))))))))))))))))))))))
    posOnly = json.loads(posOnly.to_json(orient='records'))
    return posOnly

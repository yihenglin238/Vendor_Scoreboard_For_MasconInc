import pandas as pd


#assumptions: no column name were modified, including spaces
#all python file and excel file should be in the same folder
#file format should be same
#datatype for each column should be same
#column name remain the same
#'ETD CN' modified to 'ETD ' in shipadv to HQ 20231030 file

#create the report table



def report_generator(vendor_name_i, time_start_i, time_end_i, table1_price_i, table2_shpadv_i):
    #user inputs 
    vendor_name = vendor_name_i
    time_start = time_start_i
    time_end = time_end_i
    
    #read all files
    #read NCR file
    table0 = pd.read_excel("-----------NCR & QA Master List-----------.xlsx", header = 0, sheet_name='NCR List')
    #read price history file
    table1 = pd.read_excel(table1_price_i, header = 2, skiprows=0)
    #read shipment advice
    table2 = pd.read_excel(table2_shpadv_i, header = 0, sheet_name='ShptComplete')
    
    
    table0['Vendor'] = table0['Vendor'].str.upper()
    table1['Vendor'] = table1['Vendor'].str.upper()
    table2['Vendor'] = table2['Vendor'].str.upper()
    
    

    
    from datetime import datetime
    #extract information needed from table 2
    table2 = table2[["Vendor", "P/N", "Q\'ty ", "vendor confirm date", "ETD "]]
    table2 = table2.fillna(1)
    
    length = table2['vendor confirm date'].size
    for i in range(length):
        if type(table2['vendor confirm date'][i]) == str:
            table2['vendor confirm date'][i] = 1
        if type(table2['ETD '][i]) == str:
            table2['ETD '][i] = 1
    
    
    
    length = table2['vendor confirm date'].size
    temp2 = table2
    for i in range(length):
        temp2['vendor confirm date'][i] = str(temp2['vendor confirm date'][i])
        temp2['ETD '][i] = str(temp2['ETD '][i])
    table2 = temp2[temp2["ETD "].str.contains("2014|2015|2016|2017|2018|2019|2020|2021|2022|2023|2024|2025|2026|2027|2028|2029|2030|2031|2032|2033|2034|2035|2036|2037|2038|2039|2040")]
    
    table2['vendor confirm date'] = table2['vendor confirm date'].replace('nan', 1)
    table2['vendor confirm date'] = table2['vendor confirm date'].fillna(1)
    table2['ETD '] = table2['ETD '].replace('nan', 1)
    table2['ETD '] = table2['ETD '].fillna(1)
    
    
    table2['vendor confirm date'] = table2['vendor confirm date'].str[:10]
    table2['vendor confirm date'] = table2['vendor confirm date'].str.replace('-','')
    table2['vendor confirm date'] = table2['vendor confirm date'].astype(int)
    table2['ETD '] = table2['ETD '].str[:10]
    table2['ETD '] = table2['ETD '].str.replace('-','')
    table2['ETD '] = table2['ETD '].astype(int)
    
    #length2 = table2['vendor confirm date'].size
    #for i in range(length2):
    #    table2['vendor confirm date'][i] = datetime.strptime(table2['vendor confirm date'][i], '%m/%d/%y %H:%M:%S')
    
    '''table2 = table2[type(table2['vendor confirm date']) == datetime]
    
    length = table2['vendor confirm date'].size
    for i in range(length):
        if type(table2['vendor confirm date'][i]) != datetime.timestamp:
            table2['vendor confirm date'] = pd.to_datetime(table2['vendor confirm date'])
       '''     

    '''
    length = table2['vendor confirm date'].size
    for i in range(length):
        if type(table2['vendor confirm date'][i]) == str:
            continue;
        if type(table2['vendor confirm date'][i]) != datetime.datetime:
            table2['vendor confirm date'][i] = pd.to_datetime(table2['vendor confirm date'])
    for i in range(length):
        if type(table2['ETD '][i]) == str:
            continue;
        if type(table2['ETD '][i]) != datetime.datetime:
            table2['ETD '][i] = pd.to_datetime(table2['ETD '])
    '''
    #get table 2 and table 1 ready for combine and get total lots value
    vendor_qty = table2[["Vendor", "P/N", "Q'ty "]]
    vendor_cost = table1[["Item", "Vendor", "Cost to cost comp"]]
    
    #clean the table
    vendor_cost = vendor_cost.rename(columns={"Item" : "P/N"})
    vendor_cost = vendor_cost.dropna(subset=["Vendor"])
    #vendor_qty["Vendor"] = vendor_qty["Vendor"].str.strip()
    #vendor_qty["P/N"] = vendor_qty["P/N"].str.strip()
    #vendor_cost["Vendor"] = vendor_cost["Vendor"].str.strip()
    vendor_cost['P/N'] = vendor_cost['P/N'].str.strip()
    
    #combine table 1 and table 2
    total_lots_value = vendor_cost[['P/N', 'Cost to cost comp']].merge(vendor_qty,
                                                                       on = ['P/N'],
                                                                       how = "right")
    
    #calculate the value and put it in a new column
    value = []
    p = 0
    err_mess = 'price info missing'
    length = total_lots_value['P/N'].size
    #total_lots_value = total_lots_value.fillna(-1)
    for i in range(length):
        if total_lots_value['Cost to cost comp'].empty:
            value.append(err_mess)
        else:
            p = total_lots_value['Cost to cost comp'][i] * total_lots_value['Q\'ty '][i]
            value.append(p)
                
    total_lots_value['total_value'] = value
                
    table2 = table2.reset_index(drop=True)
    #get the date status for each
    
    status  = []
    
    err_mess1 = 'invalid date'
    for i in range(length):
        #check if both date are valid
        if table2['vendor confirm date'][i] != 1 and table2['ETD '][i] != 1:
            if table2['vendor confirm date'][i] <= table2['ETD '][i]:
                status.append('no delay')
            else:
                #total_lots_value['vendor confirm date'][i] > total_lots_value['ETD'][i]:
                    status.append('delay')
        else:
            status.append(err_mess1)

    total_lots_value['status'] = status

    #append these date to locate the time period later
    total_lots_value['Start Date'] = table2['vendor confirm date']
    total_lots_value['Time'] = table2['ETD ']
    '''
    #select 2023 ETD data only
    total_lots_value['Time'] = total_lots_value['Time'].fillna(-1)
    s = total_lots_value['Time'].size
    for i in range(s):
        if type(total_lots_value['Time'][i]) != int:
            total_lots_value['Time'][i] = 1
        else:
            total_lots_value['Time'][i] = int(total_lots_value['Time'][i].strftime('%Y%m%d'))

    temp = total_lots_value
    total_lots_value = temp[temp['Time'] >= 20230000]
    '''

    #make NCR table uniform
    table0['Vendor'] = table0['Vendor'].str.upper()
    table1['Vendor'] = table1['Vendor'].str.upper()
    table2['Vendor'] = table2['Vendor'].str.upper()
    table0 = table0.replace('JY', 'JIAYE')
    
    #get the date of the NCR and put them in a new column
    table0['Ref Document'] = table0['Ref Document'].fillna(-1)
    ncr = table0[['Ref Document', 'Part Number']]
    ncr['Ref Date'] = ncr['Ref Document'].str[:8]
    ncr['Ref Date'] = ncr['Ref Date'].fillna(-1)
    ncr['Ref Date'].astype(int)
    ncr = ncr.rename(columns={"Part Number" : "P/N"})
    
    #merge two file together, all information needed are here
    outputList = total_lots_value.merge(ncr,
                                        on = ['P/N'],
                                        how = "left")
    
    #data type conversion. for calculations later
    outputList['Ref Date'] = outputList['Ref Date'].fillna(-1)
    #s = outputList['Time'].size
    #for i in range(s):
        #  outputList['Time'][i] = int(outputList['Time'][i].strftime('%Y%m%d'))
        
    #get only for the specific vendor
    temp_df = total_lots_value
    temp_df = temp_df[temp_df['Vendor'] == vendor_name]
    
    #then get only for that specific time period
    temp_df = temp_df[time_start <= temp_df['Time']]
    temp_df = temp_df[time_end >= temp_df['Time']]
    
    temp_df = temp_df.reset_index(drop=True)
    
    #do similar things for ncr table
    temp_df_ncr = outputList
    temp_df_ncr['Ref Date'] = temp_df_ncr['Ref Date'].astype(int)
    
    temp_df_ncr = temp_df_ncr[time_start <= temp_df_ncr['Ref Date']]
    temp_df_ncr = temp_df_ncr[time_end >= temp_df_ncr['Ref Date']]
    
    temp_df_ncr = temp_df_ncr.reset_index(drop=True)
    
    
    
    temp_df_ncr_only = ncr
    temp_df_ncr_only['Ref Date'] = temp_df_ncr_only['Ref Date'].astype(int)
    
    temp_df_ncr_only = temp_df_ncr_only[time_start <= temp_df_ncr_only['Ref Date']]
    temp_df_ncr_only = temp_df_ncr_only[time_end >= temp_df_ncr_only['Ref Date']]
    
    temp_df_ncr_only = temp_df_ncr_only.reset_index(drop=True)
    #get all variables for report
    #total lines of temp_df found
    total_shippingRecord = temp_df.shape[0]
    #print('total_shippingRecord = ', total_shippingRecord)
    
    #total parts excluding duplicate
    total_unique_parts = temp_df['P/N'].nunique()
    #print('total_unique_parts = ', total_unique_parts)
    unique_parts = temp_df['P/N'].unique()
    #print('unique_parts = ', unique_parts)
    
    #sum up total value for each vendor
    nan_count = temp_df['total_value'].isna().sum()
    #print('nan_count = ', nan_count)
    temp_df1 = temp_df
    final_total_lots_value = temp_df1.fillna(0)['total_value'].sum()
   # print('final_total_lots_value = ', final_total_lots_value)
    
    #count each in status
    temp_df['status'] = pd.Categorical(temp_df['status'], categories=['invalid date', 'no delay', 'delay'])
    total_invalid = temp_df['status'].value_counts()['invalid date']
    total_recieved = temp_df['status'].value_counts()['no delay']
    total_delayed = temp_df['status'].value_counts()['delay']
    #print('total_invalid = ', total_invalid)
    #print('total_recieved = ', total_recieved)
    #print('total_delayed = ', total_delayed)
    
    #count lines in temp_df_ncr
    total_NCR = temp_df_ncr_only.shape[0]
    #print('total_NCR = ', total_NCR)
    #total_unique_NCR
    total_unique_ncr = temp_df_ncr['P/N'].nunique()
    #print('total_unique_ncr = ', total_unique_ncr)
    unique_ncr = temp_df_ncr['P/N'].unique()
    #print('unique_ncr = ', unique_ncr)
    
    #generate report
    print('Brief Report:')
    print('For vendor', vendor_name, 'from time period', time_start, 'to', time_end, ':')
    print('In total,', total_shippingRecord, 'records were found with', total_unique_parts, 'different P/Ns', ';')
    print('which are:', unique_parts)
    print('Total lots value for this vendor is at least', final_total_lots_value, ';')
    print(nan_count, 'records have missing information in either Cost to cost comp or Quantity', ';')
    print('In', total_shippingRecord, 'records,',
          total_invalid, 'of them have invalid date;',
          total_recieved, 'of them are recieved on time;',
          total_delayed, 'of them are delayed;')
    print('during this time, the vendor recieved', total_NCR, 'NCRs;')
    print('With', total_unique_ncr, 'differnt part numbers;')
    print('which are:', unique_ncr)
    
    #create the report table
    report_df = pd.DataFrame(columns=['Vendor', 'Time(from)', 'Time(to)', 
                                      'No. of records found', 'No. of diff P/N', 'P/N list', 
                                      'Total lots value(at least)', 'No. of missing info', 
                                      'Total invalid date order', 'Total on time order', 'Total delayed order',
                                      'No. of NCRs', 'No. of NCR part', 'NCR part list'])  
    #report_df = pd.DataFrame(columns=[vendor_name, time_start, time_end,total_shippingRecord, 
    #                                  total_unique_parts, unique_parts, final_total_lots_value, 
    #                                  nan_count, total_invalid, total_recieved, total_delayed, 
    #                                  total_NCR, total_unique_ncr, unique_ncr])
    
    #append the result onto the table
    report_df_temp = {'Vendor' : vendor_name, 
                      'Time(from)' : time_start, 
                      'Time(to)' : time_end, 
                      'No. of records found' : total_shippingRecord, 
                      'No. of diff P/N' : total_unique_parts,        
                      'P/N list' : unique_parts, 
                      'Total lots value(at least)' : final_total_lots_value, 
                      'No. of missing info' : nan_count, 
                      'Total invalid date order' : total_invalid, 
                      'Total on time order' : total_recieved, 
                      'Total delayed order' : total_delayed,
                      'No. of NCRs' : total_NCR, 
                      'No. of NCR part' : total_unique_ncr, 
                      'NCR part list' : unique_ncr}
    
    report_df = report_df.append(report_df_temp, ignore_index = True)
    #location = report_df['Vendor'].size
    report_df.to_csv('report.csv', mode='a', index=False, header=False)
    
    #print table for lots information
    print(temp_df)
    #print table for ncr information
    print(temp_df_ncr)
    
    
    
######################################################################################## 
    
table_in = pd.read_csv("input.csv", header = 0)

in_len = table_in['vendor name'].size
report_df_head = pd.DataFrame(columns=['Vendor', 'Time(from)', 'Time(to)', 
                                  'No. of records found', 'No. of diff P/N', 'P/N list', 
                                  'Total lots value(at least)', 'No. of missing info', 
                                  'Total invalid date order', 'Total on time order', 'Total delayed order',
                                  'No. of NCRs', 'No. of NCR part', 'NCR part list']) 
report_df_head.to_csv('report.csv', mode='w', index=False, header=True)
for i in range(in_len):
    report_generator(table_in['vendor name'][i], table_in['time(from)'][i], 
                     table_in['time(to)'][i], table_in['price filename'][i], 
                     table_in['shpadv filename'][i])




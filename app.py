import pandas as pd
import datetime
import sys
import os
import s3fs
import boto3
from io import BytesIO
reference_data = os.getenv("reference_data","testdata/ref.csv")
output_data = os.getenv("output_location","output/")
infra = os.getenv("infra","local")
trade_data = os.getenv("trade_data","testdata/trade-data.csv")
s3_client = boto3.client('s3')
s3 = boto3.resource('s3')
s3_client.download_file('devops-iris-hackthron', 'reference/ref.csv', '/app/ref.csv')
def download_s3():
    my_bucket = s3.Bucket("devops-iris-hackthron-raw")
    for s3_object in my_bucket.objects.all():
        # Need to split s3_object.key into path and file name, else it will give error file not found.
        path, filename = os.path.split(s3_object.key)
        my_bucket.download_file(s3_object.key, f"/app/{filename}")
        my_function(filename)


def my_function(file):
    empty_list = []
    trade_data_ref = trade_data
    # Processing Data file location as input data file
    df = pd.read_csv(file)
    #trade_data_file_name = "C:\\dinesh\\file-test\\trade-data"
    # Response data file location
    response_data_file_name = output_data

    current_time = datetime.datetime.now()
    ymd = str(current_time.year) + str(current_time.month) + str(current_time.day)
    hms = str(current_time.hour) + str(current_time.minute) + \
          str(current_time.second)
    # Response status file location which contains the record is rejected or accepted
    response_status_file_name = f"status_{ymd}_{hms}.csv"

    column_names = ['Record Number', 'Transaction Id', 'Status']
    final = []
    df_res = pd.DataFrame(final, columns=column_names)
    cols = df.columns

    indexValue = find_index(cols, 'Security identifier')
    record_number = 0
    for d in df.values:
        status = ''
        print("---------------------------------------------------------")
        transaction_id = d[find_index(cols, 'Transaction ID')[0]]
        rcc = d[find_index(cols, 'Reporting Counterparty Code')][0]
        nrcc = d[find_index(cols, 'Non-Reporting Counterparty Code')][0]
        si = d[find_index(cols, 'Security identifier')][0]
        llei = d[find_index(cols, 'Loan LEI of the issuer')][0]
        at = d[find_index(cols, 'Action type')][0]
        uti = d[find_index(cols, 'UTI')][0]
        level = d[find_index(cols, 'Level')][0]
        ct = d[find_index(cols, 'Contract Type')][0]
        rcfs = d[find_index(cols, 'Reporting Counterparty Financial Status')][0]
        rcs = d[find_index(cols, 'Reporting Counterparty Sector')][0]
        nrcfs = d[find_index(cols, 'Non-Reporting Counterparty Financial Status')][0]
        nrpcs = d[find_index(cols, 'Non-Reporting Counterparty Sector')][0]
        cts = d[find_index(cols, 'Counterparty Side')][0]
        ed = d[find_index(cols, 'Event date')][0]
        tv = d[find_index(cols, 'Trading venue')][0]
        mat = d[find_index(cols, 'Master agreement type')][0]
        vd = d[find_index(cols, 'Value date')][0]
        gci = d[find_index(cols, 'General collateral Indicator')][0]
        toa = d[find_index(cols, 'Type of asset')][0]
        coas = d[find_index(cols, 'Classification of a security')][0]
        lbp = d[find_index(cols, 'Loan Base product')][0]
        lsp = d[find_index(cols, 'Loan Sub product')][0]
        lfsp = d[find_index(cols, 'Loan Further sub product')][0]
        lmts = d[find_index(cols, 'Loan Maturity of the security')][0]
        ljti = d[find_index(cols, 'Loan Jurisdiction of the issuer')][0]
        print("-----------------------------")
        print(len(str(transaction_id)))
        print(transaction_id)
        if str(transaction_id) == 'nan' or len(str(transaction_id)) > 100 or str(rcc) == 'nan' or len(
                str(rcc)) > 20 or str(rcc).isalnum() is False or str(nrcc) == 'nan' or len(str(nrcc)) > 20 or str(
                nrcc).isalnum() is False or str(si) == 'nan' or len(str(si)) > 12 or str(si).isalnum() is False:

            list = [[record_number+1, transaction_id, 'RJCT']]
            df_res = df_res.append(pd.DataFrame(list,
                                                columns=column_names),
                                   ignore_index=True)
            df_res.to_csv(response_status_file_name, index=False)
            # df = df.drop(df.index[[record_number]])
            # df.drop(df.index[[record_number]])
            empty_list.append(record_number)
            record_number = record_number + 1
            continue

        if len(str(ljti)) > 20 or len(str(coas)) > 100 or len(str(lbp)) > 20 or len(str(lsp)) > 100 or len(str(lfsp)) > 100 or len(str(mat)) > 100 or len(str(gci)) > 100 or len(str(toa)) > 100 or len(str(nrpcs)) > 100 or len(str(llei)) > 20 or len(str(at)) > 100 or len(str(uti)) > 100 or len(str(level)) > 100 or len(str(ct)) > 100 or len(str(rcfs)) > 100 or len(str(rcs)) > 100 or len(str(nrcfs)) > 100 or len(str(cts)) > 100  or len(str(tv)) > 100:

            list = [[record_number+1, transaction_id, 'RJCT']]
            df_res = df_res.append(pd.DataFrame(list,
                                                columns=column_names),
                                   ignore_index=True)
            df_res.to_csv(response_status_file_name, index=False)
            # df = df.drop(df.index[[record_number]])
            # df.drop(df.index[[record_number]])
            empty_list.append(record_number)
            record_number = record_number + 1

            continue


        first_counter = 0
        for d1 in d:

            if str(d1) == '' or str(d1) == 'nan' or str(d1) == 'none':

                col = cols[first_counter]
                replace = get_data(col, d[indexValue])

                if len(replace) > 0:
                    df.loc[record_number, col] = replace[0]
                status = 'ACPT'

            first_counter = first_counter + 1

        record_number = record_number + 1
        list = [[record_number, transaction_id, status]]
        df_res = df_res.append(pd.DataFrame(list,
                                            columns=column_names),
                               ignore_index=True)
        df_res.to_csv(response_status_file_name, index=False)
    # df['Status'] = 'rejected'
    # df["Status"] = df.loc[df['Transaction ID'].notnull() & df['Reporting Counterparty Code'].notnull() & df['Non-Reporting Counterparty Code'].notnull() & df['Security identifier'].notnull() & df['Reporting Counterparty Code'].str.isalnum() & df['Non-Reporting Counterparty Code'].str.isalnum() & df['Security identifier'].str.isalnum(),'Status'] = "Accepted"
    
    df = df.drop(df.index[empty_list])
    df.to_csv("response_" + ymd + "_" + hms + ".csv", index=False)
    print(df.values)
    s3_client.upload_file("response_" + ymd + "_" + hms + ".csv", 'devops-iris-hackthron', "processed/response_" + ymd + "_" + hms + ".csv")
    s3_client.upload_file(response_status_file_name, 'devops-iris-hackthron', f"output/{response_status_file_name}")
    s3_client.delete_object(Bucket='devops-iris-hackthron-raw', Key='trade-data.csv')


def find_index(cols, name):
    return [i for i, x in enumerate(cols) if x == name]


def get_data(missing_col_name, find_key_name):
    reference_data_file = "ref.csv"
    df2 = pd.read_csv(reference_data_file)
    colsd2 = df2.columns
    missing_col_name_index = find_index(colsd2, missing_col_name)

    for fg in df2.values:
        for ee in fg:
            if str(ee) == find_key_name:

                return fg[missing_col_name_index]

if __name__ == '__main__':
    download_s3()

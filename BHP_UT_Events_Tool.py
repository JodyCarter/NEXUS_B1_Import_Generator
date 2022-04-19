import numpy as np
import pandas as pd
from progressbar import progressbar


def asset_type_string(string):
    types_dict = {'Air Coolers': ['HE - Air Coolers', 'Heat Exchanger TML'],
                  'Filters': ['Vessel', 'Vessel TML'],
                  'Heat Exchangers': ['HE - Shell and Tube', 'Heat Exchanger TML'],
                  'Launchers and Receivers': ['Vessel', 'Vessel TML'],
                  'Pressure Vessels': ['Vessel', 'Vessel TML'],
                  'Tanks': ['Tank', 'Tank TML'],
                  'WHRU': ['Vessel', 'Vessel TML']}

    for key in types_dict:
        if key in string:
            if len(string.split(' / ')) < 7:
                return types_dict[key][0]
            else:
                return types_dict[key][1]
    return 'NOT FOUND'


if __name__ == "__main__":

    request_file_paths = False

    file_path = 'C:\\Py\\BHP_Data.xlsx'
    output_path = 'C:\\Py\\BHP_Data_Out.xlsx'
    asset_import_path = 'C:\\Py\\BHP_Assets_to_Import.xlsx'

    if request_file_paths is True:
        file_path = input('\nEnter File Path:')
        output_path = input('\nEnter File Output Path:')
        asset_import_path = input('\nEnter Asset Import File Output Path:')

    df = pd.read_excel(file_path, sheet_name='Equipment Master Import')
    print('\nReading File...')
    print('\nProcessing data...')
    target_columns = ['UT-WT.Reading 1 (N-12)',
                      'UT-WT.Reading 2 (E-3)',
                      'UT-WT.Reading 3 (S-6)',
                      'UT-WT.Reading 4 (W-9)']

    out_df = pd.DataFrame({})
    last_cml_int = 0
    last_cml_suffix = 0.01
    location = ''

    for index, row in progressbar(enumerate(df.iterrows()), max_value=int(df.shape[0])):
        asset_location = row[1]['Asset Location.Full Location (Parent)'].split(' / ')
        cml_location = row[1]['CMLs'].strip(' /').strip().split(' / ')
        dfh = row[1].to_frame().transpose()

        # current_location = str(row[1]['UT-WT.Location'])
        # previous_location = str(df.iloc[index - 1]['UT-WT.Location'])
        if asset_location[-1] == cml_location[-1]:
            i = df.iloc[index-1]['CMLs'].split(' / ')[-1]
            # if current_location != previous_location:
            #     last_cml_int = last_cml_int + 1
            if i.isdigit():
                last_cml_int = int(i)
            else:
                last_cml_int = last_cml_int
        else:
            last_cml_int = int(cml_location[-1])
            last_cml_suffix = 0.01

        ut_values = []
        for col in target_columns:
            if row[1][col] != 0.00:
                ut_values.append(row[1][col])

        for value in ut_values:
            dfh['UT-WT.Reading'] = value
            dfh[target_columns] = np.NaN
            cml_name = last_cml_int + last_cml_suffix
            last_cml_suffix = last_cml_suffix + 0.01
            new_cml_location = asset_location + [str("{:.2f}".format(cml_name))]
            dfh['CMLs'] = ' / '.join(new_cml_location)
            out_df = pd.concat([out_df, dfh])

    out_df.reindex()

    out_df['Event.Event Type'] = 'UT Wall Thickness'
    out_df['UT-WT.Date of Reading_B'] = pd.to_datetime(out_df['UT-WT.Date of Reading'])
    out_df['UT-WT.Date of Reading'] = out_df['UT-WT.Date of Reading_B'].dt.strftime('%m/%d/%Y')
    out_df['Event.Start Clock'] = out_df['UT-WT.Date of Reading_B'].dt.strftime('%m/%d/%Y %r')
    out_df['Event.End Clock'] = out_df['UT-WT.Date of Reading_B'].dt.strftime('%m/%d/%Y %r')

    columns_out = {'CMLs': 'Asset Location.Full Location',
                   'Workpack.Name': 'Workpack.Name',
                   'Event.Event Type': 'Event.Event Type',
                   'Event.Start Clock': 'Event.Start Clock',
                   'Event.End Clock': 'Event.End Clock',
                   'Commentary.Notes': 'Commentary.Notes',
                   'UT-WT.Date of Reading': 'UT-WT.Date of Reading',
                   'UT-WT.Inspection Method': 'UT-WT.Inspection Method',
                   'UT-WT.Location': 'UT-WT.Location',
                   'UT-WT.Minimum Reading': 'UT-WT.Minimum Reading',
                   'UT-WT.Position': 'UT-WT.Position',
                   'UT-WT.Reading': 'UT-WT.Reading',
                   'UT-WT.Reading 1 (N-12)': 'UT-WT.Reading 1 (N-12)',
                   'UT-WT.Reading 1 Extrados?': 'UT-WT.Reading 1 Extrados?',
                   'UT-WT.Reading 2 (E-3)': 'UT-WT.Reading 2 (E-3)',
                   'UT-WT.Reading 2 Extrados?': 'UT-WT.Reading 2 Extrados?',
                   'UT-WT.Reading 3 (S-6)': 'UT-WT.Reading 3 (S-6)',
                   'UT-WT.Reading 3 Extrados?': 'UT-WT.Reading 3 Extrados?',
                   'UT-WT.Reading 4 (W-9)': 'UT-WT.Reading 4 (W-9)',
                   'UT-WT.Reading 4 Extrados?': 'UT-WT.Reading 4 Extrados?',
                   'UT-WT.Report Number': 'UT-WT.Report Number',
                   'UT-WT.Ident': 'UT-WT.Ident',
                   'UT-WT.TML Type': 'UT-WT.TML Type'}

    out_df_2 = pd.DataFrame([])
    for column in columns_out:
        if column in out_df.columns.values:
            out_df_2[columns_out[column]] = out_df[column]

    # write to excel
    print('\nWriting to excel...')
    out_df_2.to_excel(output_path, sheet_name='UT Import', index=False)

    # check if Parent or CML exists in NEXUS_IC
    check_df = pd.read_excel(file_path, sheet_name='Assets')
    check_df['Asset Name'] = check_df['Asset Location.Full Location'].apply(lambda y: y.split(' / ')[-1])

    print('\nChecking assets...')
    asset_import_df = pd.DataFrame([])
    asset_import_df['Parent Asset'] = out_df['Asset Location.Full Location (Parent)']
    asset_import_df['CMLs'] = out_df['CMLs']
    asset_import_df['Parent Exists'] = out_df['Asset Location.Full Location (Parent)'].apply(
        lambda m: m in check_df['Asset Location.Full Location'].values)
    asset_import_df['CML Exists'] = out_df['CMLs'].apply(
        lambda l: l in check_df['Asset Location.Full Location'].values)

    a1 = pd.DataFrame([])
    a1['Asset Location.Full Location'] = asset_import_df[asset_import_df['Parent Exists'] == False]['Parent Asset']
    a2 = pd.DataFrame([])
    a2['Asset Location.Full Location'] = asset_import_df[asset_import_df['CML Exists'] == False]['CMLs']
    asset_to_nexus = pd.concat([a1, a2])

    asset_to_nexus.drop_duplicates(inplace=True, ignore_index=True)

    print('\nWriting asset import...')
    asset_to_nexus['Asset.Asset Type'] = asset_to_nexus['Asset Location.Full Location'].apply(
        lambda x: asset_type_string(x))

    asset_to_nexus.to_excel(asset_import_path, index=False, sheet_name='Assets Import')
    print('\nDone')
    p = input('press Enter to Exit')

import PySimpleGUI as sg
import pandas as pd
from datetime import datetime
import os
import time
import multiprocessing
from multiprocessing import Pool, Queue
from enum import Enum

# 全局状态
class Status(Enum):
    OFF   = 0
    MAIN  = 1
    LOAD  = 2
    TABLE = 3

global_status = Status.MAIN
global_path   = ''
global_data   = pd.DataFrame()

def is_main_process():
    current_process = multiprocessing.current_process()
    return current_process.name == 'MainProcess' and current_process.is_alive()

def is_date(string, format):
    try:
        datetime.strptime(string, format)
        return True
    except ValueError:
        return False

def ProcessRawData(csv_file):
    print('子进程'+str(os.getpid())+'处理：'+csv_file)

    start_time = time.time()
    start_index = csv_file.rfind('O') + 1
    end_index = csv_file.rfind('.csv')
    
    date=csv_file[start_index:end_index]
    
    
    
    # 求最大列数
    max_col = 0
    with open(csv_file, 'r') as file:
        for line in file:
            num_col = len(line.split(',')) + 1
            if max_col < num_col:
                max_col = num_col
                
    # 读取文件
    col_names = [i for i in range(0, max_col)]
    raw_data = pd.read_csv(csv_file, header = None, delimiter = ',', names = col_names,low_memory=False, lineterminator='\n')

    end_time = time.time()
    #print('子进程'+str(os.getpid())+'读取文件耗时：'+str(end_time-start_time))


    start_time = time.time()
    # 数据过滤
    filtered_data = raw_data[raw_data[0] == 'I']
    filtered_data = filtered_data.reset_index(drop = True)
    filtered_data = filtered_data.dropna(axis = 'columns', how = 'all')
    
    header = filtered_data.iloc[0]
    #header = header.dropna()
    filtered_data = filtered_data.iloc[1:]
    filtered_data.columns = header

    end_time = time.time()
    #print('子进程'+str(os.getpid())+'数据过滤耗时：'+str(end_time-start_time))

    
    start_time = time.time()

    time_data = filtered_data['Time']
    info_data = filtered_data.iloc[:,12:]
    info_data = info_data.T

    stacked_data = pd.concat([info_data.iloc[:,i] for i in range(0,info_data.shape[1])],ignore_index=True)
    time_label = time_data.repeat(info_data.shape[0]) 
    time_label = time_label.reset_index(drop=True)

    reass_data = pd.concat([time_label, stacked_data],ignore_index=True,axis=1)
    reass_data.columns = ['Time', 'Info']
    reass_data = reass_data.dropna()
    reass_data = reass_data.reset_index(drop = True)

    end_time = time.time()
    print('子进程'+str(os.getpid())+'数据重组耗时：'+str(end_time-start_time))
    
    #向数据中添加日期
    reass_data['DateTime'] = pd.to_datetime(date+' '+reass_data['Time'])
    reass_data = reass_data[['DateTime','Info']]
    
    return reass_data

def PrintResult(folder_path):
    result=pd.DataFrame()
    file_paths = []
    for root, dirs, files in os.walk(folder_path):
        for dir in dirs:
            if is_date(dir,'%Y%m%d') == False:
                continue
            filename='O'+dir+'.csv'
            filepath=root+'/'+dir+'/'+filename
            filepath = os.path.normpath(filepath)
            file_paths.append(filepath)
    print(file_paths)
    
    if __name__ == '__main__':
        with Pool(processes=4) as pool:
            data = pool.map(ProcessRawData, file_paths)
            
    result = pd.concat(data)
    result = result.dropna()
    result = result.sort_values('DateTime')
    return result

def FilterResult(key):
    global global_data

    data = global_data
    data = data[data['Info'].str.contains(key,case=False)]

    # 删除重复开启或重复关闭的部分
    temp_res = data
    temp_res['PrevInfo'] = temp_res['Info'].shift(1)
    temp_res['NextInfo'] = temp_res['Info'].shift(-1)
    condition = (temp_res['Info'].str.endswith('On') & (temp_res['Info'] == temp_res['PrevInfo'])) | (temp_res['Info'].str.endswith('Off') & (temp_res['Info'] == temp_res['NextInfo']))
    temp_res = temp_res[~condition]

    act_on = temp_res['Info'].str.endswith('On').sum()
    act_off = temp_res['Info'].str.endswith('Off').sum()
    temp_res['Duration'] = temp_res['DateTime'].diff()
    total_duration = temp_res['Duration'].sum()
    temp_res.loc[temp_res['Info'].str.endswith('On') == 1 ,'Duration'] = pd.to_timedelta(0)
 

    result_list = data.values.tolist()

    return result_list, str(act_on), str(act_off), str(total_duration)

def MAIN_WIN():
    global global_status
    global global_path 

    Layout =[
        [sg.FolderBrowse(),sg.In(key='-FOLDER_PATH-')],
        [sg.Button('确定'),sg.Button('取消')]
    ]

    window=sg.Window("日志分析",Layout)

    while True:
        event,values = window.read()
        
        if event == None:
            global_status =Status.OFF
            break
        if event == '确定':
            global_path = values['-FOLDER_PATH-']
            global_status = Status.LOAD
            break
        if event == '取消':
            global_status =Status.OFF
            break

    window.close()

def LOAD_FILE():
    global global_path
    global global_data
    global global_status
    result=pd.DataFrame()
    file_paths = []
    for root, dirs, files in os.walk(global_path):
        for dir in dirs:
            if is_date(dir,'%Y%m%d') == False:
                continue
            filename='O'+dir+'.csv'
            filepath=root+'/'+dir+'/'+filename
            filepath = os.path.normpath(filepath)
            file_paths.append(filepath)
    print(file_paths)
    
    if __name__ == '__main__':
        multiprocessing.freeze_support()
        with Pool() as pool:
            data = pool.map(ProcessRawData, file_paths)
            
    global_data = pd.concat(data)
    global_data = global_data.sort_values('DateTime')

    # 切换到表格状态
    global_status = Status.TABLE

def TABLE_WIN():
    global global_data
    global global_status

    result = global_data
    result_list = result.values.tolist()
        
    list_layout = [[sg.Text('筛选条件：'),sg.InputText(key='-FILTER-'),sg.Button('筛选')],
                   [sg.Text(key='on_count'),sg.Text(key='off_count'),sg.Text(key='duration')],
                   [sg.Table(values=result_list,
                             headings=result.columns.tolist(),
                             key = 'table',
                             enable_events=True,
                             max_col_width=25,
                             auto_size_columns=True,
                             justification='left',
                             num_rows=min(25, len(result_list)))]]
        
    # 创建窗口
    list_window = sg.Window('DataFrame示例', list_layout)

    # 事件循环
    while True:
        event, values = list_window.read()
        if event == sg.WINDOW_CLOSED:
            break
        if event == '筛选':
            '''
            temp_res = result[result['Info'].str.contains(values['-FILTER-'],case=False)]
            act_on = temp_res['Info'].str.endswith('On').sum()
            act_off = temp_res['Info'].str.endswith('Off').sum()
            temp_res['Duration'] = temp_res['DateTime'].diff()
            temp_res.loc[temp_res['Info'].str.endswith('On') == 1 ,'Duration'] = pd.to_timedelta(0)
            result['Duration']=temp_res['Duration']
            result_list = result[result['Info'].str.contains(values['-FILTER-'],case=False)].values.tolist()
            list_window['on_count'].Update('开启次数：'+str(act_on))
            list_window['off_count'].Update('关闭次数：'+str(act_off))
            list_window['duration'].Update('开机总时长：'+str(temp_res['Duration'].sum()))
            list_window['table'].Update(values=result_list)
            '''

            result_list, on_count, off_count, duration = FilterResult(values['-FILTER-'])
            list_window['table'].Update(values=result_list)
            list_window['on_count'].Update('开启次数：'+on_count)
            list_window['off_count'].Update('关闭次数：'+off_count)
            list_window['duration'].Update('开机总时长：'+duration)
            

            

    # 关闭窗口
    global_status = Status.MAIN
    list_window.close()

if __name__ == '__main__':
    multiprocessing.freeze_support()
    while global_status != Status.OFF:
        if global_status == Status.MAIN:
            MAIN_WIN()
        elif global_status == Status.LOAD:
            LOAD_FILE()
        elif global_status == Status.TABLE:
            TABLE_WIN()

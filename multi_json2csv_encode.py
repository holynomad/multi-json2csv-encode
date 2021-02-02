# -*- coding: utf-8 -*-
# json2csv (feat. 한글 encoding) 파서 @ 2021.02.01.
# ref & thx to : https://buttercoco.tistory.com/34

import json
import glob
from traceback import format_exc

from multiprocessing import Process

workers = 10

def iterdict(d, seq, fc):
    for k,v in d.items():
        if isinstance(v, dict):
            iterdict(v, seq + "/" + k, fc)
        elif isinstance(v, list):
            for e in v:
                iterdict(e, seq + "/" + k, fc)
        else:
            print(seq + "/" + k + ", " + str(v).replace("\n", '\\n'))
            fc.write(seq + "/" + k + ", " + str(v).replace("\n", '\\n') + "\n")

def multi_proc(json_data_list, output_path):

    for json_name, json_data in json_data_list:

        save_path = output_path + "/" + json_name + ".csv"

        try:
            fc = open(save_path, "a", encoding="euc-kr")
        except FileNotFoundError:
            print("save_path : ", save_path)

        fc.write("key, value\n")
        iterdict(json_data, "data", fc)
        fc.close()


# 메인
def main_convert(input_json_path, output_path):

    print('')
    print('=== getting started ===')
    print('')


    input_real_json_path = "G:/metadata"
    output_real_path = "G:/"

    # json 파일들이 존재하는 경로의 모든 파일을 가져옴
    json_path = glob.glob(input_real_json_path + "/*")
    
    print(json_path)

    json_data_list = []

    for each_path in json_path:

        each_json_name = each_path.split("/")[-1].split(".")[0]

        f = open(each_path, "r", encoding="euc-kr")
        json_data = {}

        try:
            json_data = json.loads(f.read())
        except:
            print(format_exc())

        print('')
        print('=== json_data ===')
        print(json_data)
        print('')

        f.close()
        json_data_list.append([each_json_name, json_data])

        print('')
        print('=== json_data_list : ===')
        print(json_data_list)
        print('')

    proc_list = []

    for wk in range(workers):
        front = int(len(json_data_list) * (wk / workers))
        rear = int(len(json_data_list) * ((wk + 1) / workers))

        proc = Process(target=multi_proc, args=(json_data_list[front:rear], output_real_path,))
        proc_list.append(proc)

    for proc in proc_list:
        proc.start()

    for proc in proc_list:
        proc.join()

# main 실행
if __name__ == '__main__':
    main_convert('','')      
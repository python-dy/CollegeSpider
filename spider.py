# -*- coding: utf-8 -*-
'''
作者 : dy
开发时间 : 2021/6/15 17:15
'''
import aiohttp
import asyncio
import pandas as pd
import json
from pathlib import Path


current_path = Path.cwd()

def get_url_list(max_id):
    url = 'https://static-data.eol.cn/www/school/%d/info.json'
    exist_id_list = []
    if Path.exists(Path(current_path, 'college_info.csv')):
        df = pd.read_csv(Path(current_path, 'college_info.csv'))
        exist_id_list = df['学校id'].values.tolist()
    url_list = []
    for id in range(0, max_id):
        if exist_id_list != [] and id in exist_id_list:
            continue
        url_list.append(url%id)
    return url_list


async def get_json_data(url, semaphore):
    async with semaphore:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
        }
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False), trust_env=True) as session:
            async with session.get(url=url, headers=headers, timeout=6) as response:
                # 更改相应数据的编码格式
                response.encoding = 'utf-8'
                # 遇到IO请求挂起当前任务，等IO操作完成执行之后的代码，当协程挂起时，事件循环可以去执行其他任务。
                json_data = await response.json()
                if json_data != '':
                    print(f"{url}爬取完成!")
                    return save_to_csv(json_data)


def save_to_csv(json_info):
    save_info = {}
    save_info['学校id'] = json_info['school_id']    # 学校id
    save_info['学校名称'] = json_info['name']    # 学校名字
    level = ""  # 高校层次
    if json_info['f985'] == '1' and json_info['f211'] == '1':
        level += "985 211"  # 判断高校层次
    elif json_info['f211'] == '1':
        level += "211"
    else:
        level += json_info['level_name']
    save_info['学校层次'] = level    # 学校层次
    save_info['学校类型'] = json_info['type_name']    # 学校类型
    save_info['所处地区'] = json_info['province_name'] + json_info['town_name']  # 所处地区
    save_info['招生办电话'] = json_info['phone']    # 招生办电话
    save_info['招生办官网'] = json_info['site']    # 招生办官网


    df = pd.DataFrame(save_info, index=[0])

    header = False if Path.exists(Path(current_path, 'college_info.csv')) else True
    df.to_csv(Path(current_path, 'college_info.csv'), index=False, mode='a', header=header)


async def main(loop):
    # 获取url列表
    url_list =  get_url_list(5000)
    # 限制并发量
    semaphore = asyncio.Semaphore(500)
    # 创建任务对象并添加到任务列表中
    tasks = [loop.create_task(get_json_data(url, semaphore)) for url in url_list]
    # 挂起任务列表
    await asyncio.wait(tasks)


if __name__ == '__main__':
    # 修改事件循环的策略
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    # 创建事件循环对象
    loop = asyncio.get_event_loop()
    # 将任务添加到事件循环中并运行循环直至完成
    loop.run_until_complete(main(loop))
    # 关闭事件循环对象
    loop.close()
    df = pd.read_csv(Path(current_path, 'college_info.csv'))
    df.drop_duplicates(keep='first', inplace=True)
    df.reset_index(drop=True, inplace=True)
    df.sort_values('学校id', inplace=True)
    df.to_csv(Path(current_path, 'college_info.csv'), index=False)
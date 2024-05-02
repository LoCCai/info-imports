import sqlite3
import threading
import time

import_file_address = "xx.txt"
import_file_lines_start = 1 #TXT文本开始行
import_file_lines_stop = 1000000 #TXT文本结束行
num_threads = 12  # 可以根据实际情况调整线程数
retry_delay = 0.1  # 重试延迟时间（秒）
import_chunk_size = 50 #每次导入尺寸

#with open(import_file_address, 'r', encoding='utf-8') as f:
#    print(len(f.readlines()))

def import_data_threaded(import_texts):
    # 创建一个新的数据库连接
    conn = sqlite3.connect('person-info.db')
    # 创建一个新的游标对象
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", ("person_info",))
    result = cursor.fetchone()
    if not result:
        # 创建一个名为person_info的表，包含多字段
        cursor.execute('''CREATE TABLE IF NOT EXISTS person_info (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    gender TEXT,
                    nation TEXT,
                    a_phone INTEGER,
                    a_id INTEGER,
                    b_phone INTEGER,
                    b_id INTEGER,
                    person_id TEXT,
                    address TEXT,
                    email TEXT,
                    other_info TEXT
                    )''')

    #检查待导入数据
    in_list = []
    for import_text in import_texts:
        
        # 判断数据库是否被锁定，如果被锁定则等待一段时间后进行重试
        while True:
            try:
                cursor.execute("SELECT * FROM person_info LIMIT 1")
                break
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e):
                    print("数据库被锁定，等待重试...")
                    time.sleep(retry_delay)
                else:
                    raise e
        
        cursor.execute("SELECT * FROM person_info WHERE a_phone = ? AND a_id = ?", (import_text[0], import_text[1]))
        result = cursor.fetchone()
        if not result:
            import_text = ["","","",import_text[0],import_text[1],0,0,"","","",""]
            in_list.append(import_text)
    # 这里可以根据需要修改导入逻辑

    # 判断数据库是否被锁定，如果被锁定则等待一段时间后进行重试
    while True:
        try:
            cursor.execute("SELECT * FROM person_info LIMIT 1")
            break
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                print("数据库被锁定，等待重试...")
                time.sleep(retry_delay)
            else:
                raise e

    # 开始插入数据
    cursor.executemany("INSERT INTO person_info (name, gender, nation, a_phone, a_id, b_phone, b_id, person_id, address, email,other_info) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", in_list)
    # 提交事务
    cursor.connection.commit()

    # 关闭连接
    conn.close()

def import_data_range(start_line, end_line):
    with open(import_file_address, 'r', encoding='utf-8') as f:
        current_line = 0  # 当前行数

        # 读取每一行直到到达指定范围内的行，跳过开始行之前的行
        for line in f:
            current_line += 1

            # 如果当前行在指定范围内，读取指定范围内的行
            if start_line <= current_line < end_line:
                # 这里可以根据需要修改文本处理
                import_texts = [f.readline().strip().split("\t") for _ in range(import_chunk_size)]  # 一次读取import_chunk_size行数据
                print(f"{current_line}-{current_line+9}|{import_texts[0][0]}-{import_texts[-1][1]}")

                # 在单独的线程中处理数据导入
                import_data_threaded(import_texts)

            # 如果已经到达结束行，则退出循环
            if current_line >= end_line:
                break

# 分配数据导入的范围给不同的线程处理
chunk_size = (import_file_lines_stop - import_file_lines_start) // num_threads
threads = []

for i in range(num_threads):
    start = import_file_lines_start + i * chunk_size
    end = start + chunk_size
    thread = threading.Thread(target=import_data_range, args=(start, end))
    threads.append(thread)
    thread.start()

# 等待所有线程完成
for thread in threads:
    thread.join()

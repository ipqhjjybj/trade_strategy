# coding=utf-8

import os
import logging
from datetime import datetime, timedelta
from logging.handlers import TimedRotatingFileHandler
from logging import DEBUG, INFO

from tumbler.function import get_folder_path


class FilePrint(object):
    """
    交易日志打印
    """

    def __init__(self, file_name, folder_name, mode="a"):
        self.folder_path = get_folder_path(folder_name)
        self.file_path = os.path.join(self.folder_path, file_name)
        self.level = DEBUG

        self.formatter = logging.Formatter('%(asctime)s  %(levelname)s: %(message)s')

        self.file_handler = TimedRotatingFileHandler(filename=self.file_path, when="H", interval=1, backupCount=0,
                                                     encoding='utf-8', delay=False)
        self.file_handler.suffix = "%Y%m%d%H%M%S"
        self.file_handler.setFormatter(self.formatter)
        self.file_handler.setLevel(self.level)

        self.logger = logging.getLogger(folder_name + file_name)
        self.logger.setLevel(self.level)
        self.logger.addHandler(self.file_handler)

    def write(self, msg, level=INFO):
        self.logger.log(level, msg)

    def close(self):
        self.file_handler.close()


'''
class FilePrint(object):
    """
    交易日志打印
    """

    def __init__(self, file_name, folder_name, mode="a"):
        self.folder_path = get_folder_path(folder_name)
        self.file_path = os.path.join(self.folder_path, file_name)
        self.fin = open(self.file_path, mode)

    def write(self, msg):
        self.fin.write(msg + "\n")
        self.fin.flush()

    def close(self):
        self.fin.close()
'''


def write_file(file_name: str, msg: str):
    f = open(file_name, "w")
    f.write(msg)
    f.close()


def append_msg(filepath: str, msg: str, is_first: bool = False):
    if is_first:
        f = open(filepath, "w")
    else:
        f = open(filepath, "a")
    f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "," + msg + "\n")
    f.close()


def read_all_lines(path: str):
    ret = []
    try:
        f = open(path, "r")
        for line in f:
            ret.append(line.strip())
        f.close()
    except Exception as ex:
        pass
    return ret


def get_last_line(filename: str):
    """
    get last line of a file
    :param filename: file name
    :return: last line or None for empty file
    """
    try:
        file_size = os.path.getsize(filename)
        if file_size == 0:
            return None
        else:
            with open(filename, 'rb') as fp:  # to use seek from end, must use mode 'rb'
                offset = -500  # initialize offset
                while -offset < file_size:  # offset cannot exceed file size
                    fp.seek(offset, 2)  # read # offset chars from eof(represent by number '2')
                    lines = fp.readlines()  # read from fp to eof
                    lines = [line for line in lines if len(line) > 1]
                    if len(lines) >= 2:  # if contains at least 2 lines
                        return lines[-1].decode('utf-8')  # then last line is totally included
                    else:
                        offset *= 2  # enlarge offset
                fp.seek(0)
                lines = fp.readlines()
                lines = [line for line in lines if len(line) > 1]
                if len(lines) == 0:
                    return ""
                else:
                    return lines[-1].decode('utf-8')
    except Exception as ex:
        return None


def get_last_several_lines(filename: str, offset: int = 1000):
    """
    get last serveral line of a file
    :param filename: file name
    :return: last line or None for empty file
    """
    try:
        file_size = os.path.getsize(filename)
        if file_size == 0:
            return []
        else:
            with open(filename, 'rb') as fp:  # to use seek from end, must use mode 'rb'
                offset = -1 * offset  # initialize offset
                while -offset < file_size:  # offset cannot exceed file size
                    fp.seek(offset, 2)  # read # offset chars from eof(represent by number '2')
                    lines = fp.readlines()  # read from fp to eof
                    lines = [line for line in lines if len(line) > 1]
                    if len(lines) >= 2:  # if contains at least 2 lines
                        lines = [line.decode('utf-8') for line in lines]
                        return lines
                    else:
                        offset *= 2  # enlarge offset
                fp.seek(0)
                lines = fp.readlines()
                lines = [line for line in lines if len(line) > 1]
                if len(lines) == 0:
                    return []
                else:
                    lines = [line.decode('utf-8') for line in lines]
                    return lines
    except Exception as ex:
        print("[Error] get_last_several_lines:{}".format(ex))
    return []


def get_datetime(msg: str, delay_minutes: int):
    try:
        time_msg = msg.split(',')[0]
        now_datetime = datetime.strptime(time_msg, "%Y-%m-%d %H:%M:%S")
        return now_datetime
    except Exception as ex:
        return datetime.now() - timedelta(minutes=delay_minutes)


def output_all_lines_to_file(filepath, all_lines, is_first=True):
    if is_first:
        f = open(filepath, "w")
    else:
        f = open(filepath, "a")
    for line in all_lines:
        f.write(line + "\n")
    f.close()


class LineOutput(object):
    """
    往一个文件夹，循环输出 数据
    目录:
    20160101
        20210206_000000.csv
        20210122_010000.csv
    """
    def __init__(self, u_dir):
        self.u_dir = u_dir

        self.pre_day = None
        self.pre_time_filename = None

        self.f = None

    def write(self, msg, timestamp):
        time_array = datetime.fromtimestamp(timestamp)
        now_day = time_array.strftime("%Y%m%d")
        new_dir = os.path.join(self.u_dir, now_day)
        if now_day != self.pre_day:
            if not os.path.exists(new_dir):
                os.mkdir(new_dir)
            self.pre_day = now_day

        now_time_file_array = time_array.replace(minute=0, second=0, microsecond=0)
        now_time_filename = now_time_file_array.strftime("%Y%m%d_%H%M%S.csv")

        if now_time_filename != self.pre_time_filename:
            self.pre_time_filename = now_time_filename
            if self.f:
                self.f.close()
            file_path = os.path.join(new_dir, now_time_filename)
            self.f = open(file_path, "w")

        self.f.write(msg + "\n")
        self.f.flush()


class LineIterator(object):
    """
    从一个文件夹中，循环读出一行行，缓存这样
    filename_parse_func 是传入的函数，通过文件名解析出 开始时间timestamp与结束时间timestamp
    """
    def __init__(self, u_dir, cache_num=1, suffix=".csv", filename_parse_func=None):
        self.suffix = suffix
        self.u_dir = u_dir
        self.list_dirs = self.get_list_dirs()
        self.is_finished = not self.list_dirs

        self.f = None
        self.current_filename = None
        self.filename_parse_func = filename_parse_func

        self.cache_num = cache_num
        self.cache_lines = []

    def get_list_dirs(self):
        ret = []
        for root, dirs, files in os.walk(self.u_dir, topdown=False):
            for name in files:
                if name.endswith(self.suffix):
                    ret.append(os.path.join(root, name))
        ret.sort()
        return ret

    def check_timestamp_bigger_than_inside(self, timestamp):
        first_file = None
        if self.current_filename:
            first_file = self.current_filename
        if self.list_dirs:
            first_file = self.list_dirs[0]

        if first_file:
            from_timestamp, to_timestamp = self.filename_parse_func(first_file)
            if from_timestamp >= timestamp:
                return True
        return False

    def get_first_file_timestamp(self):
        if self.current_filename:
            return self.filename_parse_func(self.current_filename)
        if self.list_dirs:
            return self.filename_parse_func(self.list_dirs[0])
        return None, None

    def locate_file_from_timestamp(self, timestamp):
        """
        这个函数是通过 timestamp定义哪些文件不合适，直接过滤掉
        """
        if self.current_filename:
            from_timestamp, to_timestamp = self.filename_parse_func(self.current_filename)
            if from_timestamp <= timestamp <= to_timestamp:
                print("locate_file_from_timestamp current_filename:{}".format(self.current_filename))
                return

        while self.list_dirs:
            self.current_filename = self.list_dirs[0]
            self.f = open(self.current_filename, "r")
            pop_file = self.list_dirs.pop(0)
            #print("pop file:{}".format(pop_file))

            from_timestamp, to_timestamp = self.filename_parse_func(self.current_filename)
            #print("why :{} {} {}".format(from_timestamp, timestamp, to_timestamp))
            if from_timestamp <= timestamp <= to_timestamp:
                break
            else:
                self.cache_lines.clear()
                self.f.close()
                self.f = None

    def load_data(self):
        flag = False
        if not self.f:
            if self.list_dirs:
                self.current_filename = self.list_dirs[0]
                print(self.current_filename)
                self.f = open(self.current_filename, "r")
                self.list_dirs.pop(0)
            else:
                self.is_finished = True
                return flag

        for i in range(self.cache_num):
            line = self.f.readline().strip()
            if line:
                self.cache_lines.append(line)
                flag = True
            else:
                self.f.close()
                self.f = None
                break
        return flag

    def get_last_line(self):
        if self.finished():
            return None

        if not self.cache_lines:
            if not self.load_data():
                # 如果失败，重复再试一次，处理一类特殊情况
                self.load_data()

        if self.cache_lines:
            return self.cache_lines[0]
        return None

    def pop(self):
        if self.cache_lines:
            self.cache_lines.pop(0)

    def finished(self):
        return self.is_finished


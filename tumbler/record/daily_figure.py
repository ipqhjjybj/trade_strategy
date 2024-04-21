# encoding: UTF-8

import os

from tumbler.service import log_service_manager


class MMDailyFigure(object):

    def __init__(self, _setting_position_filename="MMPositon_setting.json"):
        self.setting_position_filename = _setting_position_filename

    def parse_line(self, line):
        ret_dic = {}
        str_datetime, content = line.strip().split(':::')
        str_people, assets_content = content.split('::')
        assets_content = assets_content[1:-1]
        assets_arr = assets_content.split(',')

        for assets_str in assets_arr:
            if len(assets_str) > 0:
                asset, value = assets_str.split(':')
                ret_dic[asset] = float(value)

        str_date, str_time = str_datetime.split(' ')
        return ret_dic, str_date, str_time, str_people

    def people_figure_plot_name(self, people_name):
        if os.path.exists("./accounts") is False:
            log_service_manager.write_log("dir accounts is not exists")
        else:
            dirs = os.listdir("./accounts")

            arr_lines = []
            for fileDir in dirs:
                file_path_dir = "./accounts/" + fileDir

                if os.path.exists(file_path_dir) is True:
                    file_path = file_path_dir + "/" + people_name + ".log"
                    if os.path.exists(file_path) is True:
                        f = open(file_path, "r")
                        pre_line = ""
                        for line in f:
                            pre_line = line.strip()

                        if len(pre_line) > 0:
                            arr_lines.append(pre_line)
                        f.close()
                else:
                    log_service_manager.write_log("dir %s is not exists :{}".format(str(file_path_dir)))

            if len(arr_lines) > 0:
                if os.path.exists("./results") is False:
                    os.mkdir("./results")

                file_path = "./results/" + people_name + ".log"

                f = open(file_path, "w")
                for line in arr_lines:
                    f.write(line + "\n")
                f.close()

                if os.path.exists("./results_csv") is False:
                    os.mkdir("./results_csv")

                file_path = "./results_csv/" + people_name + ".csv"

                all_assets = set([])
                for line in arr_lines:
                    parse_assets, str_date, str_time, str_people = self.parse_line(line)

                    for asset in parse_assets.keys():
                        if 'all' in asset:
                            all_assets.add(asset)

                use_all_assets = list(all_assets)
                use_all_assets.sort()

                all_line_maps = {}
                
                for line in arr_lines:
                    parse_assets, str_date, str_time, str_people = self.parse_line(line)
                    s_line = str_date
                    for asset in use_all_assets:
                        now_value = parse_assets.get(asset, 0.0)
                        s_line = s_line + "," + str(now_value)
                    
                    all_line_maps[str_date] = s_line
                
                map_keys = list(all_line_maps.keys())
                map_keys.sort()
                
                f = open(file_path, "w")
                f.write("date" + "," + ','.join(use_all_assets) + "\n")
                for date in map_keys:
                    s_line = all_line_maps[date] + "\n"
                    f.write(s_line)
                f.close()

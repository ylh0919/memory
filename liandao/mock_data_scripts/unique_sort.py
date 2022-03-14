from numpy import float64, longlong, ndindex
import pandas as pd
import sys

# 脚本实现的内容为将219去重，转换为无重复的110格式
# 类型定义
class UniqueBar:
    instrument = ""
    exchange_id = ""
    open = ""
    close = ""
    low = ""
    high = ""
    volume = ""
    start_time = ""
    end_time = ""

    def __init__(self, instrument="", exchange_id="", open="", close="", low="", high="", volume="", start_time="", end_time=""):
        self.instrument = instrument
        self.exchange_id = exchange_id
        self.open = open
        self.close = close
        self.low = low
        self.high = high
        self.volume = volume
        self.start_time = start_time
        self.end_time = end_time
        self.key = instrument + start_time

    def __eq__(self, other):
        if self.key == other.key:
            return True
        else:
            return False

    def __hash__(self):
        return hash(self.key)

    def __lt__(self, other):
        return self.start_time < other.start_time
    
    def rnt_starttime(element):
        return 

    # def show(self):
    #     print(self.instrument, self.exchange_id, self.open, self.close,
    #           self.low, self.high, self.volume, self.start_time, self.end_time)


line_data = [{}]
row_name = []

# 数据读取解析


def read_data_form_csv(data_path):
    data_file = pd.read_csv(data_path)

    for line in data_file:
        row_name.append(line)

    unique_data = [UniqueBar]

    for index_col in range(len(data_file) - 1):
        for index_row in range(len(row_name)):
            if index_row == 3:
                bar_1000_list = []
                bar_1000_list.extend(
                    data_file[row_name[index_row]][index_col].split(";"))

                for index in range(len(bar_1000_list) - 1):
                    sub1 = bar_1000_list[index].split(",")
                    sub2 = sub1[3].split("|")
                    sub3 = sub1[4].split("|")

                    #print (data_file[row_name[0]][index_col], data_file[row_name[1]][index_col], sub1[0], sub1[1], sub1[2], sub2[0], sub2[1], sub3[0], sub3[1])
                    unique_data.append(UniqueBar(data_file[row_name[0]][index_col], data_file[row_name[1]]
                                       [index_col], sub1[0], sub1[1], sub1[2], sub2[0], sub3[1], sub2[1], sub3[0]))

    conversion_dataformat(list(set(unique_data)))

# 数据导出
def conversion_dataformat(unique_data):
    # unique_data.sort()
    instrument = []
    exchange_id = []
    open = []
    close = []
    low = []
    high = []
    volume = []
    start_time = []
    end_time = []

    for index in range(len(unique_data)):
        instrument.append(unique_data[index].instrument)
        exchange_id.append(unique_data[index].exchange_id)
        open.append(unique_data[index].open)
        close.append(unique_data[index].close)
        low.append(unique_data[index].low)
        high.append(unique_data[index].high)
        volume.append(unique_data[index].volume)
        start_time.append(unique_data[index].start_time)
        end_time.append(unique_data[index].end_time)

    # for index in range(len(unique_data)):
    #     if index != 0:
    #         print(int(unique_data[index].start_time) > int(unique_data[index - 1].start_time))

    dict = {'instrument': instrument, 'exchange_id': exchange_id, 'open': open, 'close': close,
            'low': low, 'high': high, 'volume': volume, 'start_time': start_time, 'end_time': end_time}

    df = pd.DataFrame(dict)
    df.sort_values(by='start_time',ascending=True)
    df.to_csv('./' + data_path + '_unique.csv', index=False)


if __name__ == '__main__':

    if len(sys.argv) != 2:
        print('params error: A 219 CSV data is required')
    else:
        data_path = sys.argv[1]
        read_data_form_csv(data_path)

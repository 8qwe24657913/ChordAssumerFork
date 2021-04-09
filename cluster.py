from typing import Callable

import pandas as pd
import pymysql

from chord import get_music
from config import *


class Part(object):
    """
    一段音乐
    """
    def __init__(self, start_idx: int, length: int, first: int, last: int) -> None:
        """
        start_idx: 第一个音符的下标
        length: 该段的长度
        first: 第一个音符的开始时间
        last: 最后一个音符的开始时间
        """
        super().__init__()
        self.start_idx = start_idx
        self.length = length
        self.first = first
        self.last = last
    def __len__(self) -> int:
        """
        音乐长度，以音符个数计
        """
        return self.length
    def __sub__(self, other: 'Part'):
        """
        两段音乐的距离，以时间计，1 为 1/16 音符时间
        """
        if self.first > other.last:
            return self.first - other.last
        elif self.last < other.first:
            return self.last - other.first
        else:
            raise Exception('part overlapped')
    def __add__(self, other: 'Part') -> 'Part':
        """
        合并两段音乐
        """
        assert self.start_idx + len(self) == other.start_idx, 'only consequent parts can be added'
        return Part(self.start_idx, self.length + other.length, self.first, other.last)
    def __repr__(self):
        return f'Part({self.start_idx}, {self.length}, {self.first}, {self.last})'


def cluster(music: pd.DataFrame, can_merge: Callable[[Part, Part, int, pd.DataFrame], bool]):
    """
    音乐聚类
    music: 音乐
    can_merge: 判断两段音乐是否能够合并的函数，其传入参数为：段落1, 段落2, 该合并阶段所允许的距离，整首音乐的信息
    """
    # 构造音乐段落
    data = []
    should_new = True
    for i, note in music.iterrows():
        # 段落不能跨休止符连接
        if note['step_id'] == -1:
            should_new = True
        else:
            if should_new:
                should_new = False
                data.append([])
            time = int(str(note['start_time'])) // ATOMIC_TIME
            data[-1].append(Part(int(str(i)), 1, time, time))
    # 聚类
    distance = 1 # 1/16 音符时间
    while distance <= 16: # 最多间隔一个全音符时间（后续可能修改）
        new_data = []
        for parts in data:
            new_parts = []
            for part in parts:
                if new_parts and (part - new_parts[-1] <= distance) and can_merge(new_parts[-1], part, distance, music):
                    new_parts[-1] += part
                else:
                    new_parts.append(part)
            new_data.append(new_parts)
        data = new_data
        distance *= 2
    return data
    


if __name__ == '__main__':
    mysql = pymysql.connect(**DATABASE_CONFIG)
    music = get_music(16, mysql)
    print(cluster(music, lambda a, b, distance, music: True))

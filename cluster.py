from dataclasses import dataclass
from itertools import chain
from typing import Callable, List

import pandas as pd

from config import *
from utils import get_connection, get_music


@dataclass(eq=False, frozen=True)
class Part(object):
    """
    一段音乐

    start_idx: 第一个音符的下标
    length: 该段的长度
    first: 第一个音符的开始时间
    last: 最后一个音符的开始时间
    """

    start_idx: int
    length: int
    first: int
    last: int

    def __len__(self) -> int:
        """
        音乐长度，以音符个数计
        """
        return self.length

    def __sub__(self, other: 'Part'):
        """
        两段音乐的距离，以时间计，1 为 1/BEAT_LCM 音符时间
        """
        if self.first >= other.last:
            return self.first - other.last
        elif self.last <= other.first:
            return self.last - other.first
        else:
            raise Exception('part overlapped')

    def __add__(self, other: 'Part') -> 'Part':
        """
        合并两段音乐
        """
        assert self.start_idx + \
            len(self) == other.start_idx, 'only sequential parts can be added'
        return Part(self.start_idx, self.length + other.length, self.first, other.last)


def cluster(music: pd.DataFrame, can_merge: Callable[[Part, Part, int, pd.DataFrame], bool]) -> List[Part]:
    """
    音乐聚类
    music: 音乐
    can_merge: 判断两段音乐是否能够合并的函数，其传入参数为：段落1, 段落2, 该合并阶段所允许的距离，整首音乐的信息
    """
    # 构造音乐段落
    data: List[List[Part]] = []
    should_new = True
    for i, (_, note) in enumerate(music.iterrows()):
        music.index
        # 段落不能跨休止符连接
        if note['step_id'] == -1:
            should_new = True
        else:
            if should_new:
                should_new = False
                data.append([])
            time = int(note['start_time'])  # type: ignore
            data[-1].append(Part(i, 1, time, time))
    # 聚类
    distance = 1  # 1/BEAT_LCM 音符时间
    while distance <= BEAT_LCM:  # 最多间隔一个全音符时间（后续可能修改）
        new_data: List[List[Part]] = []
        for parts in data:
            new_parts: List[Part] = []
            for part in parts:
                if new_parts and (part - new_parts[-1] <= distance) and can_merge(new_parts[-1], part, distance, music):
                    new_parts[-1] += part
                else:
                    new_parts.append(part)
            new_data.append(new_parts)
        data = new_data
        distance *= 2 # 每次迭代允许的距离翻倍
    return list(chain(*data))


if __name__ == '__main__':
    with get_connection() as mysql:
        music = get_music(16, mysql)

    def can_merge(part1: Part, part2: Part, distance: int, music: pd.DataFrame) -> bool:
        if distance <= BEAT_LCM // 16:  # 距离小于等于 1/16 音符直接合并
            return True
        if part2.last - part1.first >= BEAT_LCM * 2:  # 和弦不会长于 2 小节
            return False
        # 演示一下如何获取到具体的音符信息
        # print(music.iloc[[*range(part1.start_idx, part1.start_idx + part1.length)]])
        return True
    merged_parts = cluster(music, can_merge)
    print(pd.DataFrame(merged_parts))

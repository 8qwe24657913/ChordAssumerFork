from dataclasses import dataclass, field
from typing import Callable, List, Tuple

import pandas as pd

from config import *
from utils import (format_note, get_connection, get_music, left_justified,
                   pandas_format, simplify_fraction)


@dataclass(eq=False, frozen=True)
class PartRepr(object):
    time_length: int
    note_count: int
    notes: List[Tuple[str, str]]


@dataclass(eq=False, frozen=True)
class Part(object):
    """
    一段音乐

    music: 整首音乐
    start_idx: 第一个音符的下标
    length: 该段的长度
    """

    music: pd.DataFrame = field(repr=False)
    start_idx: int
    length: int

    @property
    def end_idx(self) -> int:
        """
        结尾位置（空指）
        """
        return self.start_idx + self.length

    @property
    def first(self):
        """
        第一个音符
        """
        return self.music.iloc[self.start_idx]

    @property
    def last(self):
        """
        最后一个音符
        """
        return self.music.iloc[self.end_idx - 1]

    @property
    def first_time(self) -> int:
        """
        第一个音符的开始时间
        """
        return int(self.first['start_time'])  # type: ignore

    @property
    def last_time(self) -> int:
        """
        最后一个音符的开始时间
        """
        return int(self.last['start_time'])  # type: ignore

    @property
    def last_is_pause(self) -> bool:
        """
        最后一个音符是否是休止符
        """
        return self.last['step_id'] == -1

    def __len__(self) -> int:
        """
        音乐长度，以音符个数计
        """
        return self.length

    def __sub__(self, other: 'Part'):
        """
        两段音乐的距离，以时间计，1 为 1/BEAT_LCM 音符时间
        """
        if self.first_time >= other.last_time:
            return self.first_time - other.last_time
        elif self.last_time <= other.first_time:
            return self.last_time - other.first_time
        else:
            raise Exception('part overlapped')

    def __add__(self, other: 'Part') -> 'Part':
        """
        合并两段音乐
        """
        assert self.music is other.music, 'only parts in the same music can be added'
        assert self.end_idx == other.start_idx, f'only sequential parts can be added'
        return Part(self.music, self.start_idx, self.length + other.length)

    def format(self) -> PartRepr:
        formatted = []
        # start_time 相等的，end_time 也应相等，且前一个音符的 end_time 等于后一个音符的 start_time
        for _, group in self.music.iloc[self.start_idx:self.end_idx].groupby('start_time'):
            notes = ' '.join(
                (format_note(note['step_id']) for _, note in group.iterrows()))
            time_length = simplify_fraction(
                int(group['duration'].iat[0]), BEAT_LCM)
            formatted.append((notes, time_length))
        end_time = self.last_time + int(self.last['duration'])  # type: ignore
        return PartRepr(
            time_length=end_time - self.first_time,
            note_count=len(formatted),
            notes=formatted,
        )


def cluster(music: pd.DataFrame, can_merge: Callable[[Part, Part, int, pd.DataFrame], bool]) -> List[Part]:
    """
    音乐聚类
    music: 音乐
    can_merge: 判断两段音乐是否能够合并的函数，其传入参数为：段落1, 段落2, 该合并阶段所允许的距离，整首音乐的信息
    """
    # 构造音乐段落
    data: List[Part] = []
    last_step_id: int = 0
    for i, (_, note) in enumerate(music.iterrows()):
        part = Part(music, i, 1)
        step_id: int = note['step_id']  # type: ignore
        # 时间相同的直接合并，多个连续的休止符也直接合并
        if data and (data[-1].last_time == part.first_time or step_id == last_step_id == -1):
            data[-1] += part
        else:
            data.append(part)
        last_step_id = step_id

    # 聚类
    step_distance = 1  # 1/BEAT_LCM 音符时间
    while step_distance <= BEAT_LCM:  # 最多间隔一个全音符时间（后续可能修改）
        new_data: List[Part] = []
        for part in data:
            # 没有上一段音符或两段音符中有一段全部为休止符，无法合并
            if part.last_is_pause or len(new_data) <= 1 and (not new_data or new_data[-1].last_is_pause):
                new_data.append(part)
                continue
            # 上一个非休止符的音符
            last_meaningful_idx = -2 if new_data[-1].last_is_pause else -1
            last_meaningful_part = new_data[last_meaningful_idx]
            if new_data and (part - last_meaningful_part <= step_distance) and can_merge(last_meaningful_part, part, step_distance, music):
                if last_meaningful_idx == -2:
                    # 跨休止符合并会将休止符一起合并进去，保证段落的完整性
                    part = new_data.pop() + part
                new_data[-1] += part
            else:
                new_data.append(part)
        data = new_data
        step_distance *= 2  # 每次迭代允许的距离翻倍
    return data


if __name__ == '__main__':
    with get_connection() as mysql:
        music = get_music(16, mysql)

    def can_merge(part1: Part, part2: Part, step_distance: int, music: pd.DataFrame) -> bool:
        # if distance <= BEAT_LCM // 16:  # 距离小于等于 1/16 音符直接合并
        #     return True
        # if part2.last_time - part1.first_time >= BEAT_LCM * 2:  # 和弦不会长于 2 小节
        #     return False
        # 演示一下如何获取到具体的音符信息
        # print(music.iloc[[*range(part1.start_idx, part1.start_idx + part1.length)]])
        # return True
        part_distance = part2 - part1
        assert part_distance <= BEAT_LCM
        merged_length = part2.last_time - part1.first_time
        return merged_length <= TOLERANCE_CLUSTER_LENGTH[part_distance // CATEGORY_LEN]
    merged_parts = cluster(music, can_merge)
    with pandas_format({
        'display.max_columns': None,
        'display.max_rows': None,
        'display.max_colwidth': None,
        'display.width': None,
        'display.colheader_justify': 'left',
    }):
        print(left_justified(pd.DataFrame(
            [part.format() for part in merged_parts]
        )))

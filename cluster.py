from dataclasses import dataclass, field
from typing import Callable, List, Union

import pandas as pd

from config import *
from utils import (format_note, get_bpm, get_connection, get_music,
                   left_justified, pandas_format, simplify_fraction)


@dataclass(eq=False, frozen=True)
class PartRepr(object):
    time_length: int
    note_count: int
    notes: List[Union[str, List[str]]]


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
    end_time: int

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
    def first_start_time(self) -> int:
        """
        第一个音符的开始时间
        """
        return self.first['start_time']  # type: ignore

    @property
    def last_start_time(self) -> int:
        """
        最后一个音符的开始时间
        """
        return self.last['start_time']  # type: ignore

    @property
    def is_rest(self) -> bool:
        """
        最后一个音符是否是休止符
        """
        return self.first['step_id'] == -1

    def __len__(self) -> int:
        """
        音乐长度，以音符个数计
        """
        return self.length

    def __sub__(self, other: 'Part'):
        """
        两段音乐的距离，以时间计，1 为 1/BEAT_LCM 音符时间
        """
        if self.first_start_time >= other.last_start_time:
            return self.first_start_time - other.last_start_time
        elif self.last_start_time <= other.first_start_time:
            return self.last_start_time - other.first_start_time
        else:
            raise Exception('part overlapped')

    def __add__(self, other: 'Part') -> 'Part':
        """
        合并两段音乐
        """
        assert self.music is other.music, 'only parts in the same music can be added'
        assert self.end_idx == other.start_idx, f'only sequential parts can be added'
        return Part(self.music, self.start_idx, self.length + other.length, max(self.end_time, other.end_time))

    def format(self) -> PartRepr:
        formatted: List[Union[str, List[str]]] = []
        # start_time 相等的，end_time 也应相等，且前一个音符的 end_time 等于后一个音符的 start_time
        for _, start_time_group in self.music.iloc[self.start_idx:self.end_idx].groupby('start_time'):
            duration_formatted: List[str] = []
            for duration, duration_group in start_time_group.groupby('duration'):
                notes = ' '.join(
                    (format_note(note['step_id']) for _, note in duration_group.iterrows()))
                time_length = simplify_fraction(duration, BEAT_LCM)
                duration_formatted.append(f'{notes} {time_length}')
            formatted.append(duration_formatted[0] if len(
                duration_formatted) == 1 else duration_formatted)
        return PartRepr(
            time_length=self.end_time - self.first_start_time,
            note_count=self.end_idx - self.start_idx,
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
    for i, (_, note) in enumerate(music.iterrows()):
        end_time: int = note['end_time']  # type: ignore
        part = Part(music, i, 1, end_time)
        if data and (
            part.first_start_time == data[-1].last_start_time  # 时间相同的直接合并
            or part.is_rest and data[-1].is_rest  # 多个连续的休止符直接合并
        ):
            data[-1] += part
        else:
            data.append(part)

    # 聚类
    step_distance = 1  # 1/BEAT_LCM 音符时间
    while step_distance <= BEAT_LCM:  # 最多间隔一个全音符时间（后续可能修改）
        new_data: List[Part] = []
        for part in data:
            # 没有上一段音符或两段音符中有一段全部为休止符，无法合并
            if part.is_rest or (len(new_data) <= 1 and (not new_data or new_data[-1].is_rest)):
                new_data.append(part)
                continue
            # 上一个非休止符的音符
            last_meaningful_idx = -2 if new_data[-1].is_rest else -1
            last_meaningful_part = new_data[last_meaningful_idx]
            if new_data and (part - last_meaningful_part <= step_distance) and (
                last_meaningful_part.end_time > part.first_start_time  # 时间片重叠，合并，不然从一个音中间切开也太离谱了
                or can_merge(last_meaningful_part, part, step_distance, music)
            ):
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
    MUSIC_ID = 49
    with get_connection() as mysql:
        music = get_music(MUSIC_ID, mysql)
        assert len(music) > 0, 'music is empty'
        bpm = get_bpm(MUSIC_ID, mysql)

    def can_merge(part1: Part, part2: Part, step_distance: int, music: pd.DataFrame) -> bool:
        # if distance <= BEAT_LCM // 16:  # 距离小于等于 1/16 音符直接合并
        #     return True
        # if part2.last_start_time - part1.first_start_time >= BEAT_LCM * 2:  # 和弦不会长于 2 小节
        #     return False
        # 演示一下如何获取到具体的音符信息
        # print(music.iloc[[*range(part1.start_idx, part1.start_idx + part1.length)]])
        # return True
        part_distance = part2 - part1
        assert 0 <= part_distance <= BEAT_LCM
        merged_length = part2.last_start_time - part1.first_start_time
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
    from to_mid import to_mid
    mid = to_mid(music, bpm, merged_parts)
    mid.save(f'{MUSIC_ID}.mid')

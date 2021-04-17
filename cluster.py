from dataclasses import dataclass, field
from typing import Callable, List, Optional

import pandas as pd

from config import *
from utils import (format_note, get_bpm, get_connection, get_music,
                   left_justified, pandas_format, simplify_fraction)


@dataclass(eq=False, frozen=True)
class PartRepr(object):
    time_length: int
    note_count: int
    beat: str
    notes: List[str]


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
        @dataclass(eq=False, frozen=True, repr=False)
        class AtomGroup(object):
            notes: pd.DataFrame
            end_time: int
        # 分组，若两音符时间上有重叠，则其应被分为一组，要求分组数量尽可能多
        grouped: List[AtomGroup] = []
        for (start_time, end_time), group in self.music.iloc[self.start_idx:self.end_idx].groupby(['start_time', 'end_time']):
            start_time = int(start_time)
            end_time = int(end_time)
            if not grouped or grouped[-1].end_time <= start_time:
                grouped.append(AtomGroup(group, end_time))
            else:
                grouped[-1] = AtomGroup(
                    pd.concat((grouped[-1].notes, group)),
                    max(grouped[-1].end_time, end_time),
                )
        # 再次分组，将同一组分为多个时间线，每个时间线均可线性表示，要求分时间线数量尽可能少
        @dataclass(eq=False, repr=False)
        class Timeline(object):
            notes: List[str]
            end_time: int
        formatted: List[str] = []
        for group in grouped:
            timelines: List[Timeline] = []
            min_start_time: int = group.notes['start_time'].iat[0]  # type: ignore
            for (start_time, duration), same_time_group in group.notes.groupby(['start_time', 'duration']):
                start_time = int(start_time)
                duration = int(duration)
                # 贪心法找到 timeline.end_time 最大但不超过 start_time 的 timeline
                greedy_timeline: Optional[Timeline] = None
                for timeline in timelines:
                    if timeline.end_time <= start_time and (
                        not greedy_timeline
                        or greedy_timeline.end_time < timeline.end_time
                    ):
                        greedy_timeline = timeline
                # 找不到合适的时间轴的话就添加一个
                if not greedy_timeline:
                    greedy_timeline = Timeline([], min_start_time)
                    timelines.append(greedy_timeline)
                # 如果 timeline.end_time 小于 start_time，则应添加休止符以补齐时间轴的空位
                if greedy_timeline.end_time < start_time:
                    greedy_timeline.notes.append(
                        f'{format_note(-1)} {start_time - greedy_timeline.end_time}'
                    )
                # 将音符组加入 greedy_timeline
                notes = ' '.join(
                    (
                        format_note(note['step_id'])
                        for _, note in same_time_group.iterrows()
                    )
                )
                time_length = simplify_fraction(duration, BEAT_LCM)
                greedy_timeline.notes.append(f'{notes} {time_length}')
                greedy_timeline.end_time = start_time + duration  # 别忘了更新 end_time
            # 尽量避免添加括号，优化可读性
            timeline_strings = [
                timeline.notes[0] if len(timeline.notes) == 1
                else f'({", ".join(timeline.notes)})'
                for timeline in timelines
            ]
            formatted.append(
                timeline_strings[0] if len(timeline_strings) == 1
                else f'({", ".join(timeline_strings)})'
            )
        beats, beat_type = self.first[['beats', 'beat_type']]
        return PartRepr(
            time_length=self.end_time - self.first_start_time,
            note_count=self.end_idx - self.start_idx,
            beat=f'{beats}/{beat_type}',
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
    MUSIC_ID = 438
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
        beats, beat_type = part1.first[['beats', 'beat_type']]
        beats2, beat_type2 = part2.first[['beats', 'beat_type']]
        # 只合并节拍相同的段落
        if (beats, beat_type) != (beats2, beat_type2):
            return False
        part_distance = part2 - part1
        assert 0 <= part_distance <= BEAT_LCM
        merged_length = part2.last_start_time - part1.first_start_time
        return merged_length <= TOLERANCE_CLUSTER_LENGTH[part_distance // CATEGORY_LEN] * beats / beat_type
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

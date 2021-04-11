from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List

import pandas as pd

from config import *
from utils import combine_weight, get_connection, get_music


@dataclass(eq=False, frozen=True)
class Transposition(object):
    """
    表示一种和弦的一种转位
    """
    chord: 'Chord'
    idx: int
    order: List[int]

    def __repr__(self) -> str:
        return f'Transposition({self.chord.name}, {self.idx})'


class Chord(object):
    """
    表示一种和弦
    """
    deduplicate_set = set()
    chords = {}

    def __init__(self, name: str, order: List[int]) -> None:
        self.name = name
        self.trans = []
        order = Chord.normalize_order(order)
        # 自动生成其转位
        for idx in range(len(order)):
            key = ','.join([str(o) for o in sorted(order)])
            if key in Chord.deduplicate_set:  # 0,4,8 和 0,3,6,9 等和弦转位时会产生重复
                # print('duplicated:', order)
                continue
            Chord.deduplicate_set.add(key)
            self.trans.append(Transposition(self, idx, order))
            order = Chord.transposition(order)

    def __repr__(self) -> str:
        return f'Chord({self.name})'

    @staticmethod
    def normalize_order(order: List[int]) -> List[int]:
        """
        使一个顺序的最小值为 0
        order: 顺序
        """
        min_order = min(order)
        return [o - min_order for o in order]

    @staticmethod
    def transposition(order: List[int]) -> List[int]:
        """
        生成下一个转位的顺序
        order: 顺序
        """
        order = order[1:] + [order[0] + 12]
        return Chord.normalize_order(order)

    @staticmethod
    def init() -> None:
        for name, order in CHORDS.items():
            Chord.chords[name] = Chord(name, order)


Chord.init()


class Measure(object):
    """
    表示一种小节
    """
    measures = {}

    def __init__(self, beats: int, beat_type: int, weight: List[float]) -> None:
        self.name = f'{beats}/{beat_type}'
        self.pb_n = BEAT_LCM // beat_type
        self.beats = beats
        self.length = beats * self.pb_n
        self.weight = weight

    def __repr__(self) -> str:
        return f'Measure(\'{self.name}\')'

    @staticmethod
    def init() -> None:
        for name, original_weight in WEIGHT.items():
            beats, beat_type = [int(n) for n in name.split('/')]
            assert beats == len(original_weight)
            b_inner = WEIGHT_B_INNER[beat_type]
            assert beat_type * len(b_inner) == BEAT_LCM
            weight = combine_weight(original_weight, b_inner)
            Measure.measures[name] = Measure(beats, beat_type, weight)


Measure.init()


@dataclass(eq=False, frozen=True)
class Note(object):
    """
    表示一个音符的数据
    """

    step_id: int
    start_time: int
    duration: int


@dataclass(eq=False, frozen=True)
class MeasurePart(object):
    """
    表示一个小节的数据
    """

    measure: Measure
    notes: List[Note]


@dataclass(eq=False, frozen=True)
class Assumption(object):
    """
    表示一个和弦猜测的数据
    """

    chord: Chord
    root: int
    weight: float
    trans: List[int] = field(default_factory=list)


def get_notes(music: pd.DataFrame) -> List[MeasurePart]:
    """
    以对象的数组形式获取一首音乐的所有音符
    """
    measure_parts = music.groupby('measure_id')
    music['start_time'] -= measure_parts['start_time'].transform('min')
    return [MeasurePart(
        measure=Measure.measures[f'{measure_part["beats"].iat[0]}/{measure_part["beat_type"].iat[0]}'],
        notes=[Note(note['step_id'], note['start_time'], note['duration'])
               for _, note in measure_part.iterrows()],
    ) for _, measure_part in measure_parts]

NoteWeightDict = Dict[int, float]

def get_weight(measure_part: MeasurePart) -> NoteWeightDict:
    """
    计算音符的权重
    """
    measure_weight = measure_part.measure.weight
    used_weight = [[] for _ in measure_weight]
    for note in measure_part.notes:
        if note.step_id != -1:
            for i in range(note.start_time, note.start_time + note.duration):
                used_weight[i].append(note.step_id)
    note_weight = defaultdict(float)
    for weight, used in zip(measure_weight, used_weight):
        if used:
            weight_piece = weight / len(used)
            for step in used:
                note_weight[step] += weight_piece
    return dict(note_weight)


def sort_chord_transpositions(note_weight: NoteWeightDict) -> List[Assumption]:
    """
    计算并排序每种转位的权重
    """
    min_step = min(note_weight.keys())
    max_step = max(note_weight.keys())
    result = []
    for chord in Chord.chords.values():
        to_be_merged = {}
        for trans in chord.trans:
            for offset in range(min_step - trans.order[-1], max_step + 1):
                weight = 0.0
                for step in trans.order:
                    weight += note_weight.get(offset + step, 0.0)
                if weight > 0.0:
                    root = offset + trans.order[-trans.idx]
                    key = f'{root}|{weight}'
                    if key not in to_be_merged:
                        item = Assumption(
                            chord=chord,
                            root=root,
                            weight=weight,
                        )
                        to_be_merged[key] = item
                        result.append(item)
                    to_be_merged[key].trans.append(trans.idx)
    result.sort(key=lambda item: item.weight, reverse=True)
    return result


if __name__ == '__main__':
    with get_connection() as mysql:
        music = get_music(16, mysql)
    measure_parts = get_notes(music)
    print(measure_parts[0])  # 太多了，只 print 一个吧
    note_weights = [get_weight(measure_part) for measure_part in measure_parts]
    print(note_weights[0])
    order = [sort_chord_transpositions(note_weight)
             for note_weight in note_weights]
    # print(order[0])
    # for item in order[0][:20]:
    #     print(item)
    print(pd.DataFrame(order[0]).head(20))

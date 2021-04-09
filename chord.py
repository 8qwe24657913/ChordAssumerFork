from collections import defaultdict

import pandas as pd
import pymysql

from config import *

# 转位类
class Transposition(object):
    """
    表示一种和弦的一种转位
    """
    def __init__(self, chord, idx, order):
        self.chord = chord
        self.idx = idx
        self.order = order
    def __repr__(self):
        return f'Transposition({self.chord.name}, {self.idx})'


class Chord(object):
    """
    表示一种和弦
    """
    deduplicate_set = set()
    chords = {}
    def __init__(self, name, order):
        self.name = name
        self.trans = []
        order = Chord.normalize_order(order)
        # 自动生成其转位
        for idx in range(len(order)):
            key = ','.join([str(o) for o in sorted(order)])
            if key in Chord.deduplicate_set: # 0,4,8 和 0,3,6,9 等和弦转位时会产生重复
                # print('duplicated:', order)
                continue
            Chord.deduplicate_set.add(key)
            self.trans.append(Transposition(self, idx, order))
            order = Chord.transposition(order)
    def __repr__(self):
        return f'Chord(\'{self.name}\')'
    @staticmethod
    def normalize_order(order):
        """
        使一个顺序的最小值为 0
        order: 顺序
        """
        min_order = min(order)
        return [o - min_order for o in order]
    @staticmethod
    def transposition(order):
        """
        生成下一个转位的顺序
        order: 顺序
        """
        order = order[1:] + [order[0] + 12]
        return Chord.normalize_order(order)
    @staticmethod
    def init():
        for name, order in CHORDS.items():
            Chord.chords[name] = Chord(name, order)
Chord.init()


class Measure(object):
    """
    表示一种小节
    """
    measures = {}
    def __init__(self, beats, beat_type, weight):
        self.name = f'{beats}/{beat_type}'
        self.pb16 = BEAT_LCM // beat_type
        self.beats = beats
        self.length = beats * self.pb16
        self.weight = weight
    def __repr__(self):
        return f'Measure(\'{self.name}\')'
    @staticmethod
    def init():
        for name, original_weight in WEIGHT.items():
            beats, beat_type = [int(n) for n in name.split('/')]
            assert beats == len(original_weight)
            b_inner = WEIGHT_B_INNER[beat_type]
            assert beat_type * len(b_inner) == BEAT_LCM
            weight = [w * b for w in original_weight for b in b_inner]
            Measure.measures[name] = Measure(beats, beat_type, weight)
Measure.init()

class Note(object):
    """
    表示一个音符
    """
    def __init__(self, step_id, start_time, duration):
        self.step_id = step_id
        self.start_time = start_time
        self.duration = duration
    def __repr__(self):
        return f'Note({self.step_id}, {self.start_time}, {self.duration})'

def get_music(mu_id, mysql):
    assert type(mu_id) is int
    mu_id = str(mu_id)
    mu_id = '0' * (MU_ID_LEN - len(mu_id)) + mu_id
    sql = f"""select n.measure_id,n.step_id,n.start_time,n.duration,m.beats,m.beat_type
                from t_music_measure m, t_music_note n
                where m.measure_id = n.measure_id
                and m.mu_id='{mu_id}'
                and n.mu_id='{mu_id}'
                and n.stave_id=1
                and m.part_id='P1'
                order by start_time asc"""
    return pd.read_sql(sql, mysql)

def get_notes(mu_id, mysql):
    """
    从数据库中获取一首音乐的所有音符
    """
    music = get_music(mu_id, mysql)
    measure_parts = music.groupby('measure_id')
    music['start_time'] -= measure_parts['start_time'].transform('min')
    music['start_time'] //= ATOMIC_TIME
    music['duration'] //= ATOMIC_TIME
    return [{
        'measure': Measure.measures[f'{measure_part["beats"].iloc[0]}/{measure_part["beat_type"].iloc[0]}'],
        'notes': [Note(int(note['step_id']), note['start_time'], note['duration']) for _, note in measure_part.iterrows()],
    } for _, measure_part in measure_parts]

def get_weight(measure_part):
    """
    计算音符的权重
    """
    measure_weight = measure_part['measure'].weight
    used_weight = [[] for _ in measure_weight]
    for note in measure_part['notes']:
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

def sort_chord_transpositions(note_weight):
    """
    计算并排序每种转位的权重
    """
    min_step = min(note_weight.keys())
    max_step = max(note_weight.keys())
    result = []
    for name, chord in Chord.chords.items():
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
                        item = {
                            'chord': trans.chord,
                            'root': root,
                            'weight': weight,
                            'trans': [],
                        }
                        to_be_merged[key] = item
                        result.append(item)
                    to_be_merged[key]['trans'].append(trans.idx)
    result.sort(key=lambda item: item['weight'], reverse=True)
    return result

if __name__ == '__main__':
    mysql = pymysql.connect(**DATABASE_CONFIG)
    measure_parts = get_notes(16, mysql)
    mysql.close()
    print(measure_parts[0]) # 太多了，只 print 一个吧
    note_weights = [get_weight(measure_part) for measure_part in measure_parts]
    print(note_weights[0])
    order = [sort_chord_transpositions(note_weight) for note_weight in note_weights]
    # print(order[0])
    # for item in order[0][:20]:
    #     print(item)
    print(pd.DataFrame(order[0]).head(20))

from dataclasses import dataclass
from typing import Callable, List

import pandas as pd
from mido import MidiFile, MidiTrack, bpm2tempo
from mido.frozen import FrozenMessage, FrozenMetaMessage
from mido.messages import BaseMessage

from cluster import Part
from config import ATOMIC_TIME, BEAT_LCM


def to_mid(music: pd.DataFrame, bpm: pd.DataFrame, clusters: List[Part]) -> MidiFile:
    # midi 音色表 https://blog.csdn.net/ruyulin/article/details/84103186
    PROGRAM = 0  # 乐曲音色
    CHANNEL = 0  # 乐曲通道
    VELOCITY = 64  # 乐曲音量
    TIME_RATIO = ATOMIC_TIME * 480 // 6720  # 1/ATOMIC_TIME 音符时长与 midi 时长的比例
    ADD_BEEP = True
    BEEP_PROGRAM = 112  # 提示音音色
    BEEP_CHANNEL = 1  # 提示音通道
    BEEP_VELOCITY = VELOCITY  # 提示音音量
    BEEP_NOTE = 90  # 提示音音符
    BEEP_LENGTH = BEAT_LCM // 4  # 提示音时长

    mid = MidiFile()
    track = MidiTrack()
    mid.tracks.append(track)
    track.append(FrozenMessage(
        'program_change',
        program=PROGRAM,
        channel=CHANNEL,
    ))

    class MessageRepr(object):
        def __init__(self, constructor: Callable[..., BaseMessage], type: str, time: int, **kwargs) -> None:
            super().__init__()
            self.constructor = constructor
            self.type = type
            self.time = time
            self.kwargs = kwargs

        def to_message(self) -> BaseMessage:
            return self.constructor(
                self.type,
                time=self.time * TIME_RATIO,
                **self.kwargs,
            )

        # def __repr__(self):
        #     return f'MessageRepr({self.constructor.__name__}, {self.type}, {self.time}, {self.kwargs})'

    messages: List[MessageRepr] = []

    # time_signature & set_tempo, 依照小节确定
    bpm_pos = 0
    last_beats = last_beat_type = None
    for measure_id, measure in music.groupby('measure_id'):
        measure_id = int(measure_id)
        beats, beat_type, time = measure.iloc[0][
            ['beats', 'beat_type', 'start_time']
        ]
        # 几几拍
        if last_beats is None or beats != last_beats or beat_type != last_beat_type:
            messages.append(MessageRepr(
                FrozenMetaMessage,
                'time_signature',
                time,
                numerator=beats,
                denominator=beat_type,
            ))
            last_beats, last_beat_type = beats, beat_type
        # tempo
        if bpm_pos < len(bpm) and measure_id == bpm['measure_id'].iat[bpm_pos]:
            tempo = bpm2tempo(bpm['bpm'].iat[bpm_pos])
            messages.append(MessageRepr(
                FrozenMetaMessage,
                'set_tempo',
                time,
                tempo=tempo,
            ))
            bpm_pos += 1

    # note_on & note_off，依照音符确定
    for _, note_info in music.iterrows():
        step_id: int = note_info['step_id']  # type: ignore
        if step_id == -1:
            continue
        step_id += 20
        messages.append(MessageRepr(
            FrozenMessage,
            'note_on',
            note_info['start_time'],  # type: ignore
            note=step_id,
            velocity=VELOCITY,
            channel=CHANNEL,
        ))
        messages.append(MessageRepr(
            FrozenMessage,
            'note_off',
            note_info['end_time'],  # type: ignore
            note=step_id,
            velocity=VELOCITY,
            channel=CHANNEL,
        ))

    # 时间相同时的排序顺序
    SORT_PRIORITY = [
        'note_off',
        'time_signature',
        'set_tempo',
        'note_on',
    ]

    SORT_PRIORITY_DICT = {
        type: i for i, type in enumerate(SORT_PRIORITY)
    }

    messages.sort(key=lambda message: (message.time, SORT_PRIORITY_DICT[message.type]))

    # 添加提示音
    if ADD_BEEP:
        track.append(FrozenMessage(
            'program_change',
            program=BEEP_PROGRAM,
            channel=1,
        ))
        beep_times = [part.end_time for part in clusters[:-1]]
        # print(beep_times)
        add_time = 0
        new_messages: List[MessageRepr] = []
        beep_idx = 0
        for message in messages:
            # 提示音应被插入到该时刻的最后一个 note_off 后，其它 message 之前
            while message.type != 'note_off' and beep_idx < len(beep_times) and message.time >= beep_times[beep_idx]:
                new_messages.append(MessageRepr(
                    FrozenMessage,
                    'note_on',
                    beep_times[beep_idx] + add_time,
                    note=BEEP_NOTE,
                    velocity=BEEP_VELOCITY,
                    channel=BEEP_CHANNEL,
                ))
                new_messages.append(MessageRepr(
                    FrozenMessage,
                    'note_off',
                    beep_times[beep_idx] + BEEP_LENGTH + add_time,
                    note=BEEP_NOTE,
                    velocity=BEEP_VELOCITY,
                    channel=BEEP_CHANNEL,
                ))
                add_time += BEEP_LENGTH
                beep_idx += 1
            message.time += add_time
            new_messages.append(message)
        messages = new_messages

    # 生成
    last_time = 0
    for message in messages:
        # print(message)
        # assert message.time >= last_time, f'time is negative: {message}'
        last_time, message.time = message.time, message.time - last_time
        track.append(message.to_message())

    return mid

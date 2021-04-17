from typing import List

import pandas as pd
from mido import MidiFile, MidiTrack, bpm2tempo
from mido.frozen import FrozenMessage, FrozenMetaMessage

from cluster import Part
from config import ATOMIC_TIME, BEAT_LCM


def to_mid(music: pd.DataFrame, bpm: pd.DataFrame, clusters: List[Part]) -> MidiFile:
    mid = MidiFile()
    track = MidiTrack()
    mid.tracks.append(track)
    # midi 音色表 https://blog.csdn.net/ruyulin/article/details/84103186
    PROGRAM = 0  # 乐曲音色
    BEEP_PROGRAM = 112  # 提示音音色
    CHANNEL = 0  # 乐曲通道
    BEEP_CHANNEL = 1  # 提示音通道
    BEEP_NOTE = 90  # 提示音音符
    BEEP_VELOCITY = VELOCITY = 64  # 音量
    track.append(FrozenMessage(
        'program_change',
        program=PROGRAM,
        channel=CHANNEL,
    ))
    track.append(FrozenMessage(
        'program_change',
        program=BEEP_PROGRAM,
        channel=1,
    ))
    bpm_pos = 0
    last_beats = last_beat_type = None
    TIME_RATIO = ATOMIC_TIME * 480 // 6720
    beep_times = [part.end_time for part in clusters[:-1]]
    ADD_BEEP = True
    BEEP_LENGTH = BEAT_LCM // 4
    beep_idx = 0
    last_updated_time = updated_time = 0

    for measure_id, measure in music.groupby('measure_id'):
        # print(measure_id)
        measure_id = int(measure_id)
        # 几几拍
        beats, beat_type = measure.iloc[0][['beats', 'beat_type']]
        if last_beats is None or beats != last_beats or beat_type != last_beat_type:
            track.append(FrozenMetaMessage(
                'time_signature',
                numerator=beats,
                denominator=beat_type,
            ))
            last_beats, last_beat_type = beats, beat_type
        # tempo
        if bpm_pos < len(bpm) and measure_id == bpm['measure_id'].iat[bpm_pos]:
            tempo = bpm2tempo(bpm['bpm'].iat[bpm_pos])
            track.append(FrozenMetaMessage(
                'set_tempo',
                tempo=tempo,
                time=0,
            ))
            bpm_pos += 1
        for (_, end_time), notes in measure.groupby(['start_time', 'end_time']):
            first_step_id: int = notes.iloc[0]['step_id']
            now_time: int = end_time * TIME_RATIO
            if first_step_id != -1:
                # on
                for _, note in notes.iterrows():
                    step_id: int = note['step_id']
                    if step_id == -1:
                        continue
                    track.append(FrozenMessage(
                        'note_on',
                        note=step_id + 20,
                        velocity=VELOCITY,
                        time=0,
                        channel=CHANNEL,
                    ))
                # off
                note_iter = iter(notes.iterrows())
                _, note = next(note_iter)
                last_updated_time, updated_time = updated_time, now_time
                track.append(FrozenMessage(
                    'note_off',
                    note=note['step_id'] + 20,
                    velocity=VELOCITY,
                    time=updated_time - last_updated_time,
                    channel=CHANNEL,
                ))
                for _, note in note_iter:
                    step_id: int = note['step_id']
                    if step_id == -1:
                        continue
                    track.append(FrozenMessage(
                        'note_off',
                        note=step_id + 20,
                        velocity=VELOCITY,
                        time=0,
                        channel=CHANNEL,
                    ))
            # beep
            if ADD_BEEP and beep_idx < len(beep_times) and beep_times[beep_idx] <= end_time:
                track.append(FrozenMessage(
                    'note_on',
                    note=BEEP_NOTE,
                    velocity=BEEP_VELOCITY,
                    time=0,
                    channel=BEEP_CHANNEL,
                ))
                track.append(FrozenMessage(
                    'note_off',
                    note=BEEP_NOTE,
                    velocity=BEEP_VELOCITY,
                    time=BEEP_LENGTH * TIME_RATIO,
                    channel=BEEP_CHANNEL,
                ))
                beep_idx += 1
    return mid

from contextlib import contextmanager
from functools import reduce
from itertools import chain, product
from math import gcd
from operator import mul
from typing import Any, Callable, Dict, List, Optional, Union

import pandas as pd
from pandas.io.formats.format import format_array
from pymysql.connections import Connection


def combine_weight(*weights: List[float]) -> List[float]:
    """
    合并多个权重数组
    """
    return [reduce(mul, weight) for weight in product(*weights)]


@contextmanager
def get_connection():
    from db_config import DATABASE_CONFIG
    connection = Connection(**DATABASE_CONFIG)
    try:
        yield connection
    finally:
        connection.close()


def format_mu_id(mu_id: Union[int, str]) -> str:
    if type(mu_id) is not str:
        from config import MU_ID_LEN
        mu_id = str(mu_id)
        mu_id = '0' * (MU_ID_LEN - len(mu_id)) + mu_id
    return mu_id


def get_music(mu_id: Union[int, str], conn: Connection) -> pd.DataFrame:
    """
    从数据库中获取一首音乐
    """

    from config import ATOMIC_TIME

    sql = """select n.measure_id,n.step_id,n.start_time,n.duration,m.beats,m.beat_type
                from t_music_measure m, t_music_note n
                where m.measure_id = n.measure_id
                and m.mu_id=%(mu_id)s
                and n.mu_id=%(mu_id)s
                and n.stave_id=1
                and m.part_id='P1'
                order by start_time asc"""
    music = pd.read_sql(sql, conn, params={
        'mu_id': format_mu_id(mu_id),
    })
    music['start_time'] = music['start_time'].astype('int') // ATOMIC_TIME
    music['duration'] = music['duration'].astype('int') // ATOMIC_TIME
    music['end_time'] = music['start_time'] + music['duration']
    music['step_id'] = music['step_id'].astype('int')
    music['beats'] = music['beats'].astype('int')
    music['beat_type'] = music['beat_type'].astype('int')
    music['measure_id'] = music['measure_id'].astype('int')
    return music


def simplify_fraction(numerator: int, denominator: int) -> str:
    """
    化简分数
    """
    div = gcd(numerator, denominator)
    numerator //= div
    denominator //= div
    return f'{numerator}/{denominator}' if denominator != 1 else str(numerator)


def format_note(step_id: int) -> str:
    """
    格式化音符
    参考: 国际谱 https://bideyuanli.com/p/3673
    """
    if step_id == -1:
        return 'R'
    from config import STEPS
    step_idx = (step_id - 1) % 12
    step = STEPS[step_idx]
    octave = (step_id - 1) // 12
    if step_idx >= 3:
        octave += 1
    return f'{step}{octave}/{step_id}'


def pandas_format(format: Dict[str, Any]):
    return pd.option_context(*chain(*format.items()))  # type: ignore


def left_justified(df: pd.DataFrame, columns: List[str] = None, formatter: Optional[Callable] = None, **kwargs) -> pd.DataFrame:
    result = pd.DataFrame()
    for li in (columns if columns is not None else df.columns):
        result[li] = format_array(df[li], formatter, justify='left', **kwargs)
    return result


def get_bpm(mu_id: Union[int, str], conn: Connection) -> pd.DataFrame:
    sql = """select cast(part_name as unsigned) as measure_id, if(sound_tempo = '', 76.0, cast(sound_tempo as decimal)) as bpm
                from t_music_part
                where mu_id = %(mu_id)s
                and part_id = 'P1'
                order by measure_id asc"""
    bpm = pd.read_sql(sql, conn, params={
        'mu_id': format_mu_id(mu_id),
    })
    # 补上默认 bpm
    if len(bpm) == 0 or bpm['measure_id'].iat[0] != 1:
        bpm = pd.concat((
            pd.DataFrame([{
                'measure_id': 1,
                'bpm': 76.0,
            }]),
            bpm,
        ))
    return bpm

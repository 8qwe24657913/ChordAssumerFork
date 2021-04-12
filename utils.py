from functools import reduce
from itertools import product
from operator import mul
from typing import List, Optional, Union

import pandas as pd
from pymysql.connections import Connection


def combine_weight(*weights: List[float]) -> List[float]:
    """
    合并多个权重数组
    """
    return [reduce(mul, weight) for weight in product(*weights)]


class get_connection(object):
    """
    连接数据库的封装，强制使用 with 语句获取 connection 对象
    """

    def __init__(self) -> None:
        super().__init__()
        self._connection: Optional[Connection] = None

    def __enter__(self) -> Connection:
        from db_config import DATABASE_CONFIG

        self._connection = Connection(**DATABASE_CONFIG)
        return self._connection

    def __exit__(self, type, value, trace) -> None:
        self._connection.close()
        self._connection = None


def get_music(mu_id: Union[int, str], conn: Connection) -> pd.DataFrame:
    """
    从数据库中获取一首音乐
    """

    from config import ATOMIC_TIME, MU_ID_LEN

    if type(mu_id) is not str:
        mu_id = str(mu_id)
        mu_id = '0' * (MU_ID_LEN - len(mu_id)) + mu_id
    sql = """select n.measure_id,n.step_id,n.start_time,n.duration,m.beats,m.beat_type
                from t_music_measure m, t_music_note n
                where m.measure_id = n.measure_id
                and m.mu_id=%(mu_id)s
                and n.mu_id=%(mu_id)s
                and n.stave_id=1
                and m.part_id='P1'
                order by start_time asc"""
    music = pd.read_sql(sql, conn, params={'mu_id': mu_id})
    music['start_time'] //= ATOMIC_TIME
    music['duration'] //= ATOMIC_TIME
    music['step_id'] = music['step_id'].astype('int')
    music['beats'] = music['beats'].astype('int')
    music['beat_type'] = music['beat_type'].astype('int')
    return music

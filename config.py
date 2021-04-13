from utils import combine_weight

# 各种和弦
CHORDS = {
    'maj3': [0, 4, 7],  # 大三和弦 根音-大三度-纯五度
    'min3': [0, 3, 7],  # 小三和弦 根音-小三度-纯五度
    'aug3': [0, 4, 8],  # 增三和弦 根音-大三度-增五度
    'dim3': [0, 3, 6],  # 减三和弦 根音-小三度-减五度

    # 张说是连四个音的和弦的也不用考虑了，只考虑三音和弦的就行
    # 'M7': [0, 4, 7, 11],  # 大七和弦 根音-大三度-纯五度-大七度
    # 'Mm7': [0, 4, 7, 10],  # 属七和弦 根音-大三度-纯五度-小七度
    # 'm7': [0, 3, 7, 10],  # 小七和弦 根音-小三度-纯五度-小七度
    # 'mM7': [0, 3, 7, 11],  # 小大七和弦 根音-小三度-纯五度-大七度
    # 'aug7': [0, 4, 8, 10],  # 增七和弦 根音-大三度-增五度-小七度
    # 'augM7': [0, 4, 8, 11],  # 增大七和弦 根音-大三度-增五度-小七度
    # 'm7b5': [0, 3, 6, 10],  # 半减七和弦 根音-小三度-减五度-减七度
    # 'dim7': [0, 3, 6, 9],  # 减减七和弦 根音-小三度-减五度-减七度
}

# 所有 beat_type 的最小公倍数
BEAT_LCM = 32  # 32 分音符

_WEIGHT_1 = [1.0]
_WEIGHT_2 = [0.6, 0.4]
_WEIGHT_3 = [0.45, 0.3, 0.25]
_WEIGHT_4 = [0.4, 0.2, 0.3, 0.1]
_WEIGHT_6 = combine_weight(_WEIGHT_2, _WEIGHT_3)
_WEIGHT_8 = combine_weight(_WEIGHT_4, _WEIGHT_2)

# 每个 beat_type 分音符的权重
WEIGHT = {
    '1/4': _WEIGHT_1,
    '4/4': _WEIGHT_4,
    '3/4': _WEIGHT_3,
    '2/4': _WEIGHT_2,
    '3/8': _WEIGHT_3,
    '6/8': _WEIGHT_6,
}

# 将 beat_type 分音符拆分为 BEAT_LCM 分音符时如何赋予权重
# 要求 beat_type * 数组长度 == BEAT_LCM
WEIGHT_B_INNER = {
    4: _WEIGHT_8,
    8: _WEIGHT_4,
}

# 全音符时长
FULL_NOTE_TIME = 26880

# BEAT_LCM 分音符的时长
ATOMIC_TIME = FULL_NOTE_TIME // BEAT_LCM

# mu_id 的字符串长度（为什么要用 varchar 存 mu_id ？？？）
MU_ID_LEN = 6

# 各音高的字符串表示
STEPS = ['A', 'A#', 'B', 'C', 'C#', 'D', 'D#', 'E', 'F', 'F#', 'G', 'G#']

# 允许聚类的距离
TOLERANCE_CLUSTER_LENGTH = [16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]

assert BEAT_LCM % len(
    TOLERANCE_CLUSTER_LENGTH) == 0, 'BEAT_LCM should divide len(TOLERANCE_CLUSTER_LENGTH)'

# 每个聚类的长度
CATEGORY_LEN = BEAT_LCM // len(TOLERANCE_CLUSTER_LENGTH)
TOLERANCE_CLUSTER_LENGTH = [l * CATEGORY_LEN for l in TOLERANCE_CLUSTER_LENGTH]

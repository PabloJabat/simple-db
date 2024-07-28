class DbExistsError(Exception):
    pass


class DbCorruption(Exception):
    pass


class SegmentSizeError(Exception):
    pass


class TableException(Exception):
    pass

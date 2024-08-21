class DbExistsError(Exception):
    pass


class DbCorruption(Exception):
    pass


class TableExistsError(Exception):
    pass


class TableException(Exception):
    pass


class PartitionExistsError(Exception):
    pass


class SegmentExistsError(Exception):
    pass


class SegmentSizeError(Exception):
    pass

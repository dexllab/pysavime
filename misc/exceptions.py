class UnknownFailure(Exception):

    def __init__(self, message):
        super().__init__(message)


class ConnectionFailure(Exception):

    def __init__(self, message):
        super().__init__(message)


class QueryHandleFailure(Exception):
    def __init__(self, message):
        super().__init__(message)


class MMapFailed(Exception):
    def __init__(self, message):
        super().__init__(message)


class SavimeSilentError(Exception):
    def __init__(self, message):
        super().__init__(message)

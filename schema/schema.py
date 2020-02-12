from abc import ABC, abstractmethod

from savime.datatype import SavimeSupportedTypes


class ReprMixIn:
    """
    Adds generic functionality for printing string representations.
    """

    def __repr__(self):
        if len(self.__slots__) > 0:
            attrs_str = ', '.join('{}={!r}'.format(attr, self.__getattribute__(attr)) for attr in self.__slots__)
        else:
            attrs_str = ', '.join('{}={!r}'.format(k, v) for k, v in self.__dict__.items())

        return f'{self.__class__.__name__}({attrs_str})'


class SavimeElement(ABC, ReprMixIn):
    """
    Interface for a SAVIME element, e.g., datasets, tars, subtars, and so on.
    """

    @abstractmethod
    def plain_query_str(self) -> str:
        """
        Interface for creating a query snippet for the element. Note this snippet is not necessary a runnable query.

        :return: A query snippet for the element.
        """
        pass


class CreatableSavimeElement(SavimeElement):
    """
    Interface for SAVIME creatable elements, e.g., datasets and tars.
    """

    @abstractmethod
    def create_query_str(self) -> str:
        """
        Interface for CREATE queries. The returned query must be runnable.

        :return: A CREATE query.
        """
        pass


class DroppableSavimeElement(SavimeElement):
    """
    Interface for SAVIME droppable elements: tars, types, and datasets.
    """

    @abstractmethod
    def drop_query_str(self) -> str:
        """
        Interface for DROP queries. The returned query must be runnable.

        :return: A DROP query.
        """
        pass


class LoadableSavimeElement(SavimeElement):
    """
    Interface for SAVIME loadable elements, e.g., subtars.
    """

    @abstractmethod
    def load_query_str(self) -> str:
        """
        Interface for LOAD queries. The returned query must be runnable.

        :return: A LOAD query.
        """
        pass


class Literal(SavimeElement):
    """
    Defines a SAVIME literal which is an array of primitive data type elements.

    Examples:
        >>> # To create a list [1, 2, 3, 4]:
        >>> Literal([1, 2, 3, 4], SavimeSupportedTypes.INT)

    Attributes:
        values: List of values defining the literal.
        data_type: Supported Savime d_type.
    """

    __slots__ = ('values', 'data_type')

    def __init__(self, values: list, data_type: SavimeSupportedTypes):
        self.values = values
        self.data_type = data_type

    def plain_query_str(self):
        include_double_quotes = True if self.data_type == SavimeSupportedTypes.CHAR else False
        values_str = ','.join(f'"{value}"' if include_double_quotes else str(value) for value in self.values)
        q = f'literal({values_str})'
        return q


class Range(SavimeElement):
    """
    Defines an increasing range of values.

    Examples:
        >>> # To create a range (1, 3, 5, 1, 3, 5):
        >>> Range(1, 5, 2, 2)

    Attributes:
        start: The first element in the range.
        stop: The last element within the range.
        step: The distance between two successive elements within the range.
        num_repetitions: The number of times the range is repeated.
    """

    def __init__(self, start: int, stop: int, step: int, num_repetitions: int = 1):
        self.start = start
        self.stop = stop
        self.step = step
        self.num_repetitions = num_repetitions

        assert 0 <= self.start < self.stop, 'The start must be greater or equal to 0 and less than the stop.'
        assert 0 < self.num_repetitions, 'The number of repetition must be greater than 0.'

    def plain_query_str(self):
        q = f'"{self.start}:{self.step}:{self.stop}:{self.num_repetitions}"'
        return q


class IntervalRange(ReprMixIn):
    """
    Defines an interval range.

    Examples:
        >>> # To define an integer interval between 1 and 9 spaced every 2 numbers: [1, 3, 5, 7, 9]
        >>> IntervalRange(1, 9, 2)

    Attributes:
        start: The interval start.
        stop: The interval stop.
        step: The space between values within the interval.
    """

    __slots__ = 'start', 'stop', 'step'

    def __init__(self, start, stop, step=1):
        self.start = start
        self.stop = stop
        self.step = step


class IndexRange(ReprMixIn):
    """
    Defines a continuous index range. Note that it just defines the boundaries of the interval and has nothing to do with the
    manner the values are spaced within the interval.

    Examples:
        >>> # To define a physical index range between 1 and 9.
        >>> IndexRange(1, 9, True)
    """

    __slots__ = 'start', 'stop', 'is_physical'

    def __init__(self, start, stop, is_physical: bool):
        self.start = start
        self.stop = stop
        self.is_physical = is_physical

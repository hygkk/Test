from enum import Enum, unique


@unique
class StringEnum(Enum):

    @classmethod
    def turn2this(cls, x):
        if isinstance(x, str):
            return cls[x]
        else:
            assert isinstance(x, cls)
            return x
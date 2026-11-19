from typing import Annotated

from pydantic import AfterValidator, Field, StringConstraints

INT32_MAX = 2_147_483_647

Code = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        max_length=64,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$",
    ),
]


def _no_nul(value: str) -> str:
    if "\x00" in value:
        raise ValueError("must not contain NUL characters")
    return value


Label = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=200),
    AfterValidator(_no_nul),
]

NonNegativeInt32 = Annotated[int, Field(strict=True, ge=0, le=INT32_MAX)]

Quantity = Annotated[int, Field(strict=True, ge=1, le=10_000)]

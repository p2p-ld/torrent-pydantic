import pytest

__all__ = ["version"]


@pytest.fixture(
    params=[
        pytest.param("v1", id="v1", marks=pytest.mark.v1),
        pytest.param("v2", id="v2", marks=pytest.mark.v2),
        pytest.param("hybrid", id="hybrid", marks=pytest.mark.hybrid),
    ],
)
def version(request: pytest.FixtureRequest) -> str:
    return request.param

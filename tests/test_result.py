from dagrattler import Err, Ok, ensure_exception


def test_ok_err_basics() -> None:
    ok = Ok(3)
    err = Err(ValueError("bad"))

    assert ok.value == 3
    assert isinstance(err.error, ValueError)


def test_ensure_exception_converts_base_exception() -> None:
    exc = ensure_exception(KeyboardInterrupt())

    assert isinstance(exc, Exception)
    assert str(exc) == ""

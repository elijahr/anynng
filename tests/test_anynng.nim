import unittest
import anynng

suite "anynng":
  test "hello returns expected greeting":
    check hello() == "hello from anynng"

  test "version present":
    check projectVersion == "0.0.1"

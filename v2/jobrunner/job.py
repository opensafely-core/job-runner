# TODO: Make this an Enum once we sort of the serialization/deserialization in
# the database layer
class State:
    PENDING = "P"
    RUNNING = "R"
    FAILED = "F"
    COMPLETED = "C"

from datetime import date, datetime
from ipaddress import IPv4Address
from typing import (
    Any,
    Mapping,
)
from uuid import UUID

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

serializer = TypeSerializer()
deserializer = TypeDeserializer()


def _serialize_to_basic_types(data: Any) -> str | dict | set | list:
    match data:
        case datetime():
            return data.isoformat()

        case date():
            return data.isoformat()

        case UUID():
            return str(data)

        case IPv4Address():
            return str(data)

        case tuple() | list():
            if not data:
                return []

            serialized = [_serialize_to_basic_types(v) for v in data]

            if any(isinstance(v, (dict, list)) for v in serialized):
                return serialized

            try:
                return set(serialized)
            except TypeError:
                return serialized

        case set():
            if not data:
                return []

            return set(_serialize_to_basic_types(v) for v in data)

        case dict():
            return {k: _serialize_to_basic_types(v) for k, v in data.items()}

        case _:
            return data


def serialize(data: Mapping[str, Any]) -> dict:
    return {
        k: serializer.serialize(_serialize_to_basic_types(v)) for k, v in data.items()
    }


def deserialize(data: Mapping[str, Any]) -> dict:
    return {k: deserializer.deserialize(v) for k, v in data.items()}

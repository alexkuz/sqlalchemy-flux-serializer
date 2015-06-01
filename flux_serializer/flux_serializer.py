# -*- coding: utf-8 -*-

"""Flux Serializer

The goal of this serializer is to generate dict with flat entities list,
which can be naturally integrated into flux stores on the client.

This way you can avoid redudant results in api response
and even redudant SQL queries.

"""
from datetime import datetime
from sqlalchemy.orm.properties import RelationshipProperty, ColumnProperty
from .flask_metadata_provider import FlaskMetadataProvider
from .objects import Dependency


_serializers = {}


def _get_value(model, name, rel_map):
    value = getattr(model, rel_map.get(name, name))

    if isinstance(value, datetime):
        return value.isoformat() + 'Z'
    else:
        return value


def _get_default_columns(model):
    """
    @summary:
    """
    relationships = {
        next(iter(r.local_columns)).key: r.key
        for r in model.__mapper__.relationships
    }

    return set([
        relationships.get(c.key, c.key)
        for c in model.__mapper__.iterate_properties
    ])


class FluxSerializer(object):
    """
    @summary:
    """
    primary_id = None

    def __init__(self, primary_id=None, columns=None):
        self.columns = columns
        self.primary_id = primary_id
        self.model_cls = None
        self.rel_map = None

    @staticmethod
    def get_serializer(model):
        """
        @summary:
        """
        serializer = _serializers.get(model)
        if not serializer:
            serializer = FluxSerializer()
            FluxSerializer.register_serializer(
                serializer, model
            )
        serializer._activate()
        return serializer

    @staticmethod
    def register_serializer(serializer, cls):
        """
        @summary:
        """
        serializer.set_model_cls(cls)
        _serializers[cls] = serializer

    def set_model_cls(self, model_cls):
        """
        @summary:
        """
        self.model_cls = model_cls

    def _activate(self):
        """
        @summary:
        """
        if self.rel_map is None:
            self.rel_map = {
                r.key: next(iter(r.local_columns)).key
                for r in self.model_cls.__mapper__.relationships
            }

    metadata_provider = FlaskMetadataProvider

    def get_api_model_name(self):
        """
        @summary:
        """
        return self.model_cls.__tablename__

    def _get_dep_ids(self, rel, ids):
        """
        @summary:
        """
        primary_id = self.get_primary_id(self.model_cls)
        column = next(iter(rel.local_columns))
        rel_class = rel.mapper.class_
        serializer = self.get_serializer(rel_class)
        rel_primary_id = serializer.get_primary_id(rel_class)

        if rel.mapper.primary_key[0] != rel_primary_id:
            query = self.model_cls.query.join(
                rel.class_attribute
            ).with_entities(
                rel_primary_id.label('id')
            ).filter(primary_id.in_(ids))
        else:
            query = self.model_cls.query.with_entities(
                column.label('id')
            ).filter(primary_id.in_(ids))

        return [row.id for row in query]

    def get_dependency_data(self, ids, include_columns, level, opts):
        """
        @summary:
        """
        model_name = self.get_api_model_name()
        columns = list(self.columns or [])
        if include_columns and include_columns.get(model_name):
            columns = list(set(columns + list(include_columns[model_name])))

        dep_data = {
            '__relationships': self._get_relationships(columns)
        }
        return dep_data

    def _get_relationships(self, columns):
        """
        @summary:
        """
        columns = columns or []
        props = self.model_cls.__mapper__.iterate_properties
        return [
            r for r in props
            if (r.key in columns and not isinstance(r, ColumnProperty)) or
            (not columns and isinstance(r, RelationshipProperty))
        ]

    def prepare_dependencies(self, ids, dep_data, level, opts):
        """
        @summary:
        """
        return [
            Dependency(
                model=rel.mapper.class_,
                ids=self._get_dep_ids(rel, ids)
            )
            for rel in dep_data.get('__relationships') or []
        ] if isinstance(dep_data, dict) else []

    def get_preloaded(self, ids, dep_data, level, opts):
        """
        @summary:
        """
        pass

    def serialize(self, model, preloaded, is_first_level_entity,
                  include_columns, opts):
        """
        @summary:
        """
        columns = list(self.columns or _get_default_columns(model))

        if include_columns:
            columns = list(set(columns + list(include_columns)))

        return {name: _get_value(model, name, self.rel_map)
                for name in columns}

    def fetch_entities(self, entity_ids, opts):
        """
        @summary:
        """
        return self.model_cls.query.filter(
            self.get_primary_id(self.model_cls).in_(entity_ids)
        )

    def get_primary_id(self, entity):
        """
        @summary:
        """
        primary_id = self.primary_id or entity.__mapper__.primary_key[0].key
        return getattr(entity, primary_id)



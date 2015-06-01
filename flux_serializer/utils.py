import collections
from .flux_serializer import FluxSerializer
from .objects import EntityObject, ApiResult, ApiObject

DEPTH_LIMIT = 20

def _merge_entities(target, source):
    """
    @summary:
    """
    result = {}
    result.update(target)
    result.update({
        key: EntityObject(
            serializer=obj.serializer,
            model=obj.model,
            preloaded=obj.preloaded
                      if not key in target
                      else deep_update(target[key].preloaded,
                                       obj.preloaded or {}),
            ids=list(obj.ids)
                if not key in target
                else list(set(target[key].ids + obj.ids))
        )
        for key, obj in source.iteritems()
    })
    return result


def deep_update(target, source):
    for key, val in source.iteritems():
        target[key] = (deep_update(target.get(key, {}), val)
                       if isinstance(val, collections.Mapping)
                       else source[key])
    return target


def _prepare(cls, ids, entities, preloaded, include_columns, level, opts=None):
    """
    @summary:
    """
    ids = set(ids)

    if level > DEPTH_LIMIT:
        raise Exception("Prepare recursive loop goes too deep")

    serializer = FluxSerializer.get_serializer(cls)
    model_name = serializer.get_api_model_name()

    if model_name in entities:
        ids = ids - set(entities[model_name].ids)

    ids = list(ids)

    if not ids:
        return entities

    dep_data = serializer.get_dependency_data(ids, include_columns,
                                              level, opts)

    entities = _merge_entities(entities, {
        model_name: EntityObject(
            model=cls,
            ids=ids,
            preloaded=deep_update(
                preloaded or {},
                serializer.get_preloaded(ids, dep_data, level, opts) or {}),
            serializer=serializer
        )
    })

    deps = serializer.prepare_dependencies(ids, dep_data, level, opts)

    if deps:
        for dep in deps:
            if not dep.ids:
                continue
            entities = _prepare(dep.model, dep.ids, entities, preloaded,
                                include_columns, level + 1, opts)

    return entities


def _get_api_object(cls, prepared, entity_ids, include_columns, opts):
    """
    @summary:
    """
    result_entities = {}
    result_type = None
    result_objects = None

    for name, data in prepared.iteritems():
        model = data.model
        preloaded = data.preloaded
        serializer = data.serializer
        entities = serializer.fetch_entities(data.ids, opts)

        first_level_entity_ids = entity_ids if cls == model else []

        include_model_columns = include_columns and include_columns.get(name)

        result_entities[name] = {
            serializer.get_primary_id(entity):
                _serialize_entity(entity, serializer,
                                  preloaded, first_level_entity_ids,
                                  include_model_columns, opts)
            for entity in entities
            if serializer.get_primary_id(entity) in data.ids
        }

        if cls == model:
            result_type = name
            result_objects = [entity_id for entity_id in entity_ids
                              if entity_id in result_entities[name]]

    return ApiObject(
        result=ApiResult(
            type=result_type,
            objects=result_objects
        ),
        entities=result_entities
    )


def _serialize_entity(entity, serializer, preloaded, first_level_ids,
                      include_columns, opts):
    """
    @summary:
    """
    primary_id = serializer.get_primary_id(entity)
    is_first_level_entity = primary_id in first_level_ids
    preloaded_entity = preloaded and preloaded.get(primary_id)
    return serializer.serialize(entity, preloaded_entity,
                                is_first_level_entity, include_columns,
                                opts)


def _serialize(model_cls, ids, include_columns, opts):
    """
    @summary:
    """
    prepared = _prepare(model_cls, ids, {}, opts, include_columns, level=0)

    return _get_api_object(model_cls, prepared, ids, include_columns, opts)


def _to_dict(api_object):
    """
    @summary:
    """
    return {
        "entities": api_object.entities,
        "result": api_object.result._asdict()
    }


def serialize_list(ids, model_cls, include_columns=None, opts=None):
    """
    @summary:
    """
    return _to_dict(_serialize(model_cls, ids, include_columns, opts))


def _get_ids_from_query(query, model_cls, primary_id, secondary_relation):
    """
    @summary:
    """
    ids = [m.id for m in query
           .with_entities(primary_id.label('id'))]

    if secondary_relation:
        id_map = {row.from_id: row.to_id for row in
                  model_cls.query.with_entities(
                    primary_id.label('from_id'),
                    secondary_relation.label('to_id')
                  )
                  .filter(primary_id.in_(ids))}
        ids = [id_map[model_id] for model_id in ids]

    return ids

def serialize_query(query, order_by=None, model_cls=None,
                    secondary_relation=None,
                    include_columns=None, opts=None):
    """
    @summary:
    """
    if not model_cls:
        model_cls = query._primary_entity.type
    serializer = FluxSerializer.get_serializer(model_cls)
    primary_id = serializer.get_primary_id(model_cls)

    query = _get_sorted_query(order_by, query, primary_id, model_cls, 0)

    ids = _get_ids_from_query(query, model_cls, primary_id, secondary_relation)
    if secondary_relation:
        model_cls = secondary_relation.__mapper__.class_
        # if 'get_id_map' in dir(model_cls) and ids:
        #     id_map = model_cls.get_id_map(ids)
        #     ids = [id_map[model_id] for model_id in ids]

    return _to_dict(_serialize(model_cls, ids, include_columns, opts))


def serialize_model(model, include_columns=None, opts=None):
    """
    @summary:
    """
    serializer = FluxSerializer.get_serializer(model.__class__)
    ids = [serializer.get_primary_id(model)]
    return _to_dict(_serialize(model.__class__, ids, include_columns, opts))


def _get_sorted_query(order_by, query, primary_id, model_cls, after_id=0):
    """
    @summary:
    """
    from sqlalchemy.sql.elements import UnaryExpression

    desc = False

    if order_by is not None:
        if isinstance(order_by, UnaryExpression):
            desc = order_by.modifier.func_name == 'desc_op'
            order_by = order_by.element

        order_clause = (
            order_by.desc() if desc else order_by,
            primary_id.desc() if desc else primary_id,
        )
    else:
        order_by = primary_id
        order_clause = (primary_id,)

    if not after_id or order_by == primary_id:
        return query.order_by(*order_clause)

    if str(primary_id.type) == 'INTEGER':
        after_id = int(after_id)

    after_value_query = (
        model_cls.query.with_entities(order_by)
        .filter(primary_id == after_id)
    )

    return query.order_by(
        after_id != primary_id,
        *order_clause
    ).filter(
        order_by <= after_value_query if desc
        else order_by >= after_value_query
    )

def serialize_paginated_query(query, order_by=None, model_cls=None,
                              secondary_relation=None, per_page=24,
                              include_columns=None, opts=None):
    """
    @summary:
    """
    if not model_cls:
        model_cls = query._primary_entity.type
    serializer = FluxSerializer.get_serializer(model_cls)

    after_id = serializer.metadata_provider.get_after_id()
    primary_id = serializer.get_primary_id(model_cls)

    query = _get_sorted_query(order_by, query, primary_id, model_cls, after_id)

    next_id = query.with_entities(
        primary_id.label('id')
    ).offset(per_page).first()

    ids = _get_ids_from_query(query.limit(per_page), model_cls,
                              primary_id, secondary_relation)

    if secondary_relation:
        model_cls = secondary_relation.__mapper__.class_

    response = _to_dict(_serialize(model_cls, ids, include_columns, opts))

    if next_id:
        meta = serializer.metadata_provider.get_metadata(next_id.id)
        response['result']['meta'] = meta

    return response


def register_serializer(serializer=None):
    """
    @summary:
    """
    def decorator(cls):
        """
        @summary:
        """
        _serializer = serializer
        if not serializer:
            _serializer = FluxSerializer()
        elif isinstance(serializer, type):
            _serializer = serializer()

        FluxSerializer.register_serializer(_serializer, cls)
        return cls

    return decorator

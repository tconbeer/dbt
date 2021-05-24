
{% materialization incremental, default -%}

  {% set unique_key = config.get('unique_key') %}

  {% set target_relation = this.incorporate(type='table') %}
  {% set existing_relation = load_relation(this) %}
  {%- set full_refresh_mode = (should_full_refresh()) -%}

  {% set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [] %}

  {# -- first check whether we want to full refresh for source view or config reasons #}
  {% set trigger_full_refresh = (full_refresh_mode or existing_relation.is_view) %}
  {% do log('full refresh mode: %s' % trigger_full_refresh) %}
  
  {# -- double check whether full refresh should happen if on_schema_change config is True #}
  {% if not trigger_full_refresh and on_schema_change == 'full_refresh' %}
    {%- set tmp_relation = make_temp_relation(target_relation) -%}
    {%- do run_query(create_table_as(True, tmp_relation, sql)) -%}

    {%- set schema_changed = check_for_schema_changes(tmp_relation, target_relation) -%}
    {%- if schema_changed -%}
      {%- set trigger_full_refresh = True -%}
      {%- set sql = adapter.incremental_remove_where(sql) %}
      {%- do log('detected a schema change with on_schema_change == full_refresh, refreshing table', info=true) -%}

    {%- endif -%}

    {# -- BELOW CODE ALSO WORKS #}
    {# -- set source_columns = get_columns_in_query(sql) -#}
    {# -- set target_columns = get_column_names(adapter.get_columns_in_relation(target_relation)) -#}
    {# -- set source_not_in_target = diff_arrays(source_columns, target_columns) -#}
    {# -- set target_not_in_source = diff_arrays(target_columns, source_columns) -#}
    {# -- if source_not_in_target|length > 0 or target_not_in_source|length > 0 -#}
      {# -- set trigger_full_refresh = True -#}
      {# -- this removes the WHERE clause from the input model sql in the event a full refresh is needed #}
      {# -- set sql = adapter.incremental_remove_where(sql) #}
      {# -- do log('detected a schema change with on_schema_change == full_refresh, refreshing table') -#}
    {# -- endif -#}

  {% endif %}

  {% if existing_relation is none %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
  
  {% elif trigger_full_refresh %}
      {#-- Make sure the backup doesn't exist so we don't encounter issues with the rename below #}
      {% do log('running full refresh procedure', info=true) %}
      {% set tmp_identifier = model['name'] + '__dbt_tmp' %}
      {% set backup_identifier = model['name'] + '__dbt_backup' %}

      {% set intermediate_relation = existing_relation.incorporate(path={"identifier": tmp_identifier}) %}
      {% set backup_relation = existing_relation.incorporate(path={"identifier": backup_identifier}) %}

      {% do adapter.drop_relation(intermediate_relation) %}
      {% do adapter.drop_relation(backup_relation) %}

      {% set build_sql = create_table_as(False, intermediate_relation, sql) %}
      {% set need_swap = true %}
      {% do to_drop.append(backup_relation) %}
  
  {% else %}
    {% set tmp_relation = make_temp_relation(target_relation) %}
    {% do run_query(create_table_as(True, tmp_relation, sql)) %}
    
    {% if on_schema_change not in ('ignore', 'full_refresh') %}
      {% set schema_changed = check_for_schema_changes(tmp_relation, target_relation) %}
      {% do log('schema changed: %s' % schema_changed, info=true) %}
      {% if schema_changed %}
        {% do process_schema_changes(on_schema_change, tmp_relation, existing_relation) %}
        {% set build_sql = incremental_upsert(tmp_relation, target_relation, unique_key=unique_key) %}
      
      {% else %}
        {% do adapter.expand_target_column_types(
             from_relation=tmp_relation,
             to_relation=target_relation) %}
        {% set build_sql = incremental_upsert(tmp_relation, target_relation, unique_key=unique_key) %}
      {% endif %}
    
    {% else %}
      {% do adapter.expand_target_column_types(
             from_relation=tmp_relation,
             to_relation=target_relation) %}
      {% set build_sql = incremental_upsert(tmp_relation, target_relation, unique_key=unique_key) %}
    {% endif %}
  
  {% endif %}

  {% call statement("main") %}
      {{ build_sql }}
  {% endcall %}

  {% if need_swap %} 
      {% do adapter.rename_relation(target_relation, backup_relation) %} 
      {% do adapter.rename_relation(intermediate_relation, target_relation) %} 
  {% endif %}

  {% do persist_docs(target_relation, model) %}

  {% if existing_relation is none or existing_relation.is_view or should_full_refresh() %}
    {% do create_indexes(target_relation) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}

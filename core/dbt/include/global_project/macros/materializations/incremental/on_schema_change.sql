{% macro incremental_validate_on_schema_change(on_schema_change, default='ignore') %}
   
   {% if on_schema_change not in ['sync_all_columns', 'append_new_columns', 'fail', 'ignore'] %}
     
     {% set log_message = 'invalid value for on_schema_change (%s) specified. Setting default value of %s.' % (on_schema_change, default) %}
     {% do log(log_message, info=true) %}
     
     {{ return(default) }}

   {% else %}
     {{ return(on_schema_change) }}
   
   {% endif %}

{% endmacro %}

{% macro incremental_validate_alter_column_types(alter_column_types, default=False) %}
   
   {% if alter_column_types not in [True, False] %}
     
     {% set log_message = 'invalid value for alter_column_types (%s) specified. Setting default value of %s.' % (alter_column_types, default) %}
     {% do log(log_message, info=true) %}

     {{ return(default) }}

   {% else %}
     {{ return(alter_column_types) }}
   
   {% endif %}

{% endmacro %}

{% macro get_column_names(columns) %}
  
  {# -- this needs the | list or comparisons downstream don't work against the generators which come out of map() #}
  {% set result = columns | map(attribute = 'column') | list %}

  {{ return(result) }}

{% endmacro %}

{% macro diff_columns(source_columns, target_columns) %}

  {% set result = [] %}
  {% set source_names = get_column_names(source_columns) %}
  {% set target_names = get_column_names(target_columns) %}
   
   {# --check whether the name attribute exists in the target - this does not perform a data type check #}
   {% for sc in source_columns %}
     {% if sc.name not in target_names %}
        {{ result.append(sc) }}
     {% endif %}
   {% endfor %}
  
  {{ return(result) }}

{% endmacro %}

{% macro diff_column_data_types(source_columns, target_columns) %}
  
  {% set result = [] %}
  {% for sc in source_columns %}
    {% set tc = target_columns | selectattr("name", "equalto", sc.name) | list | first %}
    {% if tc %}
      {% if sc.data_type != tc.data_type %}
        {{ result.append( { 'column_name': tc.name, 'new_type': sc.data_type } ) }} 
      {% endif %}
    {% endif %}
  {% endfor %}

  {{ return(result) }}

{% endmacro %}

{% macro check_for_schema_changes(source_relation, target_relation) %}
  
  {% set schema_changed = False %}
  
  {%- set source_columns = adapter.get_columns_in_relation(source_relation) -%}
  {%- set target_columns = adapter.get_columns_in_relation(target_relation) -%}
  {%- set source_not_in_target = diff_columns(source_columns, target_columns) -%}
  {%- set target_not_in_source = diff_columns(target_columns, source_columns) -%}
  
  {% set new_target_types = diff_column_data_types(source_columns, target_columns) %}

  {% if source_not_in_target != [] %}
    {% set schema_changed = True %}
  {% elif target_not_in_source != [] or new_target_types != [] %}
    {% set schema_changed = True %}
  {% elif new_target_types != [] %}
    {% set schema_changed = True %}
  {% endif %}

  {% do log('schema changed: %s' % schema_changed, info=true) %}
  {% do log('source_not_in_target: %s' % source_not_in_target, info=true) %}
  {% do log('target_not_in_source: %s' % target_not_in_source, info=true) %}

  {{ 
    return({
      'schema_changed': schema_changed,
      'source_not_in_target': source_not_in_target,
      'target_not_in_source': target_not_in_source,
      'new_target_types': new_target_types
    }) 
  }}

{% endmacro %}

{% macro sync_schemas(on_schema_change, alter_column_types, target_relation, schema_changes_dict) %}

  {%- set add_to_target_arr = schema_changes_dict['source_not_in_target'] -%}
  {%- set remove_from_target_arr = schema_changes_dict['target_not_in_source'] -%}
  {%- set new_target_types = schema_changes_dict['new_target_types'] -%}

  {%- if on_schema_change == 'append_new_columns'-%}
     {%- if add_to_target_arr | length > 0 -%}
       {%- do alter_relation_add_columns(target_relation, add_to_target_arr) -%}
     {%- endif -%}
  
  {% elif on_schema_change == 'sync_all_columns' %}
     {% if add_to_target_arr | length > 0 %} 
       {%- do alter_relation_add_columns(target_relation, add_to_target_arr) -%}
     {% endif %}

     {% if remove_from_target_arr | length > 0 %}
       {%- do alter_relation_drop_columns(target_relation, remove_from_target_arr) -%}
     {% endif %}

     {% if alter_column_types == True and new_target_types != [] %}
       {% for ntt in new_target_types %}
         {% do log(ntt, info=true) %}
         {% set column_name = ntt['column_name'] %}
         {% set new_type = ntt['new_type'] %}
         {% do alter_column_type(target_relation, column_name, new_type) %}
       {% endfor %}
     {% endif %}
  
  {% endif %}

  {{ 
      return(
             {
              'columns_added': add_to_target_arr,
              'columns_removed': remove_from_target_arr,
              'data_types_changed': new_target_types
             }
          )
  }}
  
{% endmacro %}

{% macro process_schema_changes(on_schema_change, alter_column_types, target_relation, schema_changes_dict) %}
      
    {% if on_schema_change=='fail' %}
      
      {{ 
        exceptions.raise_compiler_error("The source and target schemas on this incremental model are out of sync!
             You can specify one of ['fail', 'ignore', 'append_new_columns', 'sync_all_columns'] in the on_schema_change config to control this behavior.
             Please re-run the incremental model with full_refresh set to True to update the target schema.
             Alternatively, you can update the schema manually and re-run the process.") 
      }}
    
    {# -- unless we ignore, run the sync operation per the config #}
    {% else %}
      
      {% set schema_changes = sync_schemas(on_schema_change, alter_column_types, target_relation, schema_changes_dict) %}
      {% set columns_added = schema_changes['columns_added'] %}
      {% set columns_removed = schema_changes['columns_removed'] %}
      {% set data_types_changed = schema_changes['data_types_changed'] %}

      {% if on_schema_change == 'append_new_columns' %}
        {% do log('columns added: ' + columns_added|join(', '), info=true) %}
      
      {% elif on_schema_change == 'sync_all_columns' %}
        {% do log('columns added: ' + columns_added|join(', '), info=true) %}
        {% do log('columns removed: ' + columns_removed|join(', '), info=true) %}
        {% do log('data types changed: ' + data_types_changed|join(', '), info=true) %}
      
      {% endif %}
      
    
    {% endif %}

{% endmacro %}
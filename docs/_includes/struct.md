{% if include.struct.name %}
# {{ include.struct.name }}
{% endif %}

{% if include.struct.description %}
{{ include.struct.description }}
{% endif %}

{% if include.struct.fields %}
## Structure
<table class="bp3-html-table bp3-html-table-striped" style="width: 100%">
  <thead>
    <tr>
      <td>
        Field
      </td>
      <td>
        Type
      </td>
      <td>
        Description
      </td>
    </tr>
  </thead>
  <tbody>
    {% for field in include.struct.fields %}
    <tr>
      <td>
        <code>{{field.name}}</code>
      </td>
      <td>
        <code>{{field.type | escape }}</code>
      </td>
      <td>
        {{field.description}}
      </td>
    </tr>
    {% endfor %}
  </tbody>
</table>
{% endif %}

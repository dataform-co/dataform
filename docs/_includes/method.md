# {{ include.method.name }}

{% if include.method.description %}
{{ include.method.description }}
{% endif %}

```js
{% if include.context %}${{"{"}}{% endif %}{{ include.method.signature }}{% if include.context %}}{% endif %}
```

{% if include.method.args %}
## Arguments
<table class="bp3-html-table bp3-html-table-striped" style="width: 100%">
  <thead>
    <tr>
      <td>
        Name
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
    {% for arg in include.method.args %}
    <tr>
      <td>
        <code>{{arg.name}}</code>
      </td>
      <td>
        {{arg.type}}
      </td>
      <td>
        {{arg.description}}
      </td>
    </tr>
    {% endfor %}
  </tbody>
</table>
{% endif %}

{% if include.method.returns %}
## Returns
{{ include.method.returns }}
{% endif %}

<#escape x as x?html>
<html>
  <body>

    <h1>${row.recordId}</h1>

    <h2>Family "data" columns</h2>

    <table border="1">
      <thead>
        <tr>
          <th></th>
          <th></th>
          <th colspan="${row.allVersionsLength}">
            Versions
          </th>
        </tr>
        <tr>
          <th>Category</th>
          <th>Name</th>

          <#list row.allVersions as version>
            <th>${version}</th>
          </#list>
        </tr>
      </thead>
      <tbody>

        <#list row.systemFields.names as sysFieldName>
          <tr>
            <td>System Field</td>
            <td>${sysFieldName}</td>
            <#list row.allVersions as vv>
              <#if row.systemFields.getValue(sysFieldName, vv)??>
                <td>${row.systemFields.getValue(sysFieldName, vv)?string}<br/></td>
              <#else>
                <td><br/></td>
              </#if>
            </#list>
          </tr>
        </#list>

        <#list row.fields.fieldTypes as fieldType>
          <tr>
            <td>Field</td>
            <td>
              ${fieldType.id}
              <br/>
              ${fieldType.name}
              <br/>
              ${fieldType.valueType.name}
              <br/>
              ${fieldType.scope}
            </td>
            <#list row.allVersions as version>
              <td>
                <#if row.fields.isDeleted(version, fieldType.id)>
                    <i>deleted marker</i>
                <#elseif row.fields.isNull(version, fieldType.id)>
                    <br/>
                <#elseif fieldType.valueType.multiValue>
                    <#list row.fields.getValue(version, fieldType.id) as item>
                      ${item}<br/>
                    </#list>
                <#else>
                    ${row.fields.getValue(version, fieldType.id)}
                </#if>
              </td>
            </#list>
          </tr>
        </#list>

        <#list row.unknownColumns as unknownColumn>
          <tr>
            <td>Unknown column</td>
            <td>${unknownColumn}</td>
          </tr>
        </#list>

      </tbody>
    </table>

    <h2>Unknown or untreated column families</h2>
    <#if row.unknownColumnFamilies?has_content>
        <ul>
          <#list row.unknownColumnFamilies as item>
            <li>${item}</li>
          </#list>
        </ul>
    <#else>
        None
    </#if>
  </body>

</html>
</#escape>
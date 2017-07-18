const tedious = require('tedious')
const { Observable } = require('rxjs')

const connect = (config) => new Promise((resolve, reject) => {
  const connection = new tedious.Connection(config)
  connection.close = connection.close.bind(connection)
  connection.once('connect', (err) => err ? reject(err) : resolve(connection))
  connection.once('error', (err) => reject(err))
})

const request = (sql, cb) => {
  if (sql.sql) {
    const req = new tedious.Request(sql.sql, cb)
    sql.parameters.forEach((param) =>
      req.addParameter(param.name, param.type, param.value)
    )
    return req
  } else {
    return new tedious.Request(sql, cb)
  }
}

const query = (connection, sql) => Observable.create((observer) => {
  var columnsMetadata
  var abort = false
  var done = false
  const req = request(sql, (err, rowCount) => {
    if (err) observer.error(err)
  })
  req.on('columnMetadata', (cols) => {
    columnsMetadata = cols
  })
  req.on('requestCompleted', () => {
    done = true
    observer.complete()
    if (connection.abort) connection.abort()
  })
  req.on('row', (columns) => {
    if (abort) return
    const record = {}
    if (Array.isArray(columns)) {
      columns.forEach((col) => {
        record[col.metadata.colName] = col.value
      })
    } else {
      columnsMetadata.forEach((col) => {
        record[col.colName] = columns[col.colName].value
      })
    }
    observer.next(record)
  })

  connection.execSql(req)
  return () => {
    if (!done) connection.cancel()
    abort = true
  }
})

const queryFactory = (config) => (line) => Observable.fromPromise(
  connect(config).then((conn) =>
    query(conn, line)
      .do(null, null, conn.close.bind(conn))
  )
).concatAll()

const createParameter = (name, value) => ({
  name,
  value,
  type: typeof value === 'string' ? tedious.TYPES.NVarChar
    : value instanceof Date ? tedious.TYPES.DateTime
    : typeof value === 'object' ? tedious.TYPES.Variant
    : value === null ? tedious.TYPES.Null
    : value === undefined ? tedious.TYPES.Null
    : typeof value !== 'number' ? tedious.TYPES.Variant
    : Math.floor(value) !== value ? tedious.TYPES.Real
    : value < 1 << 31 ? tedious.TYPES.BigInt
    : value > ~(1 << 31) ? tedious.TYPES.BigInt
    : tedious.TYPES.Int,
  toString: () => typeof value === 'string' ? `N'${value.split("'").join("''")}'`
    : value instanceof Date ? `'${value.toISOString()}'`
    : typeof value === 'object' ? '[Object]'
    : value === null ? 'NULL'
    : value === undefined ? 'NULL'
    : typeof value !== 'number' ? '[Unknown]'
    : value.toString()
})

const raw = (string) => new ''.constructor(string)

function sql (template) {
  var sqlParts = [template[0]]
  var stringParts = [template[0]]
  var parameters = []
  for (var i = 1; i < arguments.length; i++) {
    var pname = `P${i}`
    var arg = arguments[i]
    if (typeof arg === 'object' && !(arg instanceof Date)) {
      sqlParts.push(arg, template[i])
      stringParts.push(arg, template[i])
    } else {
      var param = createParameter(pname, arguments[i])
      parameters.push(param)
      sqlParts.push(`@${pname}`, template[i])
      stringParts.push(param, template[i])
    }
  }
  return {
    sql: sqlParts.join(''),
    parameters,
    toString: () => stringParts.join('')
  }
}

module.exports = {
  connect, query, queryFactory, sql, raw
}

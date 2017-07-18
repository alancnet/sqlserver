const { queryFactory, sql, raw } = require('./sqlserver')

const query = queryFactory({
  server: '10.81.1.109',
  options: {
    port: 1433,
    database: 'OutsourcedAttributes',
    connectTimeout: '60000'
  }
})

query(sql`SELECT * FROM INFORMATION_SCHEMA.TABLES`)
.subscribe(console.log, console.error)

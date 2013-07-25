var sql = require("./gi-mysql.js")
  , gi = sql.gi
  , db_info require("./db.js");
;

sql.connect(db_info.host,db_info.user,db_info.password,db_info.database);


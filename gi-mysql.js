var gi = require("./gi.js")
  , mysql = require("mysql")
  , EventEmitter = require("events").EventEmitter
;

/* 
  Will execute an sql statement

  cb possible value:
    begin ( field{arr:array of field name, raw:map -> {field_name : field_desc,...} })
    , row ( data, row_index, firld.raw )
    , end ()
    , error ( msg )
*/

gi.conf("alias_in_group",true);

function select( data, cb ) {
  var parsed = gi.gen.select( data )
    , query = new Query( cb )
  ;
  query.exec( parsed.sql, parsed.data );
}

function Query( cb ) {
  this.events = new EventEmitter();
  gi.on( this.events, cb );
  if( !gi.is.function(cb.error) ) {
    this.events.on("error",function(msg){
      console.log("ERROR:" + gi.dump(msg));
    })
  }
}
Query.prototype.on = function(){ gi.on.apply(this,[this].concat(arguments)) };
Query.prototype.exec = function(statement, values){
  var me = this;
  pool.get(function(err, connection){
    if(err) {
      me.events.emit("error",gi.log("while getting connection. err dump: " + gi.dump(err), "ERROR"));
      return;
    }
    var _fields = {count:0};
    gi.log(statement,"EXEC");
    gi.log(gi.dump(values).replace(/\n/g, "\t"),"PARAM");
    connection.query( statement, values )
    .on('fields', function(fields, index) {
      _fields[index] = { raw : {}, count : 0, arr : gi.map(fields,function(e){ return e.name }) };
      gi.each(fields,function(i,e){ _fields[index].raw[e.name] = e });
      me.events.emit("begin",_fields[index]);
    }).on("result",function(row, index){
      _fields.count++;
      me.events.emit("row",gi.throw_up(row, _fields[index].arr), _fields[index].count++, _fields[index].raw );
    }).on("end",function(){
      me.events.emit("end",_fields.count);
      connection.end();
    }).on("error",function(err){
      me.events.emit("error", gi.log( "while executing query, error dump:" + gi.dump(err), "ERROR" ) );
    });
  })
};

var pool = { pool : null };
pool.get = function (cb){
  if(!pool.pool) {
    throw gi.log("gi-mysql::pool.get -> no database connected. Call gi-mysql.connect before.","ERROR")
  }
  pool.pool.getConnection(cb);
};
pool.connect = function(host, username, password, database) {
  if( !pool.pool ) {
    pool.param = { host: host, user: username, password: password, database: database };
    pool.pool = mysql.createPool( pool.param );
  } else {
    throw  gi.log( "Multiple pool connection", "WARNING" );
  }
};

gi.log.colors("param","magenta");
gi.log.colors("exec","green");


module.exports = {
  connect : pool.connect
  ,select : select
  ,gi : gi
}
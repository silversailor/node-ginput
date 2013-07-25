var sql = require("./gi-mysql.js")
  , gi = sql.gi
  , db_info = require("./db.js")

  , app = require('express')()
  , server = require('http').createServer(app)
  , io = require('socket.io').listen(server)

;

gi.conf("debug",true);
sql.connect(db_info.host,db_info.user,db_info.password,db_info.database);

server.listen(80);

app.get('/', function (req, res) {
  res.sendfile(__dirname + '/public/html/index.html');
});

app.get(/^\/(assets|vendor)\/(.*)$/, function(req, res){
  var matchs = req.url.match(/\/(assets|vendor)\/(.*)$/)
    , url = "/public/" + matchs[1] + "/" + matchs[2]
  ;
  gi.log("serving - " + url, "notice");
  res.sendfile(__dirname + url );
});

io.sockets.on( "connection", function ( socket ) {
  var __data = gi.configuratioN({"name":"guest"});
  socket.emit( "please-identify", {field:["name"]} );
  socket.on( "identifying", function( data ){
    gi.each(data,function(i,e){
      __data(i,e);  
    });
  });
  socket.on( "get", function ( data ) {
    /*
      data = {
        select : [
        { id : transaction_id
          ,data : gi.gen.select @data
          ,fetch_field : boolean
        }]
      }
     */
    gi.each( data.select, function(i, request) {
      sql.select( request.data, {
        begin: function( fields ){
          var _data = {id: request.id};
          if( request.fetch_field === "full" ) {
            _data.fields = gi.map( fields.map, function(e){
              return gi.throw_up(e, ["name","charsetNr","length","type","flags","decimals","default","zeroFill","fieldLength"])
            });
          } else if( request.fetch_field === true ) {
            _data.fields = fields.arr;
          }
          socket.emit( "get-begin", _data );
        }
        ,row: function( data, index ){
          socket.emit( "get-row", {id:request.id, data:data, index:index} );
        }
        ,end:function( count ) {
          socket.emit( "get-end", {id:request.id, count:count} );
          gi.log(count + " row sent to " + __data("name") );
        }
        ,error:function( error ) {
          socket.emit( "get-error", {id:request.id, error: error} );
          gi.log( " with " + __data("name") + " : " + dump(error), "ERROR" );
        }
      })
    })
  });
});

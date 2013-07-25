/* gi-sql
  will get data from database and fire event when "registered data" is changed using the "set" method.
  
*/
/* 
function gen.select(data)
	@data: {map}
  	.field:
      {string} field name & alias
		  {map}
    		{key} => field alias
    		{value} =>
          {string} => field name
          {boolean} => return
          {map} options
        		.name => {string} {optionnal} {default: field alias} field name
        		.return => {boolean} {optionnal} {default: true} return value in resultset or not
        		.aggregate => {string} {optionnal} {default: ""} aggregate function to run on the field
        		.where => {map} // to tell what value to filter on
        			.operator => {optionnal} {default: { ".group": (is.string(operator) ? operator : "AND") } }
                {string} operator to use between multiple "operation"
                {map}
                  {key} group on which it take place
                  {value} operator to use between operation
              .group => {variant} {opitonnal} {default: "1"} group of operation, (same group will be in parenthesis, multiple level can be used ex: 1.1)
              {key} => operation, wich test to do with the value (ex: "=", "like",...)
              {value} =>
                {variant} => value to test with
                {array{variant}} => multiple value to test with
                  *{default.operator : .filter.operator}

                {map} => to configure operation between multiple value (will test as follow: $(typeof(map) == "object" && map && map.value)
                  .operator => {.filter.operator}
                  .group => {string} {optionnal} {default: .filter.operator}
                  .value => {.filter.value{variant | array{variant}}}
            .having => {.field[i].where}
    .table:
      {string} => field name & alias
      {map}
        {key} => table alias
        {value} => {map}
          .name => {string} {optionnal} {default: table alias} table name (may be fully qualified / have db name in it)
          .join => {string} {optionnal: for only 1 elements, required for every other} {default: ""} type of join to use
          .on => {map} {optionnal} {default:{}} on which field to make the join (use table alias to determine ambiguous field name)
            {key} field name to join on (in any table)
            {value} =>
              {string} => same as {key}
              {map}
                {name} => {string} same as .on{key}
                {operator} => {string} {optionnal} {default: "="} operator to use (.on{key} .on{key}.operator .on{key}.name)
}*/

var colors = require("colors")
  , undef, is = {
  object : function( o ) { return typeof( o ) === "object" && o; }
  ,function : function( f ) { return typeof( f ) === "function"; }
  ,array : function( a ) { return is.object( a ) && typeof( a.length ) === "number"; }
  ,string : function( s ) { return typeof( s ) === "string"; }
  ,number : function( n ) { return typeof( n ) === "number"; }
  ,boolean : function( b ) { return typeof( b ) === "boolean"; }
  ,defined : function( o ) { return typeof( o ) !== "undefined"; }
  ,empty : function( o ) {
    if( is.object(o) ) {
      if( is.number(o.length) ) {
        return o.length === 0;
      } else {
        for( var i in o ) break;
        return !is.defined( i );
      }
    }
    return true;
  }
};

function parse( data ) {
  var result = {
    table : parse.table( data.table )
    ,field : parse.field( data.field )
  };
  return is.empty( result.table ) || is.empty( result.field ) ? null : result;
}
parse._each = function ( fct, data, defaults) {
  var r, result = is.object( defaults ) ? defaults : {};
  each( data, function(i, e){
    if( r = fct(e, i, result) ) {
      result[ i ] = r;
    } else {
      gi.log("Error while parsing data["+i+"]","warning");
    }
  });

  return result;
};
parse.table = function ( data ) { return parse._each( parse.table.each, is.string( data ) ? obj( data, data ) : ( is.object( data )  ? data : {} ) ) };
parse.table.on = function( data ){  return parse._each( parse.table.on.each, data ) };
parse.field = function( data ) { return parse._each( parse.field.each, is.string( data ) ? obj( data, data ) : ( is.object( data )  ? data : {} ) ) };
parse.field.operator = function( data, d_group, d_oper ) { return is.string( data ) ? obj( d_group, data ) : ( is.object(data) ? data : (d_oper ? obj( d_group, d_oper ) : "" )) };
parse.field.filter = function( data ) {
  var defaults = { group: is.string( data.group ) ? data.group : "1" };
  defaults.operator = parse.field.operator( data.operator, defaults.group, "AND" );
  return parse._each( parse.field.filter.each, digest(data, ["operator", "group"]), defaults);
};
parse.table.each = function( data, index ){
  var result = {}
    , defaults = is.object( data ) ? data : {}
  ;
  result.name = is.string( defaults.name ) ? defaults.name : ( is.string( data ) ? data : index );
  result.join = is.string( defaults.join ) ? defaults.join : "";
  result.on = is.object( defaults.on ) ? parse.table.on( defaults.on ) : {};

  return is.string(result.name) ? result : null;
};
parse.table.on.each = function( data ) {
  var result = {}
    , defaults = is.object( data ) ? data : {}
  ;
  result.name = is.string( data ) ? data : defaults.name;
  result.operator = is.string( defaults.operator ) ? defaults.operator : "=";

  return result.name ? result : null;
};
parse.field.each = function( data, index ) {
  var field = new Field( index, data );
  return field.name ? field : null;
};
parse.field.filter.each = function( data, index, root ) {
  var data_is_map, result = {}
    , defaults = ( data_is_map = is.object(data) && is.defined(data.value) ) ? data : {}
  ;
  result.group = is.string( defaults.group ) ? defaults.group : (root.group);
  result.operator = parse.field.operator( defaults.operator, result.group, "" );
  result.value = is.defined( defaults.value ) ? defaults.value : ( data_is_map ? undef : data );
  if( is.defined(result.value) ) {
    if( !is.array(result.value) ) {
      result.value = [result.value];
    }
    return result;
  }
  return null;
};

function Field( alias, data ) {
  var defaults = is.object( data ) ? data : {};

  this["return"] = is.boolean( data ) ? data : ( is.defined(defaults["return"]) ? defaults["return"] : true );
  this.name = is.string( data ) ? data : ( is.string(defaults.name) ? defaults.name : alias );
  this.alias = alias;
  this.aggregate = is.string( defaults.aggregate ) ? defaults.aggregate : "";
  this.where = is.object( defaults.where ) ? parse.field.filter( defaults.where ) : {};
  this.having = is.object( defaults.having ) ? parse.field.filter( defaults.having ) : {};
}
Field.prototype.getName = function( include_alias ) {
  var result = this.name;
  if( this.aggregate ) {
    result = this.aggregate + "(" + result + ")";
  }
  if( include_alias && this.alias !== result ) {
    result += " as " + this.alias;
  }
  return result;
};
function FilterGroup() {
  this.child = {};
  this.field = [];
  this.operator = "AND";
}
FilterGroup.prototype.addField = function( name, operation, value ) {
  this.field.push({ name: name, operation: operation, value: value });
};
FilterGroup.prototype.addOperator = function( operator ) {
  var oper, current = this, path = [];
  each( operator, function( group, oper ) {
    each( group.split("."), function(i, group) {
      path.push(group);
      current = is.defined( current.child[group] ) ? current.child[ group ] : current.child[ group ] = new FilterGroup();
      if( oper = operator[path.join(".")] ) {
        current.operator = oper;
      }
    });
  });
};
FilterGroup.prototype.addChild = function( groupName, operator ) {
  var oper, current = this, path = [];
  each( groupName.split("."), function(i, group) {
    path.push(group);
    current = is.defined( current.child[group] ) ? current.child[ group ] : current.child[ group ] = new FilterGroup();
    if( oper = operator[path.join(".")] ) {
      current.operator = oper;
    }
  });
  return current;
};
FilterGroup.prototype.resolve = function() {
  var result = [], values = [], current = this;
  each( this.field, function(i, field) {    
    result.push( field.name + " " + field.operation + " ?" );
    values.push( field.value );
  });
  each( this.child, function(i, child) {
    var c_result = child.resolve();
    if(c_result.sql) {
      result.push( c_result.sql );
      values = values.concat( c_result.values );
    }
  });
  var sql = result.join( " " + this.operator + " " );
  return { values: values, sql: result.length ? ( result.length > 1 ? "(" + sql + ")" : sql ) : "" };
};

var format = {};
format.field = function( fields ) {
  return map( fields, function(field){ return field["return"] ? field.getName(true) : null } ).join( "," )
};
format.on = function( on ) {
  var result = [];
  return map(on, function(data, field_1) {
    return field_1 + data.operator + data.name;
  }).join("AND");
};
var gen = {};
gen.select = function (data){
  var statement, parsed = parse(data);
  statement = "SELECT "+ format.field(parsed.field) + " FROM";

  each( parsed.table, function(alias, table) {
    var on = on = (on = format.on( table.on )) ? " ON " + on : ""
      , join = table.join ? " " + table.join + " JOIN" : ""
      , name = " " + table.name + (table.name !== alias ? " as " + alias : "")
    ;
    statement += join + name + on;
  });
  var filter = {
    where : new FilterGroup()
  , having : new FilterGroup()
  };
  each( parsed.field, function(alias, field) {
    var name = conf("alias_in_where") ? field.alias : field.getName();
    each( ["where","having"], function(i,type){
      each( field[type], function(operation, filter_data) {
        switch( operation ) {
          case "operator": filter[ type ].addOperator(filter_data);
          case "group": return;
          default:
            var target = filter[ type ].addChild( filter_data.group, filter_data.operator );
            each( filter_data.value, function(i, value) {
              target.addField( name, operation, value );
            });
        }
      });
    });
  });

  var where = filter.where.resolve()
    , having = filter.having.resolve()
    , do_group = false
  ;
  each(parsed.field, function(i,field) { return !( do_group = (field.aggregate ? true : false) )});
  if( where.sql ) { statement += " WHERE " + where.sql; }
  if(do_group) {
    statement += " GROUP BY " + map( parsed.field, function(field){ return field.aggregate ? null : (conf("alias_in_group") ? field.alias : field.name) }).join(",");
  }
  if( having.sql ) { statement += " HAVING " + having.sql; }

  return {sql:statement, data:where.values.concat(having.values)};
}

function on( l, e, h ) {
  if( is.function(h) ) {
    l.on( e, h );
  } else if( is.object(e) ) {
    each(e, function(n,h){ l.on(n, h) });
  }
}
function obj() {
  var r = {}, max = arguments.length -1;
  for( var i = 0 ; i < max ; i+=2 ) {
    r[ arguments[i] ] = arguments[ i+1 ];
  }
  return r;
}
function log(msg, type){
  if(conf.data.debug) {
    var t = (type?type+":":"") + msg
      , c = log.colors((type?type:"").toLowerCase())
    ;
    console.log( c ? t[c] : t );
  }
  return msg;
}

log.colors = configuratioN({
  /*
    bold
    italic
    underline
    inverse
    yellow
    cyan
    white
    magenta
    green
    red
    grey
    blue
    rainbow
    */
  "":"white"
  ,notice:"cyan"
  ,error:"red"
  ,warning:"yellow"
})
function each( o, f ){
  if( is.object(o) ){
    if( is.number(o.length) ) {
      for( var i = 0 ; i < o.length ; i++ ) {
        if( f.call(o[i], i, o[i]) === false ) {
          break;
        }
      }
    } else {
      for( var i in o ) {
        if( f.call(o[i], i, o[i]) === false ) {
          break;
        }
      }
    }
  }
  return o;
}
function map( o, f ){
  var r, result = [];
  if( is.object(o) ){
    if( is.number(o.length) ) {
      for( var i = 0 ; i < o.length ; i++ ) {
        if( (r = f.call(o[i], o[i], i)) !== null ) {
          result.push( r );
        }
      }
    } else {
      for( var i in o ) {
        if( (r = f.call(o[i], o[i], i)) !== null ) {
          result.push ( r );
        }
      }
    }
  }
  return result;
}
function in_array( a, v ) {
  var r = false;
  each(a,function(i,e) {
    return !( r = e === v );
  });
  return r;
}
function pad(value, qty, pad) {
  /** pad a number with leading zero (maxlength = qty)
    if qty is negative, right pad
  **/
  var r = "",
    p = pad ? pad : "0",
    abs_qty = Math.abs(qty);
  for (var i = abs_qty; i > 0; i--) {
    r += p;
  }
  return qty > 0 ? (r + value).slice(-abs_qty) : (value + r).slice(0,abs_qty);
}
function dump( o, showFunction, i ) {
  var indt = i ? i : ""
    , result = ""
    , sf = typeof(showFunction) === "boolean" ? showFunction : true
  ;
  switch( typeof(o) ) {
    case "object":
      if(o === null) {
        result += "NULL";
      } else {
        result += "{" + (is.number(o.length) ? "array["+o.length+"]" : "object") + "}";
        var content = map(o, function(e, j) { return indt + dump( j, showFunction ) + " => " + dump( e, showFunction, "  " + indt ) });
        if(content.length) {
          result += "\n" + content.join("\n");
        } else {
          result += "...vide...";
        }
      }
    break;
    case "function": if( showFunction !== true ) { result += "{Function}"; break; }
    case "string": result += '"' + o + '"'; break;
    case "number": case "boolean": default: result += o;
  }
  return result;
}
function configuratioN(defaults) {
  function f( key, value ){
    if( is.defined(value) ) {
      f.data[ key ] = value;
    }
    return f.data[ key ];
  }
  f.data = is.object( defaults ) ? defaults : {};
  return f;
}
function digest( o, k, copy ) {
  /* remove all k keyd value from o */
  var result,keys = [].concat( k );
  each( keys , function(i, key) {
    if( is.defined(o[key]) ) {
      delete o[ key ];
    }
  });
  return o;
}
function throw_up( o, k ) {
  /* return new object with all k keyd value (shallow copyed) from o */
  var result = {}
    , keys = is.array( k ) ? k : [ k ]
  ;
  each( keys, function(i, key) {
    result[ key ] = o[ key ];
  });
  return result;
}
var conf = configuratioN({"alias_in_where": false, "alias_in_group": false, "debug":false});

module.exports = {
   is : is
  ,on : on
  ,gen : gen
  ,obj : obj
  ,log : log
  ,map : map
  ,pad : pad
  ,conf : conf
  ,dump : dump
  ,each : each
  ,parse : parse
  ,digest : digest
  ,throw_up : throw_up
  ,in_array : in_array
  ,configuratioN : configuratioN
};

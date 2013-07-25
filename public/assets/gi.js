var gi = (function($){
	var _ = {};
	_.init = function(){
 		_.socket = io.connect( "http://localhost" );

  	_.socket.on( "please-identify", function( data ){
  		var result = {}, f;
  		for( var i = 0 ; i < data.field.length ; i++ ) {
  			result[ f = data.field[i] ] = prompt( f );
  		}
  		_.socket.emit("identifying",result);
  	});
  	_.socket.on( "get-begin", function ( data ) {
  		$( selects[data.id] ).trigger( "begin", [data] );
  	});
  	_.socket.on( "get-row", function ( data ) {
  		$( selects[data.id] ).trigger( "row", [data] );
  	});
  	_.socket.on( "get-end", function ( data ) {
  		$( selects[data.id] ).trigger( "end", [data] );
  		delete selects[data.id];
  	});
  	_.socket.on( "get-error", function ( data ) {
  		$( selects[data.id] ).trigger( "error", [data] );
  		console.error( "Error on select[id:" + data.id + "] : (dump todo:" + data.error + ")" );
  	});
	};

	var selects = {};
	_.select = function() {
		var s = new Select();
		selects[s.__id] = s;
		return s;
	};

	function Last( name ) {
		this.obj = null;
		this.name = name;
	}
	Last.prototype.set = function( obj ) {
		return this.obj = obj;
	};
	Last.prototype.ifset = function( bool ) {
		if( this.obj ) {
			return bool === false ? true : this.obj;
		}	else {
			console.warn( "Last-> " + this.name + " isn't set" );
		}
		return bool === false ? false : this.obj;
	};
	Last.prototype.append = function( key, value ) {
		if( this.ifset(false) ) {
			if( typeof(key) !== "object" ) {
				this.obj[ key ] = value;
			}	else {
				for( var k in key ) {
					this.obj[ k ] = key[ k ];
				}
			}
		}
		return this.obj;
	};

	function Select(){
		this.__data = {	table:{}, field:{} };
		this.__last = {};
		this.__id = Select.prototype._next_id++;
	}
	Select.prototype.__fetch_field = false;
	Select.prototype._next_id = 0;
	Select.prototype._last = function(key) {
		return this.__last[ key ] ? this.__last[ key ] : ( this.__last[key] = new Last(key) );
	};
	Select.prototype.from = function( alias, name ) {
		/* add a table to work on (name's optionnal, default: alias) */
		this.__data.table[ alias ] = this._last( "table" ).set({ name : name ? name : alias });
		return this;
	};
	Select.prototype.join = function( join ) {
		/* select how the last added table (.from) will be joined ["LEFT","INNER","RIGHT"] */
		this._last( "table" ).append( "join", join );
		return this;
	};
	Select.prototype.on = function( a, b ) {
		/* determine on which field to make the last join defined */
		var _last_from;
		if( _last_from = this._last("table").ifset() ) {
			if( !_last_from.on ) {
				_last_from.on = {};
			}
			_last_from.on[ a ] = b;
		}
		return this;
	};
	Select.prototype.field = function( alias, name ) {
		this.__data.field[ alias ] = this._last( "field" ).set({ name: (name ? name : alias) });
		return this;
	};
	Select.prototype.aggregate = function( aggregate ) {
		this._last( "field" ).append( "aggregate", aggregate );
		return this;
	};
	Select.prototype.do_return = function( do_return ) {
		this._last( "field" ).append( "return", do_return );
		return this;
	};
	Select.prototype.filter = function( type, b, c ) {
		var value, operation, _last_field;
		if( typeof(c) !== "undefined" ) {
			operation = b;
			value = c;
		}	else {
			operation = "=";
			value = b;
		}

		if( _last_field = this._last("field").ifset() ) {
			if( ! _last_field[type] ) {
				_last_field[ type ] = {};
			}
			_last_field[ type ][ operation ] = this._last( "filter" ).set({ value: value });
		}
		return this;
	};
	Select.prototype.group = function( group ) {
		/* set in which group of operation last filter will occur. group may be nested ex: group#1 AND ( group#1.1 OR group#1.2 )*/
		this._last("group").set(this._last("filter").append("group", group) );
		return this;
	};
	Select.prototype.operator = function( operator ) {
		/* set operator to be used on the last group entered */
		this._last( "group" ).append( "operator", operator );
		return this;
	};
	Select.prototype.do_fetch_field = function(state) {
		this.__fetch_field = state;
		return this;
	};
	Select.prototype.where = function(/* [operation = "="], value */) {
		return this.filter.apply(this,["where"].concat(Array.prototype.splice.call(arguments,0)));
	};
	Select.prototype.having = function(/* [operation = "="], value */) {
		return this.filter.apply(this,["having"].concat(Array.prototype.splice.call(arguments,0)));
	};
	Select.prototype.get = function(){
		_.socket.emit("get", { select:[{id:this.__id, data : this.__data, fetch_field : this.__fetch_field }]});
		return this;
	}

	Select.prototype.do_log = function(){
		$(this).on("begin",function(ev,data){ console.log("#" + data.id + " - begin") })
			.on("end",function(ev,data){ console.log("#" + data.id + " - end - " + data.count + " row received.") })
			.on("row",function(ev,data){ console.log("#" + data.id + " - row" + data.index + ": " + $.map(data.data,function(e,i){ return i + " => " + e }).join("\t") ) })
			.on("error",function(ev,data){ console.log("#" + data.id + " - error - " + data ) })
		;
		return this;
	}

	return _;
})(jQuery);
const {promisify} = require('util');
const etl = require('./etl');
const {
	isMANIFEST,
	isETLOptions,
	isCONFIG
} = require('./schemas');
const FOLDER = 'mySQL';

const DEFAULTS = {
	templateSQL:query=>query,
	runQuery:null,
	getFile:null
};
function SqlETL(CONFIG = {}){
	this.config = Object.assign({},DEFAULTS,CONFIG);
	isCONFIG(this.config);
	if(typeof this.config.runQuery !== 'function'){
		throw new Error('runQuery must be a Fn')
	};
	Object.keys(this).forEach(k=>{
		if(typeof this[k] == 'function'){
			this[k] = this[k].bind(this);
		}
	});
	return this
};
SqlETL.prototype.setConfig = function(CONFIG) {
	this.config = Object.assign(this.config,CONFIG)
	return this
};
SqlETL.prototype.getConfig = function(){
	return this.config;
}
SqlETL.prototype.getFile = function(fileName) {
	return this.config.getFile(fileName)
};
SqlETL.prototype.run = function(options){
	isETLOptions(options);
	return etl.call(this,options)
}
SqlETL.prototype._templateSQL = function(sql,params){
	return this.config.templateSQL(sql,params)
}
SqlETL.prototype._runQuery = function(query,info){
	return this.config.runQuery(query,info);
}
module.exports = SqlETL




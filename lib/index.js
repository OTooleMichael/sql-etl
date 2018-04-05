const {promisify} = require('util');
const etl = require('./etl');
const { downloadVersion,uploadVersion, getFile } = require('./manageFiles');
const FOLDER = 'mySQL';
const DEFAULTS = {
	directory:__dirname+`/../${FOLDER}`,
	zipDir:__dirname+`/../${FOLDER}.zip`,
	ENV:'dev',
	S3Key:`${FOLDER}.zip`,
	S3Bucket:null,
	templateSQL:query=>query
};
const REQUIRED_S3 = ['runQuery','S3Bucket','S3Key','AWS'];
const REQUIRED_OTHER = ['runQuery','upload','download'];
function SqlETL(CONFIG = {}){
	this.config = Object.assign(DEFAULTS,CONFIG);
	let REQUIRED = REQUIRED_S3;
	if(CONFIG.upload || CONFIG.download){
		REQUIRED = REQUIRED_OTHER
	}else{
		this.config.AWS = require('aws-sdk')
	}
	
	REQUIRED.forEach(k=>{
		if(!this.config[k]) throw new Error(k+' is required')
	});
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
SqlETL.prototype.uploadVersion = function() {
	const upload = this.config.upload ? this.config.upload : uploadVersion;
	return upload(this.config)
};
SqlETL.prototype.downloadVersion = function() {
	const download = this.config.download ? this.config.download : uploadVersion;
	return download(this.config)
};
SqlETL.prototype.getFile = function(fileName) {
	return getFile.call(this,fileName)
};
SqlETL.prototype.run = function(options){
	return etl.call(this,options)
}
SqlETL.prototype._templateSQL = function(sql,params){
	return this.config.templateSQL(sql,params)
}
SqlETL.prototype._runQuery = function(query){
	return this.config.runQuery(query);
}
module.exports = SqlETL




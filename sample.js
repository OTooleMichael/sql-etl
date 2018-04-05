const myAuthorisedAWS = require('../myAuthorisedAWS');
const SqlETL = require('./lib/index');
const ejs = require('ejs');
const {
	QueryPromise
} = require("./myDb");
const redis = require('./myRedisClient');
const directory = __dirname+"/../etlSQLFiles";
const CONFIG = {
	directory,
	zipDir:directory+'.zip',
	AWS:myAuthorisedAWS,
	ENV:'dev',
	templateSQL:function(query,params){
		return ejs.render(query, params, {});
	},
	runQuery:function(query){
		const ETL_USER = 'etl_user'
		return QueryPromise('etl_user',query); // returns rows of json;
	}
};
async function start(){
	const sqlETL = new SqlETL(CONFIG);
	let update = await redis().hget('features','update_etl');
	update = JSON.parse(update_runner);
	if(update_runner){
		await sqlETL.loadNewDeploy();
		await redis().hset('features','update_runner',false.toString());
	}
	return sqlETL.run({
		manifest:'runner.json'
	})
}
module.exports = start;
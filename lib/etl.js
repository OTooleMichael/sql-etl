const {
	isMANIFEST,
	isETLOptions
} = require('./schemas');
async function etl(options){
	isETLOptions(options);
	let {
		manifest,
		startAt = 0,
		endAt,
		debug = false
	} = options;
	if(typeof manifest == 'string'){
		let params = await this.getFile.call(this,manifest);
		manifest = JSON.parse(params);
	};
	isMANIFEST(manifest);
	manifest.debug = debug;
	manifest.startAt = startAt;
	manifest.endAt = endAt || manifest.sequence.length;
	return runBuild.call(this,manifest);
}

async function runTestPart(query,info){
	info = Object.assign({type:'test'},info);
	query = query.trim();
	if(!query || query.length == 0 ){
		return 'EMPTY STRING';
	}
	let output = {query};
	try{
		const rows = await this._runQuery(query,info);
		output.result = (rows) ? rows[0] : null;
		if(!rows[0] || !rows[0].test_pass){
			output.error = (rows[0]) ? "TEST FAIL" : "NO RESULT"  ;
			return Promise.reject(output);
		}
		return output;
	}catch(error){
		output.error = error;
		return Promise.reject(output);
	}
}
async function runNextStep(step){
	const start = Date.now()
	const promises = step.map(async (el)=>
	{
		const {path,params} = el;
		el.start = start;
		let sql = await this.getFile(path);
		sql = this._templateSQL(sql.trim(),params)
		const transform = await runTransformSQLPart.call(this,sql,el);
		el.end = Date.now();
		el.runTime = (el.end - el.start)/1000;
		if(!el.test) return el;
		let tests = await this.getFile("tests/"+path);
		tests = this._templateSQL(tests.trim(),params)
		let queries = tests.split(';').map((q)=>runTestPart.call(this,q,el) );
		return Promise.all(queries).then(testResults=>el);
	});
	return Promise.all(promises);
}


async function runBuild({sequence,debug=false})
{
	let completed = [];
	let startRunner = Date.now();
	sequence = sequence.map(step => (step instanceof Array) ? step : [step]);
	let i = 0;
	try{
		for(var step of sequence){
			completed[i] = {
				i,
				startTime:new Date()
			};
			if(debug){
				console.log(i+" Runner ");
				console.log(' ------ ',step.map((l)=>l.path));
				let runTime = (Date.now() - startRunner)/1000;
				console.log('Running '+runTime+' Secs');
			}
			let res = await runNextStep.call(this,step);
			completed[i].paths = res;
			completed[i].endTime = Date.now();
			completed[i].runTime = (completed[i].endTime - completed[i].startTime)/1000;
			if(debug){
				console.log(completed[i])
			}
			i++;
		}
		return completed;
	}catch(error){
		return Promise.reject({
			error,
			completed
		});
	};
}
async function runTransformSQLPart(query,info){
	info = Object.assign({type:'transform'},info);
	try{
		const rows = await this._runQuery(query,info);
		return 'DONE'
	}catch(error){
		const needsRollback = query.search(/BEGIN;/i) !== -1;
		if(needsRollback){
			await this._runQuery('ROLLBACK;',{type:'rollback'});
		}
		return Promise.reject({error,query})
	}
};
module.exports = etl;
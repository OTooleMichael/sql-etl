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
		debug = false,
		runTests = true,
		runTransforms = true
	} = options;
	if(typeof manifest == 'string'){
		let params = await this.getFile.call(this,manifest);
		manifest = JSON.parse(params);
	};
	isMANIFEST(manifest);
	manifest.debug = debug;
	manifest.startAt = startAt;
	manifest.runTests = runTests;
	manifest.runTransforms = runTransforms;
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
async function runNextStep(step,manifest){
	const {runTransforms, runTests} = manifest;
	async function parallelSteps(pathOptions){
		pathOptions.start = Date.now();
		const {path,params} = pathOptions;
		if(runTransforms){
			await runTransform.call(this,pathOptions);
		}
		pathOptions.end = Date.now();
		pathOptions.runTime = (pathOptions.end - pathOptions.start)/1000;
		if(!runTests || !pathOptions.test) return pathOptions;
		await runTestQueries.call(this,pathOptions);
		return pathOptions;
	};
	const proms = step.map(path=>parallelSteps.call(this,path));
	return Promise.all(proms);
}
async function runTransform(pathOptions){
	let info = Object.assign({type:'transform'},pathOptions);
	const { path,params } = pathOptions;
	let sql = await this.getFile(path);
		sql = this._templateSQL(sql.trim(),params)
	try{
		const rows = await this._runQuery(sql,info);
		return 'DONE'
	}catch(error){
		const needsRollback = sql.search(/BEGIN;/i) !== -1;
		if(needsRollback){
			await this._runQuery('ROLLBACK;',{type:'rollback'});
		}
		return Promise.reject({error,sql})
	}
};
async function runTestQueries(pathOptions){
	const {path,params} = pathOptions;
	let tests = await this.getFile("tests/"+path);
	tests = this._templateSQL(tests.trim(),params)
	let queries = tests
		.split(';')
		.map((q)=>runTestPart.call(this,q,params) );
	return Promise.all(queries);
}

async function runBuild(manifest)
{
	const {
		startAt,
		endAt,
		sequence,
		debug = false,
		params = {}
	} = manifest;
	let completed = [];
	let startRunner = Date.now();
	let i = 0;
	try{
		for(var step of sequence){
			step = (step instanceof Array) ? step : [step]
			let progress = {
				i,
				startTime:new Date()
			};
			if(i>= startAt && i<= endAt){
				if(debug){
					console.log(i+" Runner ");
					console.log(' ------ ',step.map(l=>l.path));
					let runTime = (Date.now() - startRunner)/1000;
					console.log('Running '+runTime+' Secs');
				};
				step.params = Object.assign({},step.params||{},params);	
				let res = await runNextStep.call(this,step,manifest);
				progress.paths = res;
				progress.endTime = Date.now();
				progress.runTime = (progress.endTime - progress.startTime)/1000;
				if(debug){
					console.log(progress)
				}
				completed.push(progress);
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

module.exports = etl;
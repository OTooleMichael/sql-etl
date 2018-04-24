const AdmZip = require('adm-zip');
const archiver = require('archiver');
const { promisify } = require('util');
const fs = require('fs');

async function getFile(file)
{
	if(typeof file !== 'string'){
		throw new Error('file name requied')
	}
	const {ENV,directory,zipDir} = this.config;
	if(ENV == 'dev'){
		let readFile = promisify(fs.readFile);
		return readFile(directory+'/'+file,'UTF-8')
	}
	if(!fs.existsSync(zipDir)){
		await this.downloadVersion();
	}
	return new Promise(function(resolve,reject){
		var zip = new AdmZip(zipDir);
		zip.readAsTextAsync(file,function(text)
		{
			if(!text || text == 'Invalid filename')
			{
				return reject("NOT FOUND:"+text+'\n DIR:'+zipDir+'\n FILE:'+file);
			}
			return resolve(text);
		},"UTF-8");
	});
};

function downloadVersion(CONFIG)
{
	const {ENV,directory,zipDir,AWS,S3Key,S3Bucket} = CONFIG;
	return new Promise(function(resolve,reject)
	{
		const S3 = new AWS.S3({apiVersion: '2006-03-01'});
		let file = fs.createWriteStream(zipDir);
		file.on("finish",resolve);
		file.on('error',reject);
		S3.getObject({ 
			Bucket:S3Bucket, 
			Key:S3Key 
		})
		.createReadStream().pipe(file);
	})
}

function zipRunner({zipDir,directory}){
	return new Promise(function(resolve,reject)
	{
		var output = fs.createWriteStream(zipDir);
		output.on('close', resolve);
		// modules is only working with Node versions above 8; node --version
		var archive = archiver('zip', {
		    zlib: { level: 9 }
		}); 
		archive.on('warning', reject);
		archive.on('error',reject);
		// pipe archive data to the file
		archive.pipe(output);
		archive.directory(directory, false);
		archive.finalize();
	})
}
function uploadRunner(CONFIG){
	const {AWS,S3Key,S3Bucket,zipDir} = CONFIG;
	const S3 = new AWS.S3({apiVersion: '2006-03-01'});
	return new Promise(function(resolve,reject){
		const body = fs.createReadStream(zipDir);
		body.on('error',reject);
		S3.upload({
			Bucket:S3Bucket,
			Key:S3Key,
			Body:body,
		},function(err,res){
			if(err) return reject(err);
			return resolve(res);
		})
	})	
};

function uploadVersion(CONFIG){
	return zipRunner(CONFIG)
	.then(function(){
		return uploadRunner(CONFIG)
	})
}
module.exports = {
	downloadVersion,
	getFile,
	uploadVersion,
	zipRunner
}











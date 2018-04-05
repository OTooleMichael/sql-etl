const Validator = require('jsonschema').Validator;
const OPTS = { throwError:true }
const STRING = { type:'string'};
const PATH = {
	id:'/manifestPath',
    "type": "object",
    "properties": {
		path:{type:'string',required:true},
		params:{type:'object'},
		test:{type:'boolean'}
	},
	required:['path']
}
const MANIFEST = {
	id:'/manifest',
    "type": "object",
    "properties": {
    	title:{ type:'string'},
		version:{ type:'string'},
		sequence:{
			type:'array',
			required:true,
			items: { 
				"anyOf":[
			      {"$ref": "/manifestPath"},
			      {
			        "type": 'array',
			        items:{$ref:"/manifestPath"}
			      }
			    ]
			}
		}
	},
	required:['sequence']
};
const ETLOptions = {
	id:'/etlOptions',

    "type": "object",
    "properties": {
    	manifest:{
    		"anyOf":[
		      {type:'string'},
		      {$ref:"/manifest"}
		    ],
		    required:true	
    	}
    }
}
const CONFIG_S3 = {
	id:'/configS3',
    "type": "object",
	properties:{
		directory:STRING,
		zipDir:STRING,
		ENV:STRING,
		AWS:{type:'object',required:true},
		S3Key:{type:'string',required:true},
		S3Bucket:{type:'string',required:true},
		templateSQL:{type:'function'},
		runQuery:{type:'function',required:true}
	}
};
const CONFIG_OTHER = {
	id:'/configOther',
    "type": "object",
	properties:{
		directory:STRING,
		zipDir:STRING,
		ENV:STRING,
		templateSQL:{type:'function'},
		runQuery:{type:'function',required:true},
		upload:{type:'function',required:true},
		download:{type:'function',required:true}
	}
}
const vMANIFEST = new Validator();
vMANIFEST.addSchema(PATH, '/manifestPath');
function isMANIFEST(obj) {
	return vMANIFEST.validate(obj,MANIFEST,OPTS)
}
function isETLOptions(obj){
	const v = new Validator();
	v.addSchema(vMANIFEST,'/manifest')
	v.addSchema(PATH, '/manifestPath');
	return v.validate(obj,ETLOptions,OPTS)
};
function isCONFIG(obj){
	const v = new Validator();
	return v.validate(obj,{
		anyOf:[CONFIG_S3,CONFIG_OTHER]
	},OPTS)
}
module.exports = {
	isMANIFEST,
	isETLOptions,
	isCONFIG
};







const AWS = require("aws-sdk");
AWS.config.update({ region: 'eu-west-2' });

var docClient = new AWS.DynamoDB.DocumentClient();

var partnerTable = "Partner-Live-Test";
var partnerTypeTable = "PartnerType-kehnbwxlifg5lcsbujaouso5dq-live";

const zeroPad = (num, places) => String(num).padStart(places, '0')
   
const Migrate = async (table1,table2) => {
  
  let prefix="";
  let ZSTFnum=0;
  let ZCPTnum=0;
  let ZRDRnum=0;
  let ZAGTnum=0;
  let ZFRLCnum=0;
  let ZMRCHnum=0;

  try {
    const tbl1Result= await Table1(table1);
    for(let i = 0; i < tbl1Result.Items.length; i++){ 
 
        let partnerID=tbl1Result.Items[i].id;
        let oldCode=tbl1Result.Items[i].code;
        
        // let partnerID="a00b125d-0db0-425d-a0b9-402e0179dd4a";
        // let oldCode="ZdJMf";
        
        let partnerTypdId=tbl1Result.Items[i].partnerTypeID;
      
        const tbl2Result= await Table2(partnerTypdId,table2);
        for(let i = 0; i < tbl2Result.Items.length; i++){ 
          prefix=tbl2Result.Items[i].code;
        }

        if(prefix==="ZSTF"){
          ZSTFnum++;
          let updateCodeResult=await UpdateCode(table1,partnerID,oldCode,prefix,ZSTFnum);
          let newCode=updateCodeResult.code.Attributes['code'];
          let parentCode=updateCodeResult.parentCode;
          for(let i = 0; i < parentCode.Items.length; i++){ 
            let partnerParentCodeid=updateCodeResult.parentCode.Items[i].id;
            updateParentCode(table1,partnerParentCodeid,newCode)
          }
        }

        else if(prefix==="ZCPT"){
          ZCPTnum++;
          let updateCodeResult=await UpdateCode(table1,partnerID,oldCode,prefix,ZCPTnum);
          let newCode=updateCodeResult.code.Attributes['code'];
          let parentCode=updateCodeResult.parentCode;
          for(let i = 0; i < parentCode.Items.length; i++){ 
            let partnerParentCodeid=updateCodeResult.parentCode.Items[i].id;
            updateParentCode(table1,partnerParentCodeid,newCode)
          }
        }

        else if(prefix==="ZRDR"){
          ZRDRnum++;
          let updateCodeResult=await UpdateCode(table1,partnerID,oldCode,prefix,ZRDRnum);
          let newCode=updateCodeResult.code.Attributes['code'];
          let parentCode=updateCodeResult.parentCode;
          for(let i = 0; i < parentCode.Items.length; i++){ 
            let partnerParentCodeid=updateCodeResult.parentCode.Items[i].id;
            updateParentCode(table1,partnerParentCodeid,newCode)
          }
        }

        else if(prefix==="ZAGT"){
          ZAGTnum++;
          let updateCodeResult=await UpdateCode(table1,partnerID,oldCode,prefix,ZAGTnum);
          let newCode=updateCodeResult.code.Attributes['code'];
          let parentCode=updateCodeResult.parentCode;
          for(let i = 0; i < parentCode.Items.length; i++){ 
            let partnerParentCodeid=updateCodeResult.parentCode.Items[i].id;
            updateParentCode(table1,partnerParentCodeid,newCode)
          }
        }
        
        else if(prefix==="ZFRLC"){
          ZFRLCnum++;
          let updateCodeResult=await UpdateCode(table1,partnerID,oldCode,prefix,ZFRLCnum);
          let newCode=updateCodeResult.code.Attributes['code'];
          let parentCode=updateCodeResult.parentCode;
          for(let i = 0; i < parentCode.Items.length; i++){ 
            let partnerParentCodeid=updateCodeResult.parentCode.Items[i].id;
            updateParentCode(table1,partnerParentCodeid,newCode)
          }
        }

        else if(prefix==="ZMRCH"){
          ZMRCHnum++;
          let updateCodeResult=await UpdateCode(table1,partnerID,oldCode,prefix,ZMRCHnum);
          let newCode=updateCodeResult.code.Attributes['code'];
          let parentCode=updateCodeResult.parentCode;
          for(let i = 0; i < parentCode.Items.length; i++){ 
            let partnerParentCodeid=updateCodeResult.parentCode.Items[i].id;
            updateParentCode(table1,partnerParentCodeid,newCode)
          }
        }

      
    }

  } catch (e){
    console.log(e);
  }
}


const Table1 = async (table)=>{
  const params = {
    TableName: table,
    IndexName : '__typename-code-index', //Sort by code
    // Limit: 1,
  }
  try {
    const data = await docClient.scan(params).promise()
    return data

  } catch (e){
      console.log(e)
  }
}

const Table2 = async (id,table)=>{
    const params = {
      TableName: table,
      FilterExpression: 'id = :idVal',
      ExpressionAttributeValues: {
        ":idVal": id   
      }
    }
    try {
      const data = await docClient.scan(params).promise()
      return data
  
    } catch (e){
        console.log(e)
    }
}

const UpdateCode = async (table,partnerID,oldCode,prefix,num)=>{
      let newCode=prefix+zeroPad(num, 8);
      var params = {
        TableName:table,
        Key:{
            "id": partnerID,
        },
        UpdateExpression: "set code = :c",
        ExpressionAttributeValues:{
            ":c": newCode,
        },
        ReturnValues:"UPDATED_NEW"
      };
    try {
      const data= await docClient.update(params).promise();
      //Get the list of associated partner via parentCode
      const parentCodeResult= await getParentCode (table,oldCode);
      console.log(num,"code updated successfuly!",oldCode ,"->", newCode);
      return {
        "code":data,
        "parentCode":parentCodeResult,
      };

    } catch (e) {
      console.error("Unable to update code w/ id:",partnerID,"->",e);
    }
}

const getParentCode = async (table,oldCode) =>{
  const params = {
    TableName: table,
    FilterExpression: 'parentCode = :c',
    ExpressionAttributeValues: {
      ":c": oldCode   
    }
  }
  try {
    const data = await docClient.scan(params).promise()
    // console.log("@getParentCode",data);
    return data

  } catch (e){
      console.log(e)
  }
}


const updateParentCode = async (table,pcid,newCode) =>{
  var params = {
    TableName:table,
    Key:{
        "id": pcid,
    },
    UpdateExpression: "set parentCode = :pc",
    ExpressionAttributeValues:{
        ":pc": newCode,
    },
    ReturnValues:"UPDATED_NEW"
  };

  try {
    const data= await docClient.update(params).promise();
    console.log("parentCode updated successfully!", "->", newCode );
    return data;

  } catch (e) {
    console.error("Unable to update parentCode w/ id: ",pcid,"->",e);
  }
} 


Migrate(partnerTable,partnerTypeTable);


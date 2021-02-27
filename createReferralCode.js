
const AWS = require("aws-sdk");
AWS.config.update({ region: 'eu-west-2' });

var docClient = new AWS.DynamoDB.DocumentClient();

var partnerTable = "PartnerTable-Test";
var partnerTypeTable = "PartnerType-dba6bopgp5cajovtmejn734qj4-staging";
var partnerType="169bc9d0-4518-492f-8d58-ca9d7d847409";

const zeroPad = (num, places) => String(num).padStart(places, '0')

const generateReferralCode = async (table1,table2,partnerTypeId)=>{

    const partnerTypeResult= await getPrefix(table2,partnerTypeId);    
    const prefix=partnerTypeResult.Item['code'];
  
    var params = {
      TableName: table1,
      IndexName: '__typename-code-index',
      KeyConditionExpression: '#__typename = :__typename and begins_with (#code,:code)', 
      ExpressionAttributeNames: { 
        '#__typename': '__typename',
        '#code': 'code',
       },
      ExpressionAttributeValues: { 
        ':__typename': 'Partner',
        ':code': prefix,
       },
      Limit: 1,
      ScanIndexForward: false
    }    
  
    try {
      
      const data= await docClient.query(params).promise();
      const lastCodeResult=data.Items[0].code;
      const lastNumberFilterResult= lastCodeResult.replace(prefix,'');
      const lastNumberResult=parseInt(lastNumberFilterResult,10);
      const newCode=prefix+zeroPad(lastNumberResult+1, 8);
      console.log(newCode);
      return newCode;

    } catch (e) {
      console.error("Unable to generate code.",e);
    }

}

const getPrefix = async (table,id) =>{
  const params = {
    TableName: table,
    Key:{
      "id": id
    }
  }
  try {
    const data = await docClient.get(params).promise()
    return data

  } catch (e){
      console.log(e)
  }
}

generateReferralCode(partnerTable,partnerTypeTable,partnerType);
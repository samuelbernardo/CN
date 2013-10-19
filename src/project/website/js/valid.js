var w3c=(document.getElementById)?true:false;
var ns4=(document.layers)?true:false;
var ie4=(document.all && !w3c)?true:false;
var ie5=(document.all && w3c)?true:false;
var ns6=(w3c && navigator.appName.indexOf("Netscape")>=0)?true:false;

function whiteField(area,defaultValue){
	if (area.value==defaultValue){ area.value="";}
}

function sendQuery(qry){
	var send=false;
	if (qry.submit.value!="query2" &&( qry.phoneId.value==""||qry.phoneId.value=="phoneId"))
	   alert("Insert phoneId");
	else if (qry.submit.value!="query2" && isNaN(qry.phoneId.value))
		alert("wrong format: phoneId must be numeric");
	else if (qry.date.value==""||qry.date.value=="date")
	   alert("Insert date");
	else if (qry.submit.value=="query2" && (qry.time.value==""||qry.time.value=="time"))
		alert("Insert the time");
	else if (qry.submit.value=="query2" && (qry.cellId.value==""||qry.time.value=="cellID"))
		alert("Insert the cellId");
	else {
		send=true;
	}
	if(send){alert("I'm sending");}
	return send;
}



<?php

// getting url parameters

$submit=$_GET['submit'];
$date=$_GET['date'];
$cellID="";
$phoneId="";
$time="";
if ($submit!="query2"){
	$phoneId=$_GET['phoneId'];
}
else{
	$cellId=$_GET['cellId'];
	$time=$_GET['time'];
}
echo "submit=$submit date= $date cellId=$cellId phoneId=$phoneId time=$time";

// database connection

$db_host = 'localhost';
$db_user = 'uns';
$db_pwd = '';
$database = 'my_uns';

//$conn = @pg_connect("host=$db_host dbname=$database user=$db_user password=$db_pwd");

//if(!$conn) {
//    die(‘Connessione fallita !<br />’);
//} else {
//    echo ‘Connessione riuscita !<br />’;
//}

// inizializing the right query

$querySelect="";

if($submit=="query1")
	$querySelect="select....";
else if($submit=="query2")
	$querySelect="select....";
else if($submit=="query3")
	$querySelect="select....";
	
// query e show results


	
//close connection: pg_close($conn);

?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.1//EN" "http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
   <head>
		<meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1" />
		<link href="css/style.css" rel="stylesheet" type="text/css" />
		<script type="text/javascript" src="js/valid.js"></script>

<title>CN project: Filippo Rodrigo Samuel</title>
</header>
<body>
	<div id="developer"> 
	<h1>Cloud Computing Project</h1>
	<br/> Developer: Rodrigo, Samuel and Filippo.
	</div>
	<div id="queryContainer">
	<h2>Chose a query and insert the specification</h2>
		<div class="queryForm">
		Query 1
		<form name="query1" id="query1" method="get" action="query.php" onsubmit="return sendQuery(this)">
			phoneId:<input type="text" name="phoneId" id="phoneId1" value="phoneId" onclick="whiteField(this,'phoneId')"></i>
			date:<input type="text" name="date" id="date1" value="date" onclick="whiteField(this,'date')"></i>
			<input type="submit" name="submit" id="submit1" value ="query1"></i>
		</form>
	</div>
	<div class="queryForm">
		Query 2
		<form name="query2" id="query2" method="get" action="query.php" onsubmit="return sendQuery(this)">
				cellId:<input type="text" name="cellId" id="cellId2" value="cellId" onclick="whiteField(this,'cellId')"></i>
				date:<input type="text" name="date" id="date2" value="date" onclick="whiteField(this,'date')"></i>
				time:<input type="text" name="time" id="time2" value="time" onclick="whiteField(this,'time')"></i>

				<input type="submit" name="submit" id="submit" value= "query2"></i>
		</form>
	</div>
	<div class="queryForm">
		Query 3
		<form name="query3" id="query3" method="get" action="query.php" onsubmit="return sendQuery(this)">
				phoneId:<input type="text" name="phoneId" id="phoneID3" value="phoneId" onclick="whiteField(this,'phoneId')"></i>
				date:<input type="text" name="date" id="date3" value="date" onclick="whiteField(this,'date')"></i>
				<input type="submit" name="submit" id="submit3" value="query3"></i>
		</form>
    	</div>
    </div>

</body>
</html>

<?php

  // getting url parameters
  $submit = $_GET['submit'];
  $date = $_GET['date'];
  $id = $_GET['id'];
  $time = "";
  $sql = "";

  if ($submit!="query2"){
	if($submit == "query1"){
		$sql = "SELECT value FROM  logs WHERE id='$id' and date='$date'";	
  }
	else{
		$sql = "SELECT number FROM logs WHERE id='$id' and date='$date'";	
	}
  }
  else{
	$time = $_GET['time'];
	$sql = "SELECT value FROM logs WHERE id like '$id%' and date='$date' and number='$time'";
  }

  echo"<div id='queryResult'>";
  // <DEBUG>
  echo($sql);	
  echo "/n submit=$submit date= $date id=$id time=$time";
  // </DEBUG>


  // database connection
  $user = 'ist167074';
  $host = 'db.ist.utl.pt';
  $port = 5432;
  $password = "eE92Hb41w";
  $dbname = $user;
  $connection = pg_connect("host=$host port=$port user=$user password=$password dbname=$dbname") or die(pg_last_error());
	
  // <DEBUG>
  echo("<p>Connected to Postgres on $host as user $user on database $dbname.</p>");
  // </DEBUG>

  // query to get results
  $result = pg_query($sql) or die('ERROR: ' . pg_last_error());
  $num = pg_num_rows($result);
  $fieldName = pg_field_name($result,0);
  echo("<p>$num records retrieved:</p>");

  echo('<table border="5">');
  echo("<tr><td>$fieldName </td></tr>");
  
  while ($row = pg_fetch_row($result)) {
    echo("<tr><td>");
	echo($row[0]);
	echo("</td></tr>");
  }
  echo("</table>");
		
  $result = pg_free_result($result) or die('ERROR: ' . pg_last_error());
  //<DEBUG>
  echo("<p>Query result freed.</p>");
  //</DEBUG>
  echo"</div>";

  //close connection: 
  pg_close($connection);
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
			phoneId:<input type="text" name="id" id="phoneId1" value="phoneId" onclick="whiteField(this,'phoneId')"></i>
			date:<input type="text" name="date" id="date1" value="date" onclick="whiteField(this,'date')"></i>
			<input type="submit" name="submit" id="submit1" value ="query1"></i>
		</form>
	</div>
	<div class="queryForm">
		Query 2
		<form name="query2" id="query2" method="get" action="query.php" onsubmit="return sendQuery(this)">
			cellId:<input type="text" name="id" id="cellId2" value="cellId" onclick="whiteField(this,'cellId')"></i>
			date:<input type="text" name="date" id="date2" value="date" onclick="whiteField(this,'date')"></i>
			time:<input type="text" name="time" id="time2" value="time" onclick="whiteField(this,'time')"></i>
			<input type="submit" name="submit" id="submit" value= "query2"></i>
		</form>
	</div>
	<div class="queryForm">
		Query 3
		<form name="query3" id="query3" method="get" action="query.php" onsubmit="return sendQuery(this)">
			phoneId:<input type="text" name="id" id="phoneID3" value="phoneId" onclick="whiteField(this,'phoneId')"></i>
			date:<input type="text" name="date" id="date3" value="date" onclick="whiteField(this,'date')"></i>
			<input type="submit" name="submit" id="submit3" value="query3"></i>
		</form>
	</div>
    </div>
</body>
</html>

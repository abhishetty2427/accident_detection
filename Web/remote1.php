<?php
defined('DB_HOST') or define('DB_HOST', 'localhost');
defined('DB_USER') or define('DB_USER', 'root');
defined('DB_PASS') or define('DB_PASS', '');
defined('DB_NAME') or define('DB_NAME', 'addb');
 $conn1 = new mysqli(DB_HOST,DB_USER,DB_PASS,DB_NAME);
if($conn1->connect_error){
die("connection failed" . $conn->connect_error());
}
/*echo "connection successful. <br>";*/
 //error_reporting(0); 	
?>